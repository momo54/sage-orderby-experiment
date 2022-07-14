import requests
import json
import time
import sys
import logging

from typing import Dict, Any, List
from base64 import b64decode, b64encode
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery

from approaches.approach import Approach
from approaches.topk_struct import TOPKStruct
from approaches.iterators_pb2 import RootTree
from spy import Spy


class TOPKOperator():

    def __init__(self, query: str, limit: int = 10):
        self._exprs = translateQuery(parseQuery(query)).algebra.p.p.p.expr
        self._limit = limit
        self._topk = self.__initialize_topk__(limit)

    def __initialize_topk__(self, limit: int) -> TOPKStruct:
        keys = []
        for index, order_condition in enumerate(self._exprs):
            if order_condition.order is None or order_condition.order == "ASC":
                order = "ASC"
            else:
                order = "DESC"
            keys.append((f"__order_condition_{index}", order))
        return TOPKStruct(keys, limit=limit)

    def insert(self, mappings: Dict[str, str]) -> None:
        self._topk.insert(mappings)

    def update_threshold(self, saved_plan: str) -> str:
        if len(self._topk) < self._limit:  # the threshold is not defined
            return saved_plan

        threshold = self._topk.lower_bound()

        root = RootTree()
        root.ParseFromString(b64decode(saved_plan))

        projection = getattr(root, root.WhichOneof("source"))

        topk = getattr(projection, projection.WhichOneof("source"))
        for key in threshold:
            topk.threshold[key] = threshold[key]

        return b64encode(root.SerializeToString()).decode("utf-8")

    def flatten(self) -> List[Dict[str, str]]:
        solutions = list()
        for mappings in self._topk.flatten():
            for index in range(len(self._exprs)):
                del mappings[f"__order_condition_{index}"]
            solutions.append(mappings)
        return solutions


class SaGeTopKCollab(Approach):

    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        super().__init__(name)
        self._endpoint = config["endpoints"]["sage"]["url"]
        self._graph = config["endpoints"]["sage"]["graph"]
        self._refresh_rate = kwargs.setdefault("refresh_rate", 0.0)

    def remove_topk(self, query: str) -> str:
        return query.split("ORDER")[0]

    def execute_query(
        self, query: str, spy: Spy, **kwargs
    ) -> List[Dict[str, str]]:
        limit = kwargs.setdefault("limit", 10)
        quota = kwargs.setdefault("quota", None)
        force_order = kwargs.setdefault("force_order", False)

        topk = TOPKOperator(query, limit=limit)

        query = self.__set_projection__(query)  # to make the validation easier
        query = self.__set_limit__(query, limit=limit)

        logging.info(f"{self.name} - query sent to the server:\n{query}")
        logging.info(f"{self.name} - limit = {limit}")
        logging.info(f"{self.name} - quota = {quota}ms")

        headers = {
            "accept": "text/html",
            "content-type": "application/json"}
        payload = {
            "query": query,
            "defaultGraph": self._graph,
            "next": None,
            "quota": quota,
            "forceOrder": force_order,
            "topkStrategy": f"ClientServer-{self._refresh_rate}"}

        has_next = True

        start = time.time()

        while has_next:
            data = json.dumps(payload)
            response = requests.post(
                self._endpoint, headers=headers, data=data).json()

            has_next = response["next"] is not None

            for solution in response["bindings"]:
                topk.insert(solution)

            if has_next:
                payload["next"] = topk.update_threshold(response["next"])

            spy.report_http_calls(1)
            spy.report_data_transfer(sys.getsizeof(data))
            spy.report_data_transfer(sys.getsizeof(json.dumps(response)))

        results = topk.flatten()

        elapsed_time = time.time() - start

        spy.report_execution_time(elapsed_time)
        spy.report_solutions(len(results))

        solutions = []  # solutions are formated to make the validation easier
        for mappings in results:
            solution = {}
            for key, value in mappings.items():
                if value.startswith('"') and value.endswith('"'):
                    solution[key] = value[1:-1]
                else:
                    solution[key] = value
            solutions.append(solution)

        return solutions
