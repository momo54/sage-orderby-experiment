import requests
import json
import time
import sys
import logging

from typing import Dict, Any, List
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.sparql import Bindings, QueryContext
from rdflib.plugins.sparql.parserutils import Expr
from rdflib.term import Identifier, Variable, URIRef
from rdflib.util import from_n3

from approaches.approach import Approach
from approaches.topk_struct import TOPKStruct
from spy import Spy


class TOPKOperator():

    def __init__(self, query: str, limit: int = 10):
        self._exprs = translateQuery(parseQuery(query)).algebra.p.p.p.expr
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

    def __to_rdflib_term__(self, value: str) -> Identifier:
        if value.startswith("http"):
            return URIRef(value)
        elif '"^^http' in value:
            index = value.find('"^^http')
            value = f"{value[0:index+3]}<{value[index+3:]}>"
        return from_n3(value)

    def __eval_rdflib_expr__(
        self, expr: Expr, mappings: Dict[str, str]
    ) -> Any:
        if isinstance(expr, Variable):
            return mappings[expr.n3()]
        rdflib_mappings = dict()
        for key, value in mappings.items():
            rdflib_mappings[Variable(key[1:])] = self.__to_rdflib_term__(value)
        context = QueryContext(bindings=Bindings(d=rdflib_mappings))
        return expr.eval(context)

    def insert(self, mappings: Dict[str, str]) -> None:
        for index, order_condition in enumerate(self._exprs):
            mappings[f"__order_condition_{index}"] = self.__eval_rdflib_expr__(
                order_condition.expr, mappings)
        self._topk.insert(mappings)

    def flatten(self) -> List[Dict[str, str]]:
        solutions = list()
        for mappings in self._topk.flatten():
            for index in range(len(self._exprs)):
                del mappings[f"__order_condition_{index}"]
            solutions.append(mappings)
        return solutions


class SaGe(Approach):

    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        super().__init__(name)
        self._endpoint = config["endpoints"]["sage"]["url"]
        self._graph = config["endpoints"]["sage"]["graph"]

    def __remove_topk__(self, query: str) -> str:
        return query.split("ORDER")[0]

    def execute_query(
        self, query: str, spy: Spy, **kwargs
    ) -> List[Dict[str, str]]:
        limit = kwargs.setdefault("limit", 10)
        quota = kwargs.setdefault("quota", None)
        force_order = kwargs.setdefault("force_order", False)

        topk = TOPKOperator(query, limit=limit)  # client-side top-k operator

        query = self.__set_projection__(query)  # to make the validation easier
        query = self.__remove_topk__(query)  # top-k is computed by the client

        logging.info(f"{self.name} - query sent to the server:\n{query}")
        logging.info(f"{self.name} - limit = {limit}")
        logging.info(f"{self.name} - quota = {0 if None else quota}ms")

        headers = {
            "accept": "text/html",
            "content-type": "application/json"}
        payload = {
            "query": query,
            "defaultGraph": self._graph,
            "next": None,
            "quota": quota,
            "forceOrder": force_order}

        has_next = True

        start = time.time()

        while has_next:
            data = json.dumps(payload)
            response = requests.post(
                self._endpoint, headers=headers, data=data).json()

            payload["next"] = response["next"]
            has_next = response["next"] is not None

            for solution in response["bindings"]:
                topk.insert(solution)

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
