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
    """
    This class implements a data structure that allows to maitain a
    multiple-keys order between the solutions mappings. It is used to compute
    the TOP-K on the client.

    Parameters
    ----------
    query: str
        The SPARQL TOP-K query for which we want to compute the TOP-K.
    limit: int
        The size of the TOP-K.
    """

    def __init__(self, query: str, limit: int = 10):
        self._exprs = translateQuery(parseQuery(query)).algebra.p.p.p.expr
        self._limit = limit
        self._keys = []
        for index, order_condition in enumerate(self._exprs):
            if order_condition.order is None or order_condition.order == "ASC":
                order = "ASC"
            else:
                order = "DESC"
            self._keys.append((f"__order_condition_{index}", order))
        self._topk = TOPKStruct(self._keys, limit=limit)

    @property
    def key(self) -> List[str]:
        return self._keys

    def insert(self, mappings: Dict[str, str]) -> None:
        """
        Inserts a solution mappings in the TOP-K data structure.

        Parameters
        ----------
        mappings: Dict[str, str]
            A solution mappings.
        """
        self._topk.insert(mappings)

    def update_threshold(self, saved_plan: str) -> str:
        """
        Updates the lowest TOP-K solution in the saved plan received by the
        server.

        Parameters
        ----------
        saved_plan: str
            The saved plan of the query received by the server.
        """
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
        """
        Returns the TOP-K as an ordered list of solutions mappings.

        Parameters
        ----------
        List[Dict[str, str]]
            A list of solutions mappings.
        """
        solutions = list()
        for mappings in self._topk.flatten():
            for key, _ in self._keys:
                del mappings[key]
            solutions.append(mappings)
        return solutions


class SaGePartialTopK(Approach):
    """
    This class executes SPARQL TOP-K queries against a preemptable SPARQL
    endpoint that support a partial TOP-K iterator. This approach consists in
    collaborating with the server to compute TOP-K queries. After each quantum,
    the server sends a partial TOP-K to the client, the client merges the TOP-K
    computed by the server with its own TOP-K, and so on until the query
    is completed. To improve performance, the client also sends the lowest
    solution in the TOP-K to the server. Thus, the server can use this
    information to perform early pruning and to avoid transferring useless
    solutions to the client.

    Parameters
    ----------
    name: str
        The name of the approach. It is used to differentiate between the
        different approaches.
    config: Dict[str, Any]
        The configuration file of the experimental study. It is used to
        retrieve the URL of the endpoint and the name of the RDF graph.
    """

    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        super(SaGePartialTopK, self).__init__(name)
        self._endpoint = config["endpoints"]["sage"]["url"]
        self._graph = config["endpoints"]["sage"]["graph"]

    def execute_query(
        self, query: str, spy: Spy, **kwargs
    ) -> List[Dict[str, str]]:
        """
        Executes a SPARQL TOP-K query against a preemptable SPARQL endpoint.

        Parameters
        ----------
        query: str
            A SPARQL TOP-K query.
        spy: Spy
            An object used to collect statistics about the execution of the
            query.

        Returns
        -------
            The result of the query.
        """
        limit = kwargs.setdefault("limit", 10)
        quota = kwargs.setdefault("quota", None)
        force_order = kwargs.setdefault("force_order", False)
        early_pruning = kwargs.setdefault("early_pruning", False)
        stateless = kwargs.setdefault("stateless", True)
        max_limit = kwargs.setdefault("max_limit", None)

        topk = TOPKOperator(query, limit=limit)

        orderby_variables = self.__get_orderby_variables__(query)

        query = self.__set_projection__(query, ['*'])
        query = self.__set_limit__(query, limit=limit)

        logging.info(f"{self.name} - query sent to the server:\n{query}")
        logging.info(f"{self.name} - limit = {limit} (max={max_limit})")
        logging.info(f"{self.name} - quota = {quota} (ms)")
        logging.info(f"{self.name} - stateless = {stateless}")
        logging.info(f"{self.name} - early-pruning = {early_pruning}")

        headers = {
            "accept": "text/html",
            "content-type": "application/json"}
        payload = {
            "query": query,
            "defaultGraph": self._graph,
            "next": None,
            "quota": quota,
            "forceOrder": force_order,
            "topkStrategy": "partial_topk",
            "earlyPruning": early_pruning,
            "stateless": stateless,
            "maxLimit": max_limit}

        has_next = True

        start = time.time()

        while has_next:
            data = json.dumps(payload)
            response = requests.post(
                self._endpoint, headers=headers, data=data).json()

            has_next = response["next"] is not None

            # merges the TOP-K with the client's TOP-K
            for solution in response["bindings"]:
                topk.insert(solution)

            # updates the threshold in the saved plan
            if has_next:
                payload["next"] = topk.update_threshold(response["next"])

            spy.report_http_calls(1)
            spy.report_data_transfer(sys.getsizeof(data))
            spy.report_data_transfer(sys.getsizeof(json.dumps(response)))
            spy.report_loading_time(response["stats"]["resuming_time"])
            spy.report_saving_time(response["stats"]["saving_time"])

        results = topk.flatten()

        elapsed_time = (time.time() - start) * 1000

        spy.report_execution_time(elapsed_time)
        spy.report_solutions(len(results))

        solutions = []  # solutions are formated to make the validation easier
        for mappings in results:
            solution = {}
            for key, value in mappings.items():
                if key in orderby_variables:  # to make the validation easier
                    if value.startswith('"') and value.endswith('"'):
                        solution[key] = value[1:-1]
                    else:
                        solution[key] = value
            solutions.append(solution)

        return solutions
