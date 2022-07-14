import logging
import time
import json
import sys

from typing import Dict, Any, List
from SPARQLWrapper import SPARQLWrapper, JSON

from approaches.approach import Approach
from spy import Spy


class Virtuoso(Approach):

    def __init__(self, name: str, config: Dict[str, Any], **kwargs) -> None:
        super().__init__(name)
        self._endpoint = config["endpoints"]["virtuoso"]["url"]
        self._graph = config["endpoints"]["virtuoso"]["graph"]

    def __insert_force_order_pragma__(self, query: str) -> str:
        return f'DEFINE sql:select-option "order" {query}'

    def execute_query(
        self, query: str, spy: Spy, **kwargs
    ) -> List[Dict[str, str]]:
        limit = kwargs.setdefault("limit", 10)
        force_order = kwargs.setdefault("force_order", False)

        if force_order:
            query = self.__insert_force_order_pragma__(query)
        query = self.__set_projection__(query)
        query = self.__set_limit__(query, limit=limit)

        logging.info(f"{self.name} - query sent to the server:\n{query}")
        logging.info(f"{self.name} - limit = {limit}")

        sparql = SPARQLWrapper(self._endpoint)
        sparql.setQuery(query)
        sparql.addDefaultGraph(self._graph)
        sparql.setReturnFormat(JSON)

        start_time = time.time()
        response = sparql.queryAndConvert()
        elapsed_time = time.time() - start_time

        spy.report_http_calls(1)
        spy.report_data_transfer(sys.getsizeof(json.dumps(response)))
        spy.report_execution_time(elapsed_time)
        spy.report_solutions(len(response["results"]["bindings"]))

        solutions = []
        for mappings in sparql.queryAndConvert()["results"]["bindings"]:
            solution = {}
            for key in mappings:
                solution[f"?{key}"] = str(mappings[key]["value"])
            solutions.append(solution)
        return solutions
