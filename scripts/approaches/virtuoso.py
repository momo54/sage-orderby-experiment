import logging
import time
import json
import sys

from typing import Dict, Any, List
from SPARQLWrapper import SPARQLWrapper, JSON

from approaches.approach import Approach
from spy import Spy


class Virtuoso(Approach):
    """
    This class executes SPARQL TOP-K queries against the Virtuoso endpoint.

    Parameters
    ----------
    name: str
        The name of the approach. It is used to differentiate between the
        different approaches.
    config: Dict[str, Any]
        The configuration file of the experimental study. It is used to
        retrieve the URL of the endpoint and the name of the RDF graph.
    """

    def __init__(self, name: str, config: Dict[str, Any], **kwargs) -> None:
        super().__init__(name)
        self._endpoint = config["endpoints"]["virtuoso"]["url"]
        self._graph = config["endpoints"]["virtuoso"]["graph"]

    def __insert_force_order_pragma__(self, query: str) -> str:
        return f'DEFINE sql:select-option "order" {query}'

    def execute_query(
        self, query: str, spy: Spy, **kwargs
    ) -> List[Dict[str, str]]:
        """
        Executes a SPARQL TOP-K query against the Virtuoso endpoint.

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
        force_order = kwargs.setdefault("force_order", False)

        orderby_variables = self.__get_orderby_variables__(query)

        if force_order:
            query = self.__insert_force_order_pragma__(query)
        query = self.__set_projection__(query, ["*"])
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
                # to make the validation easier
                if f"?{key}" in orderby_variables:
                    solution[f"?{key}"] = str(mappings[key]["value"])
            solutions.append(solution)
        return solutions
