import requests
import json
import time
import sys
import logging

from typing import Dict, Any, List

from approaches.approach import Approach
from spy import Spy


class SaGeTopK(Approach):
    """
    This class let the SaGe server compute SPARQL TOP-K queries. Queries are
    sent to the server, the client just follows the next links as defined in
    the Web preemption model.

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
        super().__init__(name)
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
            "topkStrategy": "topk_server",
            "earlyPruning": early_pruning,
            "stateless": stateless,
            "maxLimit": max_limit}

        results = []
        has_next = True

        start = time.time()

        while has_next:
            data = json.dumps(payload)
            response = requests.post(
                self._endpoint, headers=headers, data=data).json()
            results.extend(response["bindings"])

            payload["next"] = response["next"]
            has_next = response["next"] is not None

            spy.report_http_calls(1)
            spy.report_data_transfer(sys.getsizeof(data))
            spy.report_data_transfer(sys.getsizeof(json.dumps(response)))
            spy.report_loading_time(response["stats"]["resuming_time"])
            spy.report_saving_time(response["stats"]["saving_time"])

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
