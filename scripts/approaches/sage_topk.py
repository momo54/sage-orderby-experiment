import requests
import json
import time
import sys
import logging

from typing import Dict, Any, List

from approaches.approach import Approach
from spy import Spy


class SaGeTopK(Approach):

    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        super().__init__(name)
        self._endpoint = config["endpoints"]["sage"]["url"]
        self._graph = config["endpoints"]["sage"]["graph"]

    def execute_query(
        self, query: str, spy: Spy, **kwargs
    ) -> List[Dict[str, str]]:
        limit = kwargs.setdefault("limit", 10)
        quota = kwargs.setdefault("quota", None)
        force_order = kwargs.setdefault("force_order", False)

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
            "forceOrder": force_order}

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
