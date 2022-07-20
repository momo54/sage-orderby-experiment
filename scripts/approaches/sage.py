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
    """
    This class implements a data structure that allows to maitain a
    multiple-keys order between the solutions mappings. It is used to compute
    the TOP-K on the client. This is the baseline in our experimental study.

    Parameters
    ----------
    query: str
        The SPARQL TOP-K query for which we want to compute the TOP-K.
    limit: int
        The size of the TOP-K.
    """

    def __init__(self, query: str, limit: int = 10):
        self._exprs = translateQuery(parseQuery(query)).algebra.p.p.p.expr
        keys = []
        for index, order_condition in enumerate(self._exprs):
            if order_condition.order is None or order_condition.order == "ASC":
                order = "ASC"
            else:
                order = "DESC"
            keys.append((f"__order_condition_{index}", order))
        self._topk = TOPKStruct(keys, limit=limit)

    def __to_rdflib_term__(self, value: str) -> Identifier:
        """
        Formats an RDF term into an RDFLib term. The RDFLib is a module used to
        parse and evaluate SPARQL expressions.

        Parameters
        ----------
        value: str
            An RDF term.

        Returns
        -------
        Identifier
            An RDF term formatted for the RDFLib.
        """
        if value.startswith("http"):
            return URIRef(value)
        elif '"^^http' in value:
            index = value.find('"^^http')
            value = f"{value[0:index+3]}<{value[index+3:]}>"
        return from_n3(value)

    def __eval_rdflib_expr__(
        self, expr: Expr, mappings: Dict[str, str]
    ) -> Any:
        """
        Evaluates a SPARQL expression with the given mappings.

        Parameters
        ----------
        expr: Expr
            A SPARQL expression parsed by the RDFLib.
        mappings: Dict[str, str]
            A solution mappings.

        Returns
        -------
        Any
            The result of evaluating the SPARQL expression with the given
            mappings.
        """
        if isinstance(expr, Variable):
            return mappings[expr.n3()]
        rdflib_mappings = dict()
        for key, value in mappings.items():
            rdflib_mappings[Variable(key[1:])] = self.__to_rdflib_term__(value)
        context = QueryContext(bindings=Bindings(d=rdflib_mappings))
        return expr.eval(context)

    def insert(self, mappings: Dict[str, str]) -> None:
        """
        Inserts a solution mappings in the TOP-K data structure.

        Parameters
        ----------
        mappings: Dict[str, str]
            A solution mappings.
        """
        for index, order_condition in enumerate(self._exprs):
            mappings[f"__order_condition_{index}"] = self.__eval_rdflib_expr__(
                order_condition.expr, mappings)
        self._topk.insert(mappings)

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
            for index in range(len(self._exprs)):
                del mappings[f"__order_condition_{index}"]
            solutions.append(mappings)
        return solutions


class SaGe(Approach):
    """
    This class executes SPARQL TOP-K queries against a preemptable SPARQL
    endpoint that does not support the evaluation of TOP-K queries. Queries
    are sent to the server without the ORDER-BY and LIMIT clauses. Once all
    solutions transferred, the TOP-K is computed by the client.

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

    def __remove_topk__(self, query: str) -> str:
        """
        Removes the ORDER-BY and LIMIT clauses from a SPARQL TOP-K query.

        Parameters
        ----------
        query: str
            A SPARQL TOP-K query.

        Returns
        -------
        str
            A SPARQL TOP-K query without the ORDER-BY and LIMIT clauses.
        """
        return query.split("ORDER")[0]

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

        topk = TOPKOperator(query, limit=limit)  # client-side top-k operator

        orderby_variables = self.__get_orderby_variables__(query)

        query = self.__set_projection__(query, ['*'])
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
            "forceOrder": force_order,
            "earlyPruning": early_pruning}

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
                if key in orderby_variables:  # to make the validation easier
                    if value.startswith('"') and value.endswith('"'):
                        solution[key] = value[1:-1]
                    else:
                        solution[key] = value
            solutions.append(solution)

        return solutions
