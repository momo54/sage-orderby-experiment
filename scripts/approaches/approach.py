from abc import ABC, abstractmethod
from typing import Dict, List
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery

from spy import Spy


class Approach(ABC):

    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def __set_projection__(self, query: str, variables: List[str]) -> str:
        """
        Updates the SELECT clause of the query.

        Parameters
        ----------
        query: str
            A SPARQL query.
        variables: List[str]
            The variables that need to be projected.

        Returns
        -------
        str
            Returns the SPARQL query with the updated SELECT clause.
        """
        prefixes = query.split("SELECT")[0]
        projection = " ".join(variables)
        where_clause = query.split("WHERE")[1]
        return f"{prefixes}SELECT {projection} WHERE {where_clause}"

    def __set_limit__(self, query: str, limit: int = 10) -> str:
        """
        Updates the LIMIT clause of the query.

        Parameters
        ----------
        query: str
            A SPARQL query.
        limit: int - (default = 10)
            The value with which to update the LIMIT clause.

        Returns
        -------
        str
            Returns the SPARQL query with the updated LIMIT clause.
        """
        if "LIMIT" in query:
            query = query.split("LIMIT")[0]
        return f"{query} LIMIT {limit}"

    def __get_orderby_variables__(self, query: str) -> List[str]:
        """
        Returns the variables that appear in the ORDER BY clause of the query.

        Parameters
        ----------
        query: str
            A SPARQL TOP-K query.

        Returns
        -------
        List[str]
            Returns the variables that appear in the ORDER BY clause of the
            query.
        """
        variables = []
        for expr in translateQuery(parseQuery(query)).algebra.p.p.p.expr:
            for variable in expr._vars:
                variables.append(variable.n3())
        return variables

    @abstractmethod
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
        pass
