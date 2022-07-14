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

    def __set_projection__(self, query: str) -> str:
        variables = []
        for expr in translateQuery(parseQuery(query)).algebra.p.p.p.expr:
            for variable in expr._vars:
                variables.append(variable.n3())
        projection = ' '.join(set(variables))
        prefixes = query.split("SELECT")[0]
        where_clause = query.split("WHERE")[1]
        return f"{prefixes}SELECT {projection} WHERE {where_clause}"

    def __set_limit__(self, query: str, limit: int = 10) -> str:
        if "LIMIT" in query:
            query = query.split("LIMIT")[0]
        return f"{query} LIMIT {limit}"

    @abstractmethod
    def execute_query(
        self, query: str, spy: Spy, **kwargs
    ) -> List[Dict[str, str]]:
        pass
