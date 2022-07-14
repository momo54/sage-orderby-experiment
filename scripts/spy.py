from pandas import DataFrame


class Spy():

    def __init__(self, query: str):
        self._query = query
        self._execution_time = 0.0
        self._data_transfer = 0.0
        self._http_calls = 0
        self._nb_solutions = 0

    @property
    def execution_time(self) -> float:
        return self._execution_time

    @property
    def data_transfer(self) -> float:
        return self._data_transfer

    @property
    def http_calls(self) -> int:
        return self._http_calls

    @property
    def solutions(self) -> int:
        return self._nb_solutions

    def report_execution_time(self, value: float) -> None:
        self._execution_time += value

    def report_data_transfer(self, value: float) -> None:
        self._data_transfer += value

    def report_http_calls(self, value: int) -> None:
        self._http_calls += value

    def report_solutions(self, value: int) -> None:
        self._nb_solutions += value

    def to_dataframe(self) -> DataFrame:
        columns = [
            'query', 'execution_time', 'data_transfer',
            'http_calls', 'solutions']
        rows = [[
            self._query, self._execution_time, self._data_transfer,
            self._http_calls, self._nb_solutions]]
        return DataFrame(rows, columns=columns)
