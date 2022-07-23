from pandas import DataFrame


class Spy():
    """
    This class is used to collect statistics about queries execution.

    Parameters
    ----------
    query: str
        The SPARQL TOP-K query for which we are collected statistics.
    execution_time: float
        The time spent on the execution of the query (seconds).
    data_transfer: float
        The amount of data transferred during the execution of the query
        (bytes).
    http_calls: int
        The number of HTTP calls sent to the server during the execution of the
        query.
    nb_solutions: int
        The number of solutions returned by the query.
    loading_time: float
        The time spent resuming saved plans by the server.
    saving_time: float
        The time spent saving query plans by the server.
    """

    def __init__(self, query: str):
        self._query = query
        self._execution_time = 0.0
        self._data_transfer = 0.0
        self._http_calls = 0
        self._nb_solutions = 0
        self._resuming_time = 0.0
        self._saving_time = 0.0

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

    def report_loading_time(self, value: float) -> None:
        self._resuming_time += value

    def report_saving_time(self, value: float) -> None:
        self._saving_time += value

    def to_dataframe(self) -> DataFrame:
        columns = [
            "query", "execution_time", "data_transfer",
            "http_calls", "solutions", "resuming_time", "saving_time"]
        rows = [[
            self._query, self._execution_time, self._data_transfer,
            self._http_calls, self._nb_solutions, self._resuming_time,
            self._saving_time]]
        return DataFrame(rows, columns=columns)
