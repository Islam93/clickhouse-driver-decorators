# -*- coding: utf8 -*-

from typing import Generator, Callable

try:
    import pandas as pd
except ModuleNotFoundError:
    raise ModuleNotFoundError('Package \'pandas\' is required for \'transform_to_pandas_dataframe\' decorator')

from clickhouse_driver_decorators.abstract_decorator import AbstractDecorator


class transform_to_pandas_dataframe(AbstractDecorator):

    def __init__(self):
        super().__init__()

        self.__data = []

    def __call__(self, function: Callable, *args, **kwargs) -> Callable[[tuple], pd.DataFrame]:
        def decorator(*args, **kwargs) -> pd.DataFrame:
            query_result = function(*args, **kwargs)

            if isinstance(query_result, Generator):
                for i, row in enumerate(query_result):
                    if i == 0:
                        self._define_column_names(row)
                        continue

                    row = self._prepare_row(row)
                    self.__data.append(row)

                return pd.DataFrame(data=self.__data, columns=self._column_names)

            columns = query_result[-1]
            self._define_column_names(columns)

            rows = query_result[0]
            for row in rows:
                row = self._prepare_row(row)
                self.__data.append(row)

            return pd.DataFrame(data=self.__data, columns=self._column_names)

        return decorator
