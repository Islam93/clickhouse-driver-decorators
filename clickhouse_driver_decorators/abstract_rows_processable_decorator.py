# -*- coding: utf8 -*-

from abc import abstractmethod
from typing import Generator, Callable

from clickhouse_driver_decorators.abstract_decorator import AbstractDecorator


class AbstractRowProcessableDecorator(AbstractDecorator):

    def __call__(self, function: Callable, *args, **kwargs) -> Callable[[tuple], Generator]:
        def decorator(*args, **kwargs) -> Generator:
            query_result = function(*args, **kwargs)

            if isinstance(query_result, Generator):
                for i, row in enumerate(query_result):
                    if i == 0:
                        self._define_column_names(row)
                        yield row
                        continue

                    row = self._prepare_row(row)
                    yield from self._process_row(row)
                return

            columns = query_result[-1]
            self._define_column_names(columns)
            yield columns

            rows = query_result[0]
            for row in rows:
                row = self._prepare_row(row)
                yield from self._process_row(row)

        return decorator

    @abstractmethod
    def _process_row(self, row: tuple) -> Generator:
        pass
