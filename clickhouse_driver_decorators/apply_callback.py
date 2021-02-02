# -*- coding: utf8 -*-

from typing import Generator, Callable

from clickhouse_driver_decorators.abstract_rows_processable_decorator import AbstractRowProcessableDecorator


class apply_callback(AbstractRowProcessableDecorator):

    def __init__(self, on_row_callable: Callable[[tuple], Generator]):
        super().__init__()

        self.__on_row_callable = on_row_callable

    def _process_row(self, row: tuple) -> Generator:
        yield from self.__on_row_callable(row)
