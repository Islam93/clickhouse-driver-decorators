# -*- coding: utf8 -*-

from typing import Generator
from collections import namedtuple

from clickhouse_driver_decorators.abstract_rows_processable_decorator import AbstractRowProcessableDecorator


class add_column_names(AbstractRowProcessableDecorator):

    def __init__(self, row_to_namedtuple=False):
        super().__init__()

        self.__row_to_namedtuple = row_to_namedtuple
        self.__namedtuple_factory = None

    def _process_row(self, row: tuple) -> Generator:
        if self.__row_to_namedtuple:
            if self.__namedtuple_factory is None:
                self.__namedtuple_factory = namedtuple('Row', self._column_names)

            yield self.__namedtuple_factory(*row)
            return

        yield dict(zip(self._column_names, row))
