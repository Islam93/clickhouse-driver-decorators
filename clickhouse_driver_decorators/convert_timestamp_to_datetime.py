# -*- coding: utf8 -*-

from datetime import datetime
from typing import Generator

try:
    from pytz import timezone
except ModuleNotFoundError:
    raise ModuleNotFoundError('Package \'pytz\' is required for \'convert_string_to_datetime\' decorator')

from clickhouse_driver_decorators.abstract_rows_processable_decorator import AbstractRowProcessableDecorator


class convert_timestamp_to_datetime(AbstractRowProcessableDecorator):

    def __init__(self, columns_to_convert: dict):
        super().__init__()

        self.__columns_to_convert = columns_to_convert

    def _process_row(self, row: tuple) -> Generator:
        handled_row = list(row)
        for column_to_convert, to_timezone in self.__columns_to_convert.items():
            try:
                column_index = self._column_names.index(column_to_convert)
            except ValueError:
                continue

            ts = handled_row[column_index]
            dt = datetime.fromtimestamp(ts, tz=timezone(to_timezone))
            handled_row[column_index] = dt

        yield tuple(handled_row)
