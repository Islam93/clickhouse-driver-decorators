# -*- coding: utf8 -*-

from abc import ABC, abstractmethod
from typing import Callable


class AbstractDecorator(ABC):

    def __init__(self):
        self._column_names = None

    @abstractmethod
    def __call__(self, function: Callable, *args, **kwargs):
        pass

    @staticmethod
    def _prepare_row(row) -> tuple:
        return tuple(row) if not isinstance(row, dict) else tuple(row.values())

    def _define_column_names(self, columns: list) -> None:
        try:
            self._column_names = list(map(lambda x: x[0], columns))
        except TypeError:
            raise Exception('Looks like parameter "with_column_types" is False, must be True')
