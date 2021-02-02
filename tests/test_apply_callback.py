# -*- coding: utf8 -*-

from typing import Generator

from unittest.mock import Mock

from clickhouse_driver_decorators import apply_callback


def replace_no_value_with_none(row: tuple) -> Generator:
    yield tuple([None if v == 'no_value' else v for v in row])


class TestApplyCallback:

    def test_callback_applied(self):
        expected_head, expected_row = self.__get_expected_data()

        actual = self.__decorated()
        actual_head = {}
        actual_row = {}
        for i, row in enumerate(actual):
            if i == 0:
                actual_head = row
            elif i == 1:
                actual_row = row

        assert isinstance(actual, Generator)
        assert isinstance(actual_row, tuple)
        assert expected_head == actual_head
        assert expected_row == actual_row

    @apply_callback(on_row_callable=replace_no_value_with_none)
    def __decorated(self):
        return self.__get_mocked_client()()

    @staticmethod
    def __get_expected_data():
        expected_head = [('col1', 'UInt32'), ('col2', 'String'), ('col3', 'Nullable(String)')]
        expected_row = (1, 'val2', None)

        return expected_head, expected_row

    @staticmethod
    def __get_mocked_client():
        mocked_client = Mock()
        mocked_client.return_value = (
            [
                (1, 'val2', 'no_value'),
            ],
            [
                ('col1', 'UInt32'), ('col2', 'String'), ('col3', 'Nullable(String)'),
            ],
        )

        return mocked_client
