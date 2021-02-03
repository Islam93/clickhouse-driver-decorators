# -*- coding: utf8 -*-

from typing import Generator

from unittest.mock import Mock

from clickhouse_driver_decorators import add_column_names


class TestAddColumnNames:

    def test_correct_dictionary(self):
        expected_head, expected_row = self.__get_expected_data()

        actual = self.__decorated_to_dictionary()
        actual_head = {}
        actual_row = {}
        for i, row in enumerate(actual):
            if i == 0:
                actual_head = row
            elif i == 1:
                actual_row = row

        assert isinstance(actual, Generator)
        assert isinstance(actual_row, dict)
        assert expected_head == actual_head
        assert expected_row == actual_row

    def test_correct_namedtuple(self):
        expected_head, expected_row = self.__get_expected_data()

        actual = self.__decorated_to_namedtuple()
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
        assert expected_row == actual_row._asdict()

    @add_column_names()
    def __decorated_to_dictionary(self):
        return self.__get_mocked_client()()

    @add_column_names(row_to_namedtuple=True)
    def __decorated_to_namedtuple(self):
        return self.__get_mocked_client()()

    @staticmethod
    def __get_expected_data():
        expected_head = [('col1', 'UInt32'), ('col2', 'String'), ('col3', 'Nullable(String)')]
        expected_row = {'col1': 1, 'col2': 'val2', 'col3': 'val3'}

        return expected_head, expected_row

    @staticmethod
    def __get_mocked_client():
        mocked_client = Mock()
        mocked_client.return_value = (
            [
                (1, 'val2', 'val3'),
            ],
            [
                ('col1', 'UInt32'), ('col2', 'String'), ('col3', 'Nullable(String)'),
            ],
        )

        return mocked_client
