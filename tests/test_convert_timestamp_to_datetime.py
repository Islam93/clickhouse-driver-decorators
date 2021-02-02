# -*- coding: utf8 -*-

from datetime import datetime
from typing import Generator

from pytz import timezone
from unittest.mock import Mock

from clickhouse_driver_decorators import convert_timestamp_to_datetime


class TestConvertTimestampToDatetime:

    def test_timestamp_converted(self):
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

    @convert_timestamp_to_datetime(columns_to_convert={'col3': 'Europe/Moscow'})
    def __decorated(self):
        return self.__get_mocked_client()()

    @staticmethod
    def __get_expected_data():
        expected_head = [('col1', 'UInt32'), ('col2', 'String'), ('col3', 'UInt32')]
        expected_row = (1, 'val2', datetime.fromtimestamp(1612191656, tz=timezone('Europe/Moscow')))

        return expected_head, expected_row

    @staticmethod
    def __get_mocked_client():
        mocked_client = Mock()
        mocked_client.return_value = (
            [
                (1, 'val2', 1612191656),
            ],
            [
                ('col1', 'UInt32'), ('col2', 'String'), ('col3', 'UInt32'),
            ],
        )

        return mocked_client
