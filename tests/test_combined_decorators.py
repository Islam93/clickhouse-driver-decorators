# -*- coding: utf8 -*-

from datetime import datetime
from typing import Generator

import pandas as pd
from pytz import timezone
from unittest.mock import Mock

from clickhouse_driver_decorators import *


def replace_none_with_zero(row: tuple) -> Generator:
    yield tuple([0 if v is None else v for v in row])


class TestCombinedDecorators:

    def test_combined_to_dicts(self):
        expected_head, expected_row = self.__get_expected_data_for_names_added()

        actual = self.__decorated_to_dict()
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

    @add_column_names()
    @apply_callback(on_row_callable=replace_none_with_zero)
    @convert_timestamp_to_datetime(columns_to_convert={'col4': 'Europe/Moscow'})
    @convert_string_to_datetime(date_format='%Y-%m-%d %H:%M:%S', columns_to_convert={'col3': 'Europe/Moscow'})
    def __decorated_to_dict(self):
        return self.__get_mocked_client()()

    def test_combined_to_namedtuple(self):
        expected_head, expected_row = self.__get_expected_data_for_names_added()

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

    @add_column_names(row_to_namedtuple=True)
    @apply_callback(on_row_callable=replace_none_with_zero)
    @convert_timestamp_to_datetime(columns_to_convert={'col4': 'Europe/Moscow'})
    @convert_string_to_datetime(date_format='%Y-%m-%d %H:%M:%S', columns_to_convert={'col3': 'Europe/Moscow'})
    def __decorated_to_namedtuple(self):
        return self.__get_mocked_client()()

    @staticmethod
    def __get_expected_data_for_names_added():
        expected_head = [('col1', 'UInt32'), ('col2', 'Nullable(UInt32)'), ('col3', 'String'), ('col4', 'UInt32')]
        expected_row = {
            'col1': 1,
            'col2': 0,
            'col3': datetime.strptime('2021-02-01 18:00:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone('UTC')).astimezone(tz=timezone('Europe/Moscow')),
            'col4': datetime.fromtimestamp(1612191656, tz=timezone('Europe/Moscow')),
        }

        return expected_head, expected_row

    def test_combined_to_pandas_frame(self):
        expected_df = self.__get_expected_data_to_pandas_frame()
        actual_df = self.__decorated_to_pandas_frame()

        assert isinstance(actual_df, pd.DataFrame)
        assert expected_df.equals(actual_df)

    @transform_to_pandas_dataframe()
    @add_column_names(row_to_namedtuple=True)
    @apply_callback(on_row_callable=replace_none_with_zero)
    @convert_timestamp_to_datetime(columns_to_convert={'col4': 'Europe/Moscow'})
    @convert_string_to_datetime(date_format='%Y-%m-%d %H:%M:%S', columns_to_convert={'col3': 'Europe/Moscow'})
    def __decorated_to_pandas_frame(self):
        return self.__get_mocked_client()()

    @staticmethod
    def __get_expected_data_to_pandas_frame():
        expected_columns = ['col1', 'col2', 'col3', 'col4']
        expected_data = [(
            1,
            0,
            datetime.strptime('2021-02-01 18:00:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone('UTC')).astimezone(tz=timezone('Europe/Moscow')),
            datetime.fromtimestamp(1612191656, tz=timezone('Europe/Moscow')),
        )]

        return pd.DataFrame(data=expected_data, columns=expected_columns)

    @staticmethod
    def __get_mocked_client():
        mocked_client = Mock()
        mocked_client.return_value = (
            [
                (1, None, '2021-02-01 18:00:00', 1612191656)
            ],
            [
                ('col1', 'UInt32'), ('col2', 'Nullable(UInt32)'), ('col3', 'String'), ('col4', 'UInt32')
            ],
        )

        return mocked_client
