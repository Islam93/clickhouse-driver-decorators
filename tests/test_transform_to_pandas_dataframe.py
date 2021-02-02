# -*- coding: utf8 -*-

import pandas as pd
from unittest.mock import Mock

from clickhouse_driver_decorators import transform_to_pandas_dataframe


class TestTransformToPandasDataframe:

    def test_callback_applied(self):
        expected_df = self.__get_expected_data()
        actual_df = self.__decorated()

        assert isinstance(actual_df, pd.DataFrame)
        assert expected_df.equals(actual_df)

    @transform_to_pandas_dataframe()
    def __decorated(self):
        return self.__get_mocked_client()()

    @staticmethod
    def __get_expected_data():
        expected_columns = ['col1', 'col2', 'col3']
        expected_data = [
            (1, 'val12', 'val13'),
            (2, 'val22', 'val23'),
            (3, 'val32', 'val33'),
        ]

        return pd.DataFrame(data=expected_data, columns=expected_columns)

    @staticmethod
    def __get_mocked_client():
        mocked_client = Mock()
        mocked_client.return_value = (
            [
                (1, 'val12', 'val13'),
                (2, 'val22', 'val23'),
                (3, 'val32', 'val33'),
            ],
            [
                ('col1', 'UInt32'), ('col2', 'String'), ('col3', 'Nullable(String)'),
            ],
        )

        return mocked_client
