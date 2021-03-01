### clickhouse-driver-decorators

![license](https://img.shields.io/badge/license-MIT-brightgreen)
![python](https://img.shields.io/badge/python-%3E%3D3.5-blue)
![coverage](https://img.shields.io/badge/coverage-100%25-green)

Bunch of decorators to decorate [clickhouse-driver's](https://github.com/mymarilyn/clickhouse-driver) `execute` method


#### Available decorators:
1. `add_column_names` -- converts each row to dict or namedtuple with column names as keys;
2. `apply_callback` -- applies a given callback to each row of result;
3. `convert_string_to_datetime` -- converts string datetime to python datetime object in a needed timezone for a given column. String datetime have to be in UTC;
4. `convert_timestamp_to_datetime` -- converts timestamp to python datetime object in a needed timezone for a given column.
5. `transform_to_pandas_dataframe` -- creates pandas frame based on result from db


#### Examples
For a closer look, check out the [tests](tests)
```python
from typing import Generator

from pandas import DataFrame
from clickhouse_driver import Client

from clickhouse_driver_decorators import *


def replace_empty_with_none(row: tuple) -> Generator:
    yield tuple([None if v == '' else v for v in row])


class A:
    def __init__(self, client: Client):
        self.__client = client

    def get_data(self) -> Generator[]:
        return self.__decorated_execute("""
            select
                toUnixTimestamp(now()) as now_ts,
                formatDateTime(now(), '%F %T', 'UTC') as now_str,
                '' as empty
        """)
        
    @add_column_names()
    @apply_callback(on_row_callable=replace_empty_with_none)
    @convert_timestamp_to_datetime(columns_to_convert={'now_ts': 'Europe/Moscow'})
    @convert_string_to_datetime(date_format='%Y-%m-%d %H:%M:%S', columns_to_convert={'now_str': 'Europe/Moscow'})
    def __decorated_execute(self, query: str):
        return self.__client.execute(query, with_column_types=True)


class B:
    def __init__(self, client: Client):
        self.__client = client

    def get_pandas_df(self) -> DataFrame:
        return self.__decorated_execute("select 'val1' as col1, 'val2' as col2")
        
    @transform_to_pandas_dataframe()
    def __decorated_execute(self, query: str):
        return self.__client.execute(query, with_column_types=True)
```
