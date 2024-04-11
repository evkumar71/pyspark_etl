import pytest
import datetime
from typing import Iterable, Any
from pyspark.testing import assertDataFrameEqual
from pyspark.sql import SparkSession
from metrics import Metrics
from appcontext import AppContext


class TestSparkDF:
    @pytest.fixture
    def spark_fixture(self):
        spark = (SparkSession.
                 builder.
                 appName("Testing PySpark Example").
                 getOrCreate())
        yield spark

    def test_simple(self, spark_fixture):
        test_data: Iterable[Any] = [
            {'date': datetime.date(year=2014, month=7, day=25), 'open': 8.229999542236328,
             'close': 8.180999755859375, 'maxClose': 8.180999755859375},
            {'date': datetime.date(year=2014, month=9, day=16), 'open': 10.088000297546387,
             'close': 10.08800029754638, 'maxClose': 10.088000297546387},
            {'date': datetime.date(year=2014, month=12, day=13), 'open': 8.472000122070312,
             'close': 8.475000381469727, 'maxClose': 8.475000381469727}
        ]
        expected_data: Iterable[Any] = [
            {'date': datetime.date(year=2014, month=9, day=16), 'maxClose': 10.088000297546387}
        ]
        test_df = spark_fixture.createDataFrame(data=test_data)
        expected_df = spark_fixture.createDataFrame(data=expected_data)
        context = AppContext("config/config.json")
        met = Metrics(context)
        actual = met.highest_close(test_df)
        assertDataFrameEqual(actual, expected_df)
