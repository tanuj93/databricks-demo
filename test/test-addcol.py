# test-addcol.py
import pytest

from test.spark import get_spark
from test.addcol import with_status

class TestAppendCol(object):

    def test_with_status(self):
        source_data = [
            ("pete", "pan", "peter.pan@databricks.com"),
            ("jason", "argonaut", "jason.argonaut@databricks.com")
        ]
        source_df = get_spark().createDataFrame(
            source_data,
            ["first_name", "last_name", "email"]
        )

        actual_df = with_status(source_df)

        expected_data = [
            ("pete", "pan", "peter.pan@databricks.com"),
            ("jason", "argonaut", "jason.argonaut@databricks.com")
        ]
        expected_df = get_spark().createDataFrame(
            expected_data,
            ["first_name", "last_name", "email"]
        )

        assert(expected_df.collect() == actual_df.collect())
