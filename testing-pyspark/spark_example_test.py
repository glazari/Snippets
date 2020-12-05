from pyspark.sql import SparkSession

from pyspark_test import assert_dataframes_equal
from pyspark_test import get_spark_session
from spark_example import transform


def test_transform():
    spark = get_spark_session()

    sales = (
        ("transaction1", 10.0, "user1"),
        ("transaction2", 20.0, "user2"),
    )
    schema = "transaction_id: string,  price: double, user_id: string"
    sales = spark.createDataFrame(sales, schema=schema)

    users = (
        ("user1", "John"),
        ("user2", "Rebeca"),
    )
    schema = "user_id: string, name: string"
    users = spark.createDataFrame(users, schema=schema)

    expected = (
        ("transaction1", "user1", "John"),
        ("transaction2", "user2", "Rebeca"),
    )
    schema = "transaction_id: string, user_id: string, name: string"
    expected = spark.createDataFrame(expected, schema=schema)

    got = transform(sales, users)

    assert_dataframes_equal(got, expected)
