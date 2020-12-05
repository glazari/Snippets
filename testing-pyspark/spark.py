import os

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def create_full_sales(spark, sales_path, users_path, out_path):
    sales = spark.read.csv(sales_path, sep="\t", header=True)
    users = spark.read.csv(users_path, sep="\t", header=True)

    df = sales.join(users, on=["user_id"], how="left")

    df.write.mode("overwrite").csv(out_path, sep="\t", header=True)


def transform(sales: DataFrame, users: DataFrame) -> DataFrame:
    df = sales.join(users, on=["user_id"], how="left")
    return df


def test_transform():
    spark = SparkSession.builder.master("local").getOrCreate()

    sales = (
        ("transaction1", "10.0", "user1"),
        ("transaction2", "20.0", "user2"),
    )
    schema = "transaction_id: string,  price: double, user_id: string"
    sales = spark.createDataFrame(sales, schema=schema)

    users = (
        ("user1", "Jonh"),
        ("user2", "Rebeca"),
    )
    schema = "user_id: string, name: string"
    users = spark.createDataFrame(users, schema=schema)

    expected = (
        ("transaction1", "10.0", "user1", "John"),
        ("transaction2", "20.0", "user2", "Rebeca"),
    )
    schema = "transaction_id: string, name: string"


def test_create_full_sales():
    spark = SparkSession.builder.master("local").getOrCreate()
    inputs = {
        "spark": spark,
        "sales_path": "test_sales.csv",
        "users_path": "test_users.csv",
        "out_path": "tmp_out.csv",
    }

    expected = "full_sales_expected.csv"
    create_full_sales(**inputs)

    out_file = [f for f in os.listdir(inputs["out_path"]) if f.startswith("part-")][0]

    expected = open(expected, "r").read()
    got = open(inputs["out_path"] + "/" + out_file, "r").read()

    assert expected == got


if __name__ == "__main__":
    spark = SparkSession.builder.master("local").getOrCreate()
    sales = spark.read.csv("./sales.csv", sep="\t", header=True)
    users = spark.read.csv("./user.csv", sep="\t", header=True)
    df = transform(sales, users)
    df.write.mode("overwrite").csv("./full_sales.csv", sep="\t", header=True)
