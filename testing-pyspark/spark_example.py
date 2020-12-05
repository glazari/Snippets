from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

TARGET = "./full_sales.csv"

SOURCES = {
    "sales": "./sales.csv",
    "users": "./user.csv",
}


def main(spark, sources, target):
    inputs = {k: spark.read.csv(v, sep="\t", header=True) for k, v in sources.items()}

    df = transform(**inputs)

    df.write.mode("overwrite").csv(target, sep="\t", header=True)


def transform(sales: DataFrame, users: DataFrame) -> DataFrame:
    df = sales.join(users, on=["user_id"], how="left")
    return df


if __name__ == "__main__":
    spark = SparkSession.builder.master("local").getOrCreate()
    main(spark, SOURCES, TARGET)
