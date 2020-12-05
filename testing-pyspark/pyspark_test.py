from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame


def get_spark_session():
    spark = SparkSession.builder.appName("integration_test").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 1)
    return spark


def assert_dataframes_equal(df, df_ref, order_cols=None):
    cols = df_ref.columns

    missing_cols = set(cols) - set(df.columns)
    if missing_cols:
        print_full_diff(df_ref, df)
        print_column_diff(df_ref, df)
        assert False

    assert_column_types(df_ref, df)

    if not order_cols:
        order_cols = cols

    # order datasets
    actual_df = df.select(cols).orderBy(order_cols)
    expected_df = df_ref.orderBy(order_cols)

    actual = actual_df.collect()
    expected = expected_df.collect()
    if actual != expected:
        print_full_diff(expected_df, actual_df)
        assert False
    assert True


def assert_column_types(df_ref, df):
    type_dict_e = {f.name: f.dataType for f in df_ref.schema.fields}
    type_dict_a = {f.name: f.dataType for f in df.schema.fields}
    msg = "#" * 40 + "\n# Column type mismatch" + "\n" + "#" * 39
    msg += "\n column: got -> expected"
    match = True
    for col in df_ref.columns:
        expected = type_dict_e[col]
        got = type_dict_a[col]

        got, expected = ignore_contains_null_diff(got, expected)

        if got == expected:
            continue

        match = False
        msg += f"\n {col}: {got} -> {expected}"
    if not match:
        print(msg)
        assert False


def ignore_contains_null_diff(got, expected):
    """
    There is a case in map types where the types match
    but they differ in the parameter `.valueContainsNull`
    This function is meant to go around this problem by setting
    them both to True.
    """
    try:
        if got.valueContainsNull != expected.valueContainsNull:
            # Ignoreing this error
            got.valueContainsNull = True
            expected.valueContainsNull = True
    except AttributeError:
        pass

    return got, expected


def print_full_diff(expected_df, actual_df):
    print("#" * 40, "\n# Full Results", "\n" + "#" * 39)
    print("Expected")
    expected_df.show()
    print("Got")
    actual_df.show()


def print_column_diff(expected_df, actual_df):
    print("#" * 40, "\n# Missing Columns", "\n" + "#" * 39)
    missing_cols = set(expected_df.columns) - set(actual_df.columns)
    print(f"\nMissing: {missing_cols}\n")
