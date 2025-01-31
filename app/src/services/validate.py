from pyspark.sql import DataFrame


def validate_dataframe(df: DataFrame) -> None:
    """Validates if the input is a valid Spark DataFrame

    Args:
        df (DataFrame): The DataFrame to validate

    Raises:
        TypeError: If input is not a Spark DataFrame
        ValueError: If DataFrame is empty
    """

    if not isinstance(df, DataFrame):
        raise TypeError("Input must be a Spark DataFrame")

    if df.rdd.isEmpty():
        raise ValueError("DataFrame can not be empty")


def validate_column_exists(df: DataFrame, column: str) -> None:
    """Validates if the specified column exists in the DataFrame.

    Args:
        df: The DataFrame to check
        column: Column name to validate

    Raises:
        ValueError: If column doesn't exist in DataFrame
    """
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")
