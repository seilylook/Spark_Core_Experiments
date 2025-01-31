# groupBy, agg, count, sum, avg, max, min
from typing import Union, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum, avg, max, min
from pyspark.sql.types import NumericType
from src.services.validate import validate_dataframe, validate_column_exists


class AggregationService:
    """A service class that provides various aggregation operations for Titanic DataFrame.

    This class implements common aggregation patterns like grouping and aggregating
    with proper error handling and type checking
    """

    @staticmethod
    def group_by_column(
        df: DataFrame,
        group_col: str,
        agg_cols: Union[str, List[str]] = None,
        agg_funcs: Union[str, List[str]] = None,
    ) -> DataFrame:
        """Groups DataFrame by specified column and applies aggregation functions.

        Args:
            df: Input DataFrame
            group_col: Column to group by
            agg_cols: Columns to aggregate (optional)
            agg_funcs: Aggregation functions to apply (optional)
                Supported functions: ['count', 'sum', 'avg', 'max', 'min']

        Returns:
            DataFrame: Grouped and aggregated DataFrame

        Raises:
            ValueError: If invalid aggregation function is specified
            TypeError: If input types are incorrect
        """

        # Validate Inputs
        validate_dataframe(df)
        validate_column_exists(df, group_col)

        # Initialize grouping
        grouped = df.groupby(col(group_col))

        # If no aggregation is specified, return count by group
        if not agg_cols:
            return grouped.count()

        # Validate aggregation inputs
        if not isinstance(agg_cols, (str, list)):
            raise TypeError("agg_cols must be a string or list of strings")
        if not isinstance(agg_funcs, (str, list)):
            raise TypeError("agg_funcs must be a string or list of strings")

        # Convert single value to lists
        agg_cols = [agg_cols] if isinstance(agg_cols, str) else agg_cols
        agg_funcs = [agg_funcs] if isinstance(agg_funcs, str) else agg_funcs

        # Validate column existance
        for col_name in agg_cols:
            validate_column_exists(df, col_name)

        # Map of supported aggregation functions
        agg_func_map = {
            "count": count,
            "sum": sum,
            "avg": avg,
            "max": max,
            "min": min,
        }

        # Validate aggregation functions
        invalid_funcs = set(agg_funcs) - set(agg_func_map.keys())
        if invalid_funcs:
            raise ValueError(f"Unsupported aggregation functions: {invalid_funcs}")

        # Build aggregation expressions
        agg_exprs = []
        for col_name in agg_cols:
            col_type = df.schema[col_name].dataType

            for func_name in agg_funcs:
                # Skip non-numeric columns for numeric aggregations
                if func_name in ["sum", "avg"] and not isinstance(
                    col_type, NumericType
                ):
                    continue

                agg_exprs.append(
                    agg_func_map[func_name](col(col_name)).alias(
                        f"{func_name}_{col_name}"
                    )
                )

        return grouped.agg(*agg_exprs) if agg_exprs else grouped.count()

    @staticmethod
    def group_by_boolean(df: DataFrame, column: str) -> DataFrame:
        """Groups DataFrame by boolean condition (column == 1).

        This is a specific implementation for boolean/binary grouping.

        Args:
            df: Input DataFrame
            column: Column to group by (should contain 0/1 values)

        Returns:
            DataFrame: Grouped DataFrame with counts

        Raises:
            ValueError: If column contains non-binary values
        """
        validate_dataframe(df)
        validate_column_exists(df, column)

        # Validate column contains only binary values
        distinct_values = set(
            df.select(column).distinct().rdd.flatMap(lambda x: x).collect()
        )
        if not distinct_values.issubset({0, 1}):
            raise ValueError(f"Column '{column}' must contain only binary values (0/1)")

        return df.groupBy(col(column) == 1).count()
