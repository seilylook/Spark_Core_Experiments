from pyspark.sql import DataFrame, SparkSession
import wget
import os
from typing import Union, List, Tuple
import random
import datetime


class DataFrameService:
    def generate_rows(self, num_rows: int):
        for i in range(num_rows):
            yield (
                i,
                random.randint(1, 1000),
                random.random() * 100,
                random.choice(["Kim", "Lee", "Park", "Choi"]),
                datetime.now().strftime("%Y-%m-%d"),
            )

    @staticmethod
    def create_dataframe(
        self,
        spark: SparkSession,
        num_rows: Union[int, None] = None,
        data: Union[List[Tuple], DataFrame, None] = None,
        url: Union[str, None] = None,
        column_names: List[str] = None,
    ) -> DataFrame:
        """Create DataFrame from various data sources: URL or data collection

        Args:
            spark (SparkSession): Active Spark session
            data (Union[List[Tuple], DataFrame, None]): Input data - can be:
                - List of tuples
                - DataFrame
            url (Union[str, None]): URL for CSV file
            column_names (List[str]): Column names for the DataFrame when using tuple data

        Returns:
            DataFrame: Created PySpark DataFrame

        Raises:
            ValueError: If no data is provided
            TypeError: If data type is not supported
            Exception: If there's an error in processing the data
        """
        if num_rows is None and data is None and url is None:
            raise ValueError("Please provide either data or URL")

        # Random Data
        if num_rows:
            spark.createDataFrame(
                self.generate_rows(num_rows),
                ["id", "value1", "value2", "category", "date"],
            )

        # Handle URL
        if url:
            try:
                # Get current working directory
                current_dir = os.getcwd()
                # Create a temporary directory for downloads if it doesn't exist
                data_dir = os.path.join(current_dir, "data")
                os.makedirs(data_dir, exist_ok=True)

                # Download file to temp directory with a fixed filename
                csv_file_path = os.path.join(data_dir, "titanic_data.csv")
                wget.download(url, csv_file_path)

                # Read the CSV file
                df = (
                    spark.read.option("header", "true")
                    .option("inferSchema", "true")
                    .csv(csv_file_path)
                )

                return df
            except Exception as e:
                # Clean up in case of error
                if os.path.exists(csv_file_path):
                    os.remove(csv_file_path)
                raise Exception(f"Error downloading or processing CSV: {str(e)}")

        # Handle DataFrame
        if isinstance(data, DataFrame):
            return data

        # Handle List of tuples
        if isinstance(data, list):
            try:
                if column_names:
                    return spark.createDataFrame(data, schema=column_names)
                return spark.createDataFrame(data)
            except Exception as e:
                raise Exception(f"Error converting data to DataFrame: {str(e)}")

        raise TypeError("Unsupported data type provided")
