from pyspark.sql import DataFrame
from app.src.config.mongodb import MONGODB_CONFIG


class MongoDBServiceClass:
    @staticmethod
    def save_to_mongodb(df: DataFrame, mode: str = "append") -> None:
        """Save DataFrame to MongoDB"""
        df.write.format("mongodb").mode(mode).option(
            "database", MONGODB_CONFIG["DATABASE"]
        ).option("collection", MONGODB_CONFIG["COLLECTION"]).save()

    @staticmethod
    def read_from_mongodb(spark, query: dict = None) -> DataFrame:
        """Read data from MongoDB with optional query"""
        read_options = {
            "database": MONGODB_CONFIG["DATABASE"],
            "collection": MONGODB_CONFIG["COLLECTION"],
        }
        if query:
            read_options["pipeline"] = str(query)

        return spark.read.format("mongodb").options(**read_options).load()

    @staticmethod
    def upsert_to_mongodb(df: DataFrame, key_columns: list) -> None:
        """Upsert DataFrame to MongoDB based on key columns"""
        # MongoDB의 replace 모드를 사용하여 업서트 수행
        df.write.format("mongodb").mode("overwrite").option(
            "database", MONGODB_CONFIG["DATABASE"]
        ).option("collection", MONGODB_CONFIG["COLLECTION"]).option(
            "replaceDocument", "true"
        ).option(
            "idFieldList", ",".join(key_columns)
        ).save()
