from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, sum, max
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)
from src.modules.exceptions import DataProcessingError
from typing import Tuple
from datetime import datetime


class DataAnalyzer:
    """데이터 분석 클래스"""

    def __init__(self, spark: SparkSession):
        if not spark:
            self.spark = SparkSession.builder.appName("DataAnalyzer").getOrCreate()
        else:
            self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)

    def generate_sample_data(self, num_rows: int) -> DataFrame:
        """샘플 데이터 생성 최적화"""
        try:
            schema = StructType(
                [
                    StructField("id", IntegerType(), False),
                    StructField("value", IntegerType(), True),
                    StructField("category", StringType(), True),
                    StructField("amount", FloatType(), True),
                ]
            )

            # 배치 크기로 데이터 생성
            batch_size = 100000
            data = []
            for i in range(0, num_rows, batch_size):
                end = min(i + batch_size, num_rows)
                batch = [
                    (j, j % 100, chr(65 + j % 4), float(j % 1000))
                    for j in range(i, end)
                ]
                data.extend(batch)

                if len(data) >= batch_size:
                    df = self.spark.createDataFrame(data, schema)
                    data = []

            if data:
                df = self.spark.createDataFrame(data, schema)

            return df.repartition(10)  # 적절한 파티션 수로 재분할

        except Exception as e:
            raise DataProcessingError(f"샘플 데이터 생성 실패: {str(e)}")

    def analyze_data(self, df: DataFrame, description: str) -> Tuple[DataFrame, float]:
        """데이터 분석 수행"""
        try:
            start_time = datetime.now()

            # 기본 통계 계산
            stats = df.agg(
                avg("value").alias("평균_값"),
                sum("amount").alias("총_금액"),
                max("value").alias("최대_값"),
            )

            # 카테고리별 집계
            category_stats = df.groupBy("category").agg(
                avg("value").alias("카테고리별_평균"),
                sum("amount").alias("카테고리별_총액"),
            )

            execution_time = (datetime.now() - start_time).total_seconds()

            self.logger.info(f"=== {description} 분석 결과 ===")
            self.logger.info(f"처리 시간: {execution_time:.2f}초")
            self.logger.info("\n기본 통계:")
            stats.show()
            self.logger.info("\n카테고리별 통계:")
            category_stats.show()

            return category_stats, execution_time

        except Exception as e:
            raise DataProcessingError(f"데이터 분석 중 오류 발생: {str(e)}")

    def analyze_partitions(
        self, df: DataFrame, description: str
    ) -> Tuple[DataFrame, DataFrame]:
        """파티션 분석"""
        try:
            self.logger.info(f"\n=== {description} 파티션 분석 ===")
            initial_partitions = df.rdd.getNumPartitions()
            self.logger.info(f"초기 파티션 수: {initial_partitions}")

            # repartition 테스트
            df_repartitioned = df.repartition(10)

            # coalesce 테스트
            df_coalesced = df.coalesce(5)

            self.logger.info(
                f"Repartition 후 파티션 수: {df_repartitioned.rdd.getNumPartitions()}"
            )
            self.logger.info(
                f"Coalesce 후 파티션 수: {df_coalesced.rdd.getNumPartitions()}"
            )

            return df_repartitioned, df_coalesced

        except Exception as e:
            raise DataProcessingError(f"파티션 분석 중 오류 발생: {str(e)}")
