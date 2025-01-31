from dataclasses import dataclass


@dataclass
class SparkConfig:
    """Spark 설정을 위한 데이터 클래스"""

    app_name: str
    master_url: str = "local[*]"
    log_level: str = "WARN"
    shuffle_partitions: int = 200

    def get_configs(self) -> dict:
        return {
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.log.level": self.log_level,
        }
