# tests/test_session.py
import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from src.services.session import SparkSessionManager
from src.config.spark import SparkConfig
from src.modules.exceptions import SparkConnectionError


@pytest.fixture
def mock_spark_session():
    with patch("pyspark.sql.SparkSession.builder") as mock_builder:
        mock_session = Mock(spec=SparkSession)
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session
        yield mock_session


@pytest.fixture
def spark_config():
    return SparkConfig(
        app_name="TestApp",
        master_url="local[*]",
        log_level="WARN",
        shuffle_partitions=10,
    )


class TestSparkSessionManager:
    def test_successful_session_creation(self, mock_spark_session, spark_config):
        with SparkSessionManager(spark_config) as session:
            assert session == mock_spark_session

    def test_session_cleanup(self, mock_spark_session, spark_config):
        with SparkSessionManager(spark_config) as session:
            pass
        mock_spark_session.stop.assert_called_once()

    def test_config_application(self, mock_spark_session, spark_config):
        with SparkSessionManager(spark_config) as session:
            configs = spark_config.get_configs()
            # shuffle 파티션 설정 확인
            assert configs["spark.sql.shuffle.partitions"] == "10"
            # 로그 레벨 설정 확인 (Java 옵션으로 설정됨)
            assert (
                "-Dlog4j.rootCategory=WARN" in configs["spark.driver.extraJavaOptions"]
            )

    def test_session_creation_failure(self, spark_config):
        with patch(
            "src.services.session.SparkSessionManager._create_session",
            side_effect=Exception("Connection failed"),
        ):
            with pytest.raises(SparkConnectionError) as exc_info:
                with SparkSessionManager(spark_config):
                    pass
            assert "Connection failed" in str(exc_info.value)

    def test_context_manager_exception_handling(self, mock_spark_session, spark_config):
        with pytest.raises(ValueError):
            with SparkSessionManager(spark_config):
                raise ValueError("Test error")
        mock_spark_session.stop.assert_called_once()
