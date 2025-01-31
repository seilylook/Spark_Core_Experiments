# test_exceptions.py
import pytest
from src.modules.exceptions import (
    SparkSessionError,
    SparkConnectionError,
    SparkConfigurationError,
    DataProcessingError,
    DataValidationError,
    DataTransformationError,
)


class TestSparkSessionError:
    def test_basic_error(self):
        error = SparkSessionError("기본 에러 메시지")
        assert str(error) == "기본 에러 메시지"

    def test_error_with_code(self):
        error = SparkSessionError("에러 메시지", "ERR001")
        assert str(error) == "[ERR001] 에러 메시지"

    def test_error_inheritance(self):
        error = SparkSessionError("테스트")
        assert isinstance(error, Exception)


class TestSparkConnectionError:
    def test_basic_connection_error(self):
        error = SparkConnectionError("연결 실패")
        assert "SPARK_CONN_001" in str(error)
        assert "연결 실패" in str(error)

    def test_connection_error_with_host(self):
        error = SparkConnectionError("연결 실패", "localhost:7077")
        assert "Host: localhost:7077" in str(error)

    def test_inheritance(self):
        error = SparkConnectionError("테스트")
        assert isinstance(error, SparkSessionError)


class TestSparkConfigurationError:
    def test_basic_config_error(self):
        error = SparkConfigurationError("설정 오류")
        assert "SPARK_CONF_001" in str(error)
        assert "설정 오류" in str(error)

    def test_config_error_with_key(self):
        error = SparkConfigurationError("설정 오류", "spark.executor.memory")
        assert "Config Key: spark.executor.memory" in str(error)

    def test_inheritance(self):
        error = SparkConfigurationError("테스트")
        assert isinstance(error, SparkSessionError)


class TestDataProcessingError:
    def test_basic_error(self):
        error = DataProcessingError("처리 오류")
        assert str(error) == "처리 오류"

    def test_error_with_code(self):
        error = DataProcessingError("처리 오류", "ERR001")
        assert str(error) == "[ERR001] 처리 오류"

    def test_error_with_details(self):
        details = {"step": "transformation", "row": 42}
        error = DataProcessingError("처리 오류", "ERR001", details)
        assert "Details" in str(error)
        assert "transformation" in str(error)


class TestDataValidationError:
    def test_basic_validation_error(self):
        error = DataValidationError("유효성 검사 실패")
        assert "DATA_VAL_001" in str(error)

    def test_validation_error_with_fields(self):
        invalid_fields = ["age", "email"]
        error = DataValidationError("유효성 검사 실패", invalid_fields)
        assert "age" in str(error)
        assert "email" in str(error)

    def test_inheritance(self):
        error = DataValidationError("테스트")
        assert isinstance(error, DataProcessingError)


class TestDataTransformationError:
    def test_basic_transformation_error(self):
        error = DataTransformationError("변환 실패")
        assert "DATA_TRANS_001" in str(error)

    def test_transformation_error_with_step(self):
        error = DataTransformationError("변환 실패", "데이터 정규화")
        assert "데이터 정규화" in str(error)

    def test_inheritance(self):
        error = DataTransformationError("테스트")
        assert isinstance(error, DataProcessingError)


def test_error_hierarchy():
    """전체 예외 계층 구조 테스트"""
    assert issubclass(SparkConnectionError, SparkSessionError)
    assert issubclass(SparkConfigurationError, SparkSessionError)
    assert issubclass(DataValidationError, DataProcessingError)
    assert issubclass(DataTransformationError, DataProcessingError)
