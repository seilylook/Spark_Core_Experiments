# exceptions.py
from typing import Optional


class SparkSessionError(Exception):
    """Spark 세션 관련 예외를 처리하는 기본 예외 클래스"""

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        self.message = message
        self.error_code = error_code
        self.original_error = original_error
        super().__init__(self.message)

    def __str__(self) -> str:
        base_message = (
            f"[{self.error_code}] {self.message}" if self.error_code else self.message
        )
        if self.original_error:
            return f"{base_message} (Original error: {str(self.original_error)})"
        return base_message


class SparkConnectionError(SparkSessionError):
    """Spark 클러스터 연결 실패 시 발생하는 예외"""

    def __init__(
        self,
        message: str,
        host: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        error_code = "SPARK_CONN_001"
        detailed_message = f"Spark 클러스터 연결 실패: {message}"
        if host:
            detailed_message += f" (Host: {host})"
        super().__init__(detailed_message, error_code, original_error)


class SparkConfigurationError(SparkSessionError):
    """Spark 설정 관련 오류 시 발생하는 예외"""

    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        error_code = "SPARK_CONF_001"
        detailed_message = f"Spark 설정 오류: {message}"
        if config_key:
            detailed_message += f" (Config Key: {config_key})"
        super().__init__(detailed_message, error_code, original_error)


class DataProcessingError(Exception):
    """데이터 처리 관련 예외를 처리하는 기본 예외 클래스"""

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        details: Optional[dict] = None,
    ):
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        base_message = (
            f"[{self.error_code}] {self.message}" if self.error_code else self.message
        )
        if self.details:
            return f"{base_message} - Details: {self.details}"
        return base_message


class DataValidationError(DataProcessingError):
    """데이터 유효성 검사 실패 시 발생하는 예외"""

    def __init__(self, message: str, invalid_fields: Optional[list] = None):
        error_code = "DATA_VAL_001"
        details = {"invalid_fields": invalid_fields} if invalid_fields else None
        super().__init__(message, error_code, details)


class DataTransformationError(DataProcessingError):
    """데이터 변환 중 발생하는 예외"""

    def __init__(self, message: str, transformation_step: Optional[str] = None):
        error_code = "DATA_TRANS_001"
        details = {"step": transformation_step} if transformation_step else None
        super().__init__(message, error_code, details)
