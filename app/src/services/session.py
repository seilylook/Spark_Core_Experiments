# src/services/session.py
from pyspark.sql import SparkSession
from typing import Optional
import logging
from src.config.spark import SparkConfig
from src.modules.exceptions import SparkConnectionError


class SparkSessionManager:
    def __init__(self, config: SparkConfig):
        self.config = config
        self._session: Optional[SparkSession] = None
        self._setup_logging()

    def _setup_logging(self) -> None:
        """로깅 설정"""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def _create_session(self) -> SparkSession:
        """Spark 세션 생성 메서드"""
        try:
            builder = SparkSession.builder.appName(self.config.app_name)
            builder = builder.master(self.config.master_url)

            for key, value in self.config.get_configs().items():
                builder = builder.config(key, value)

            session = builder.getOrCreate()
            self.logger.info(f"Spark 세션 생성 완료: {self.config.app_name}")
            return session
        except Exception as e:
            raise SparkConnectionError(
                message=str(e), host=self.config.master_url, original_error=e
            )

    def __enter__(self) -> SparkSession:
        """컨텍스트 매니저 진입"""
        try:
            self._session = self._create_session()
            return self._session
        except Exception as e:
            if isinstance(e, SparkConnectionError):
                raise
            raise SparkConnectionError(
                message=str(e), host=self.config.master_url, original_error=e
            )

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._session:
            self.logger.info("Spark 세션 종료")
            self._session.stop()
