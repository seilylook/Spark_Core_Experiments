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

    def _log_session_info(self, session: SparkSession) -> None:
        """Spark 세션 정보 로깅"""
        try:
            self.logger.info(f"Spark 버전: {session.version}")
            self.logger.info(f"Application ID: {session.sparkContext.applicationId}")
            self.logger.info(f"Master: {session.sparkContext.master}")

            # 활성 세션 설정 로깅
            active_configs = {
                key: value for key, value in session.sparkContext.getConf().getAll()
            }
            self.logger.info("활성 Spark 설정:")
            for key, value in active_configs.items():
                self.logger.info(f"  {key}: {value}")

            # 리소스 정보 로깅
            self.logger.info(f"기본 병렬성: {session.sparkContext.defaultParallelism}")
            self.logger.info(
                f"사용 가능한 코어 수: {session.sparkContext.defaultMinPartitions}"
            )

        except Exception as e:
            self.logger.warning(f"세션 정보 로깅 중 오류 발생: {str(e)}")

    def _create_session(self) -> SparkSession:
        """Spark 세션 생성 메서드"""
        try:
            builder = SparkSession.builder.appName(self.config.app_name)
            builder = builder.master(self.config.master_url)

            configs = self.config.get_configs()
            for key, value in configs.items():
                builder = builder.config(key, value)
                self.logger.info(f"Spark 설정 적용: {key} = {value}")

            session = builder.getOrCreate()
            self.logger.info(f"Spark 세션 생성 완료: {self.config.app_name}")

            # 세션 정보 로깅
            self._log_session_info(session)

            return session

        except Exception as e:
            self.logger.error(f"Spark 세션 생성 실패: {str(e)}")
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
        """컨텍스트 매니저 종료"""
        if self._session:
            try:
                self.logger.info("Spark 세션 종료 중...")
                self._session.stop()
                self.logger.info("Spark 세션이 성공적으로 종료되었습니다.")
            except Exception as e:
                self.logger.warning(f"세션 종료 중 오류 발생: {str(e)}")
            finally:
                self._session = None
