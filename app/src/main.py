import logging
import sys
from src.config.spark import SparkConfig
from src.services.session import SparkSessionManager
from src.modules.exceptions import SparkSessionError, DataProcessingError
from src.services.analyze import DataAnalyzer


def run_analysis(config: SparkConfig, num_rows: int) -> float:
    """분석 실행

    Returns:
        float: 실행 시간(초)
    """
    execution_time = None  # 실행 시간 초기화

    try:
        with SparkSessionManager(config) as spark:
            analyzer = DataAnalyzer(spark)

            # 데이터 생성
            logging.info(f"\n=== {config.app_name} 데이터 생성 ({num_rows:,}행) ===")
            df = analyzer.generate_sample_data(num_rows)

            # 분석 수행
            category_stats, execution_time = analyzer.analyze_data(
                df, f"{config.app_name} ({num_rows:,}행)"
            )

            # 파티션 분석
            df_repartitioned, df_coalesced = analyzer.analyze_partitions(
                df, f"{config.app_name} 파티션"
            )

            # 실행 시간 로깅
            logging.info(f"{config.app_name} 완료: 실행 시간 = {execution_time:.2f}초")
            return execution_time

    except (SparkSessionError, DataProcessingError) as e:
        logging.error(f"분석 실행 중 오류 발생: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {str(e)}")
        raise


def main() -> None:
    """메인 함수"""
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        local_time = None
        cluster_time = None

        # 로컬 모드 테스트
        logging.info("\n=== 로컬 모드 테스트 시작 ===")
        local_config = SparkConfig(app_name="LocalTest")
        local_time = run_analysis(local_config, num_rows=1000000)
        logging.info(f"로컬 모드 완료: 처리 시간 = {local_time:.2f}초")

        # 클러스터 모드 테스트
        logging.info("\n=== 클러스터 모드 테스트 시작 ===")
        cluster_config = SparkConfig(
            app_name="ClusterTest", master_url="spark://spark-master:7077"
        )
        cluster_time = run_analysis(cluster_config, num_rows=100000000)
        logging.info(f"클러스터 모드 완료: 처리 시간 = {cluster_time:.2f}초")

        # 성능 비교
        if local_time is not None and cluster_time is not None:
            speedup = local_time / cluster_time if cluster_time > 0 else 0
            logging.info("\n=== 성능 비교 ===")
            logging.info(f"1. 로컬 처리 시간: {local_time:.2f}초")
            logging.info(f"2. 클러스터 처리 시간: {cluster_time:.2f}초")
            logging.info(f"3. 속도 향상: {speedup:.2f}배")
        else:
            logging.warning("일부 테스트가 실패하여 성능 비교를 수행할 수 없습니다.")

    except Exception as e:
        logging.error(f"프로그램 실행 중 오류 발생: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
