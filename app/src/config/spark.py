class SparkConfig:
    def __init__(
        self,
        app_name: str,
        master_url: str = "local[*]",
        log_level: str = "INFO",
        shuffle_partitions: int = 200,
    ):
        self.app_name = app_name
        self.master_url = master_url
        self._configs = {
            # 기본 설정
            "spark.app.name": app_name,
            "spark.master": master_url,
            "spark.sql.shuffle.partitions": str(shuffle_partitions),
            # 메모리 설정
            "spark.driver.memory": "4g",
            "spark.executor.memory": "4g",
            "spark.driver.maxResultSize": "2g",
            # 코어 설정
            "spark.executor.cores": "3",
            # 네트워크 설정
            "spark.network.timeout": "800s",
            "spark.executor.heartbeatInterval": "60s",
            # 성능 설정
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            # 로깅 설정
            "spark.driver.extraJavaOptions": f"-Dlog4j.rootCategory={log_level} -XX:+UseG1GC",
            "spark.executor.extraJavaOptions": f"-Dlog4j.rootCategory={log_level} -XX:+UseG1GC",
        }

        # 클러스터 모드일 경우 추가 설정
        if "spark://" in master_url:
            self._configs.update(
                {
                    # 배포 모드
                    "spark.submit.deployMode": "client",
                    # 드라이버 설정
                    "spark.driver.bindAddress": "0.0.0.0",
                    "spark.driver.host": "python-app",
                    # 메모리 관리
                    "spark.memory.offHeap.enabled": "true",
                    "spark.memory.offHeap.size": "2g",
                    # 실행기 설정
                    "spark.executor.instances": "3",
                    "spark.executor.memory": "4g",
                    "spark.executor.memoryOverhead": "1g",
                    "spark.dynamicAllocation.enabled": "false",
                    # 병렬처리 설정
                    "spark.default.parallelism": "9",  # executor.cores(3) * num executors(3)
                    "spark.sql.shuffle.partitions": "18",  # parallelism * 2
                    # 성능 최적화
                    "spark.sql.adaptive.skewJoin.enabled": "true",
                    "spark.sql.adaptive.localShuffleReader.enabled": "true",
                    # 메모리 설정
                    "spark.memory.fraction": "0.8",
                    "spark.memory.storageFraction": "0.3",
                    # 셔플 설정
                    "spark.shuffle.compress": "true",
                    "spark.shuffle.file.buffer": "128k",
                    "spark.reducer.maxSizeInFlight": "96m",
                    "spark.shuffle.io.maxRetries": "10",
                    "spark.shuffle.io.retryWait": "30s",
                    # 캐시 설정
                    "spark.broadcast.compress": "true",
                    "spark.rdd.compress": "true",
                }
            )

    def get_configs(self) -> dict:
        return self._configs

    def add_config(self, key: str, value: str) -> None:
        self._configs[key] = value
