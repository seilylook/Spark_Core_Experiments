FROM python:3.12-slim

# 필요한 패키지 설치 (Default JDK와 procps, MongoDB Tools)
RUN apt-get update && \
    apt-get install -y \
    default-jdk \
    procps \
    # mongodb-clients \
    && rm -rf /var/lib/apt/lists/*

# 작업 디렉토리 설정
WORKDIR /app

# Java 환경변수 설정
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$PATH:$JAVA_HOME/bin

# Python 패스 설정
ENV PYTHONPATH=/app

# MongoDB 초기화 스크립트 복사
COPY scripts/init_mongo.sh /scripts/init_mongo.sh

# 종속성 파일만 먼저 복사하여 캐시 활용
COPY requirements.txt .

# 종속성 설치
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 및 테스트 파일 복사
COPY src/ src/
COPY tests/ tests/
COPY data/ data/
COPY conf/ conf/

# 초기화 및 Python 실행 명령어 설정
CMD python -m src.main