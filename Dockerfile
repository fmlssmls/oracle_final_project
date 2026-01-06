# Railway 배포용 Dockerfile
FROM python:3.9-slim

# 작업 디렉토리 설정
WORKDIR /app

# Oracle Instant Client 설치를 위한 패키지
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libaio1t64 \
    && rm -rf /var/lib/apt/lists/*

# Oracle Instant Client 19.23 Basic Lite 다운로드 및 설치
RUN mkdir -p /opt/oracle && \
    cd /opt/oracle && \
    wget -q https://download.oracle.com/otn_software/linux/instantclient/1923000/instantclient-basiclite-linux.x64-19.23.0.0.0dbru.zip && \
    unzip -q instantclient-basiclite-linux.x64-19.23.0.0.0dbru.zip && \
    rm instantclient-basiclite-linux.x64-19.23.0.0.0dbru.zip

# Python 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 파일 복사
COPY . .

# Wallet 디렉토리 생성
RUN mkdir -p /app/wallet && chmod 755 /app/wallet

ENV PORT=5000
CMD ["python", "app.py"]
