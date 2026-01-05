FROM python:3.9-slim

WORKDIR /app

# 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libaio1t64 \
    && rm -rf /var/lib/apt/lists/*

# Oracle Instant Client Basic Lite (더 가벼움, 60MB)
RUN mkdir -p /opt/oracle && \
    cd /opt/oracle && \
    wget -q https://download.oracle.com/otn_software/linux/instantclient/1923000/instantclient-basiclite-linux.x64-19.23.0.0.0dbru.zip && \
    unzip -q instantclient-basiclite-linux.x64-19.23.0.0.0dbru.zip && \
    rm instantclient-basiclite-linux.x64-19.23.0.0.0dbru.zip

# 환경변수
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_19_23:$LD_LIBRARY_PATH
ENV PATH=/opt/oracle/instantclient_19_23:$PATH

# Python 패키지
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Wallet 디렉토리
RUN mkdir -p /app/wallet && chmod 755 /app/wallet
ENV TNS_ADMIN=/app/wallet

EXPOSE 10000

CMD ["gunicorn", "--bind", "0.0.0.0:10000", "--timeout", "120", "--workers", "1", "app:app"]
