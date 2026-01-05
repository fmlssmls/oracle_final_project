# Railway 최적화 Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Oracle Instant Client 의존성 설치
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libaio1 \
    && rm -rf /var/lib/apt/lists/*

# Oracle Instant Client 23.5 설치
RUN mkdir -p /opt/oracle && \
    cd /opt/oracle && \
    wget https://download.oracle.com/otn_software/linux/instantclient/2350000/instantclient-basic-linux.x64-23.5.0.24.07.zip && \
    unzip instantclient-basic-linux.x64-23.5.0.24.07.zip && \
    rm instantclient-basic-linux.x64-23.5.0.24.07.zip && \
    cd instantclient_23_5 && \
    ln -s libclntsh.so.23.1 libclntsh.so && \
    ln -s libocci.so.23.1 libocci.so

# 환경변수 설정
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_23_5:$LD_LIBRARY_PATH
ENV PATH=/opt/oracle/instantclient_23_5:$PATH

# Python 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 복사
COPY . .

# Wallet 디렉토리 생성
RUN mkdir -p /app/wallet && chmod 755 /app/wallet

# TNS_ADMIN 환경변수
ENV TNS_ADMIN=/app/wallet

EXPOSE 10000

CMD ["gunicorn", "--bind", "0.0.0.0:10000", "--timeout", "120", "--workers", "1", "app:app"]
