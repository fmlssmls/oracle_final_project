# Railway 배포용 Dockerfile
FROM python:3.9-slim

# 작업 디렉토리 설정
WORKDIR /app

# Oracle Instant Client 설치를 위한 패키지
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libaio1 \
    && rm -rf /var/lib/apt/lists/*

# Oracle Instant Client 19.23 Basic Lite 다운로드 및 설치
RUN wget https://download.oracle.com/otn_software/linux/instantclient/1923000/instantclient-basiclite-linux.x64-19.23.0.0.0dbru.zip \
    && unzip instantclient-basiclite-linux.x64-19.23.0.0.0dbru.zip -d /opt/oracle \
    && rm instantclient-basiclite-linux.x64-19.23.0.0.0dbru.zip \
    && echo /opt/oracle/instantclient_19_23 > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

# 환경변수 설정
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_19_23:$LD_LIBRARY_PATH
ENV PATH=/opt/oracle/instantclient_19_23:$PATH

# Python 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 파일 복사
COPY . .

ENV PORT=5000
CMD ["python", "app.py"]
