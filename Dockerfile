FROM ubuntu:22.04

# 비대화형 설치 설정
ENV DEBIAN_FRONTEND=noninteractive

# Python 3.9 및 필수 패키지 설치
RUN apt-get update && apt-get install -y \
    python3.9 \
    python3-pip \
    wget \
    unzip \
    libaio1 \
    && rm -rf /var/lib/apt/lists/*

# Oracle Instant Client 설치
RUN wget https://download.oracle.com/otn_software/linux/instantclient/2340000/instantclient-basic-linux.x64-23.4.0.24.05.zip \
    && unzip instantclient-basic-linux.x64-23.4.0.24.05.zip -d /opt/oracle \
    && rm instantclient-basic-linux.x64-23.4.0.24.05.zip

ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_23_4:$LD_LIBRARY_PATH

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:10000"]
