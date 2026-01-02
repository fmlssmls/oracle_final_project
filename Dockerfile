FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    libaio1t64 \
    && rm -rf /var/lib/apt/lists/*

# ✅ 심볼릭 링크 생성 (Debian Trixie 호환성)
RUN if [ -f /usr/lib/x86_64-linux-gnu/libaio.so.1t64 ]; then \
        ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1; \
    fi

RUN wget https://download.oracle.com/otn_software/linux/instantclient/2340000/instantclient-basic-linux.x64-23.4.0.24.05.zip \
    && unzip instantclient-basic-linux.x64-23.4.0.24.05.zip \
    && rm instantclient-basic-linux.x64-23.4.0.24.05.zip

ENV LD_LIBRARY_PATH=/instantclient_23_4:$LD_LIBRARY_PATH

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD gunicorn app:app --bind 0.0.0.0:$PORT
