FROM oraclelinux:8-slim

RUN microdnf install -y \
    oracle-instantclient-release-el8 \
    && microdnf install -y \
    oracle-instantclient-basic \
    python39 \
    python39-pip \
    && microdnf clean all

RUN pip3.9 install --upgrade pip

WORKDIR /app

COPY requirements.txt .
RUN pip3.9 install --no-cache-dir -r requirements.txt

COPY . .

CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:10000", "--timeout", "120"]
