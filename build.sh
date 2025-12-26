#!/usr/bin/env bash
# exit on error
set -o errexit

# Oracle Instant Client 설치
wget https://download.oracle.com/otn_software/linux/instantclient/2340000/instantclient-basic-linux.x64-23.4.0.24.05.zip
unzip instantclient-basic-linux.x64-23.4.0.24.05.zip -d /opt/
rm instantclient-basic-linux.x64-23.4.0.24.05.zip

# Python 패키지 설치
pip install -r requirements.txt
