#!/usr/bin/env bash
set -o errexit

wget https://download.oracle.com/otn_software/linux/instantclient/2340000/instantclient-basic-linux.x64-23.4.0.24.05.zip
unzip instantclient-basic-linux.x64-23.4.0.24.05.zip -d /opt/
rm instantclient-basic-linux.x64-23.4.0.24.05.zip

pip install -r requirements.txt
