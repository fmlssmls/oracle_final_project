#!/usr/bin/env bash
set -o errexit

echo "🔧 Oracle Instant Client 설치 시작..."

# Oracle Client 다운로드
wget https://download.oracle.com/otn_software/linux/instantclient/2340000/instantclient-basic-linux.x64-23.4.0.24.05.zip

# 압축 풀기
unzip instantclient-basic-linux.x64-23.4.0.24.05.zip -d /opt/

# 압축 파일 삭제
rm instantclient-basic-linux.x64-23.4.0.24.05.zip

echo "✅ Oracle Instant Client 설치 완료!"

# Python 패키지 설치
echo "📦 Python 패키지 설치 시작..."
pip install -r requirements.txt

echo "🎉 모든 설치 완료!"
```

**이게 뭐하는 거야?**
→ Render가 서버 만들 때 이 명령어들을 순서대로 실행함
→ Oracle 연결 프로그램을 다운받아서 설치하는 거

**파일 저장 후 해야 할 일:**
- Git Bash나 터미널에서 실행: `chmod +x build.sh`
  (이건 이 파일을 "실행 가능하게" 만드는 명령어)

---

### 📁 파일 3: `requirements.txt` 확인

**위치**: 프로젝트 최상위 폴더

**필수 포함 내용**:
```
Flask
flask-cors
cx-Oracle
gunicorn
langchain-openai
langchain-chroma
langchain-huggingface
bcrypt
```

(이미 있으면 그대로 두면 됨)

---

## Render 웹사이트에서 설정하기

### 1️⃣ Render Dashboard 접속
- https://dashboard.render.com/
- 본인 서비스(oracle-final-project-1) 클릭

### 2️⃣ Settings 탭 클릭

### 3️⃣ Build & Deploy 섹션 찾기

**Build Command** 칸에 입력:
```
./build.sh
```

**Start Command** 칸 확인 (이미 있을 거임):
```
gunicorn app:app