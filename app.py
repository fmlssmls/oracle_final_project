"""
Railway 배포용 포트폴리오 완전판 app.py
포함 기능:
1. GPT → SQL 변환 및 실행 (핵심)
2. 컬럼 사전 선택 (내가 담당)
3. eCRF 데이터 입력/검증 (내가 담당)
4. 로그인/회원가입
제외 기능:
- evaluation_module (메모리 과다 사용, 시각화 어려움)
"""

import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import cx_Oracle
from langchain_openai import ChatOpenAI
from langchain_chroma import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
from langchain.memory import ConversationBufferMemory
from langchain.prompts import PromptTemplate
import re
import bcrypt
import json

app = Flask(__name__)

# CORS 설정
CORS(app, resources={
    r"/*": {
        "origins": ["https://gregarious-dasik-eb3f31.netlify.app", "*"],
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"],
        "supports_credentials": True
    }
})

# 환경변수 (Railway에서 설정)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PW = os.getenv("ORACLE_PW")
ORACLE_HOST = os.getenv("ORACLE_HOST")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE = os.getenv("ORACLE_SERVICE")

print(f"✅ 환경변수 로드 완료")
print(f"   Oracle: {ORACLE_USER}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")

# guide_map.json 로드 (파일이 있는 경우)
try:
    with open('guide_map.json', encoding='utf-8') as f:
        GUIDE_MAP = json.load(f)
    print("✅ guide_map.json 로드 완료")
except FileNotFoundError:
    print("⚠️  guide_map.json 없음 - 기본 가이드 사용")
    GUIDE_MAP = {
        "기본": {
            "schema": [],
            "guide": "MIMIC-IV 데이터베이스에서 Oracle SQL을 생성하세요."
        }
    }

def infer_intent(question):
    """질문에서 의도 추론"""
    q = question.lower()
    if any(w in q for w in ['임상시험', 'inclusion', 'exclusion', '제외', 'ae', 'adr', 'susar']):
        if 'ae' in q or 'adr' in q or 'susar' in q:
            return '임상시험/AE'
        if '제외' in q or 'exclusion' in q:
            return '임상시험/제외'
        return '임상시험'
    if any(w in q for w in ['혈압', '맥박', '체온', '혈당', 'wbc', 'hb', 'glucose', 'chart', 'lab', '검사']):
        return '바이탈/검사'
    if any(w in q for w in ['진단', 'icd', '코드', '시술', 'procedure', '수술']):
        return '진단/시술'
    if any(w in q for w in ['약', '투약', 'drug', '처방', 'medication', '항생제']):
        return '약물/투약'
    if any(w in q for w in ['수액', '투여', 'infusion', 'fluid']):
        return '수액/투여'
    if any(w in q for w in ['미생물', '감염', '균', 'infection']):
        return '미생물/감염'
    if any(w in q for w in ['icu', '중환자', '재원', 'los']):
        return 'ICU/재원'
    if any(w in q for w in ['입원', 'admit', '퇴원', 'discharge']):
        return '환자/입원'
    return '기본'

def load_schema_and_guide(intent):
    """의도에 맞는 스키마 및 가이드 로드"""
    guide_item = GUIDE_MAP.get(intent, GUIDE_MAP.get('기본', {}))
    schema_files = guide_item.get("schema", [])
    guide = guide_item.get("guide", "")
    context = ""
    
    for fname in schema_files:
        try:
            with open(fname, encoding='utf-8') as f:
                context += f"\n[{fname}]\n" + f.read() + "\n"
        except FileNotFoundError:
            print(f"⚠️  스키마 파일 없음: {fname}")
            continue
    
    return context, guide

# LLM 초기화
try:
    llm = ChatOpenAI(
        model="gpt-4o",
        temperature=0,
        api_key=OPENAI_API_KEY
    )
    print("✅ GPT-4o LLM 초기화 완료")
except Exception as e:
    print(f"❌ LLM 초기화 실패: {e}")
    llm = None

# ChromaDB 초기화 (파일이 있는 경우)
try:
    embedding = HuggingFaceEmbeddings(model_name="intfloat/multilingual-e5-large")
    vectordb = Chroma(
        persist_directory="./chroma_db",
        embedding_function=embedding
    )
    print("✅ ChromaDB 초기화 완료")
except Exception as e:
    print(f"⚠️  ChromaDB 초기화 실패 (RAG 기능 제한됨): {e}")
    vectordb = None

# 프롬프트 템플릿
prompt = PromptTemplate(
    input_variables=["context", "guide", "chat_history", "question"],
    template="""
[데이터 context]
{context}

[분석/SQL 변환 가이드라인]
{guide}
반드시 ORACLE SQL 쿼리문을 반환할 것

[대화 내용]
{chat_history}

[사용자 질문]
{question}
"""
)

def extract_faq_from_context(context):
    """컨텍스트에서 FAQ 추출"""
    faq_list = []
    faq_pairs = re.findall(r"Q[:：](.*?)\nA[:：](.*?)(?=\nQ[:：]|\Z)", context, re.DOTALL)
    for q, a in faq_pairs:
        faq_list.append("Q:" + q.strip() + "\nA:" + a.strip())
    return faq_list

memory = ConversationBufferMemory(memory_key="chat_history", k=5, return_messages=True)

def detect_user_intent(msg):
    """사용자 의도 감지 (인사, 긍정, 부정 등)"""
    greetings = ['안녕', 'hi', 'hello', '헬로', '하이', '반가워']
    positive = ['고마워', '감사', '땡큐', '최고', '굿', '짱']
    negative = ['싫어', '짜증', '피곤', '힘들', '귀찮']
    swear = ['씨발', 'fuck', 'shit']
    
    msg_lower = msg.lower()
    return {
        'greeting': any(word in msg_lower for word in greetings),
        'positive': any(word in msg_lower for word in positive),
        'negative': any(word in msg_lower for word in negative),
        'swear': any(word in msg_lower for word in swear)
    }

def get_db_connection():
    """Oracle DB 연결"""
    try:
        conn = cx_Oracle.connect(
            f"{ORACLE_USER}/{ORACLE_PW}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
        )
        return conn
    except cx_Oracle.Error as e:
        error_obj, = e.args
        print(f"❌ 데이터베이스 연결 오류: {error_obj.message}")
        return None

def run_sql_query(sql):
    """SQL 쿼리 실행"""
    sql = sql.strip()
    
    # 세미콜론 제거
    while sql.endswith(';'):
        sql = sql[:-1].strip()
    
    if not sql.lower().startswith("select"):
        return {"success": False, "error": "SELECT 쿼리만 실행 가능합니다."}
    
    try:
        conn = get_db_connection()
        if not conn:
            return {"success": False, "error": "데이터베이스 연결 실패"}
        
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        col_names = [i[0] for i in cursor.description] if cursor.description else []
        
        # datetime, Decimal 처리
        result = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(col_names):
                value = row[i]
                # datetime 처리
                if hasattr(value, 'isoformat'):
                    value = value.isoformat()
                # Decimal 처리
                elif hasattr(value, '__float__'):
                    value = float(value)
                row_dict[col] = value
            result.append(row_dict)
        
        cursor.close()
        conn.close()
        
        return {"success": True, "result": result, "columns": col_names}
        
    except Exception as e:
        return {"success": False, "error": str(e)}

def extract_sql_and_guide(llm_answer):
    """LLM 응답에서 SQL 추출"""
    answer = llm_answer.replace("```sql", "").replace("```", "").strip()
    sql_match = re.search(r"(SELECT[\s\S]+?)(?:$|\n\n|\Z)", answer, re.IGNORECASE)
    sql = sql_match.group(1).strip() if sql_match else ""
    guide = answer
    return guide, sql

# ==================== API 엔드포인트 ====================

@app.route('/health', methods=['GET'])
def health():
    """헬스체크"""
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.close()
            conn.close()
            return jsonify({"status": "healthy", "database": "connected"})
        else:
            return jsonify({"status": "unhealthy", "database": "disconnected"}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/', methods=['GET'])
def index():
    """루트 엔드포인트"""
    return jsonify({
        "service": "GPTify Portfolio API",
        "version": "2.0",
        "features": ["GPT→SQL", "Column Selection", "eCRF"],
        "endpoints": {
            "health": "/health",
            "chat": "/chat (POST)",
            "login": "/login (POST)",
            "signup": "/signup (POST)",
            "ecrf": "/submit_procedure (POST)",
            "columns": "/get_columns (GET)"
        }
    })

@app.route('/chat', methods=['POST'])
def chat():
    """메인 채팅 엔드포인트 - GPT → SQL 변환"""
    data = request.json
    user_msg = data.get('message', '')
    chat_history = data.get('chat_history', [])[-5:]
    
    if not user_msg:
        return jsonify({"error": "메시지가 비어있습니다"}), 400
    
    # 의도 감지
    intent_check = detect_user_intent(user_msg)
    
    if intent_check['swear']:
        return jsonify({
            "sql": "",
            "db_result": [],
            "db_error": None,
            "report_text": "부적절한 표현은 자제 부탁드립니다.",
            "columns": []
        })
    elif intent_check['greeting']:
        return jsonify({
            "sql": "",
            "db_result": [],
            "db_error": None,
            "report_text": "안녕하세요! 무엇을 도와드릴까요?",
            "columns": []
        })
    elif intent_check['positive']:
        return jsonify({
            "sql": "",
            "db_result": [],
            "db_error": None,
            "report_text": "감사합니다. 더 궁금하신 게 있으신가요?",
            "columns": []
        })
    
    # 핵심 처리: GPT → SQL
    intent = infer_intent(user_msg)
    original_context, guide = load_schema_and_guide(intent)
    
    # VectorDB 검색 (있는 경우)
    context = original_context
    if vectordb:
        try:
            docs = vectordb.similarity_search(user_msg, k=3)
            retrieved_context = "\n\n".join([d.page_content for d in docs if hasattr(d, 'page_content')])
            context = retrieved_context if retrieved_context else original_context
        except Exception as e:
            print(f"⚠️  VectorDB 검색 실패: {e}")
    
    # LLM 호출
    if not llm:
        return jsonify({
            "error": "LLM이 초기화되지 않았습니다"
        }), 500
    
    try:
        llm_input = prompt.format(
            context=context,
            guide=guide,
            chat_history="\n".join([f"User: {h.get('user', '')}\nAssistant: {h.get('assistant', '')}" 
                                   for h in chat_history]),
            question=user_msg
        )
        
        llm_response = llm.invoke(llm_input)
        llm_answer = llm_response.content if hasattr(llm_response, 'content') else str(llm_response)
        
        report_text, sql = extract_sql_and_guide(llm_answer)
        
    except Exception as e:
        return jsonify({
            "error": f"LLM 호출 실패: {str(e)}"
        }), 500
    
    # SQL 실행
    db_result = run_sql_query(sql)
    
    preview_rows = []
    columns = []
    
    if db_result.get("success"):
        all_rows = db_result.get("result", [])
        columns = db_result.get("columns", [])
        preview_rows = all_rows[:10]  # 미리보기 10개
        
        if not report_text or "SELECT" in report_text:
            if preview_rows:
                rows_for_summary = preview_rows[:3]
                sample_rows = "\n".join([
                    ", ".join([f"{col}: {row.get(col, '')}" for col in columns])
                    for row in rows_for_summary
                ])
                summary_prompt = f"아래 데이터의 의미를 간결하게 설명해주세요.\n컬럼: {', '.join(columns)}\n샘플:\n{sample_rows}"
                
                try:
                    summary_resp = llm.invoke(summary_prompt)
                    report_text = summary_resp.content if hasattr(summary_resp, 'content') else str(summary_resp)
                except:
                    report_text = f"{len(all_rows)}개의 결과를 찾았습니다."
    else:
        report_text = db_result.get("error", "SQL 실행 실패")
    
    return jsonify({
        "sql": sql,
        "db_result": preview_rows,
        "all_result": db_result.get("result", []),
        "db_error": db_result.get("error"),
        "report_text": report_text,
        "columns": columns
    })

@app.route('/download_csv', methods=['POST'])
def download_csv():
    """CSV 다운로드"""
    import csv, io
    data = request.json
    result = data.get('data', [])
    columns = data.get('columns', [])
    
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    for row in result:
        writer.writerow(row)
    output.seek(0)
    
    return output.read(), 200, {
        'Content-Type': 'text/csv; charset=utf-8',
        'Content-Disposition': 'attachment; filename="result.csv"'
    }

@app.route('/login', methods=['POST'])
def login():
    """로그인"""
    data = request.json
    user_id = data.get('user_id')
    user_pw = data.get('user_pw')
    
    if not user_id or not user_pw:
        return jsonify({"success": False, "message": "ID와 PW를 입력하세요"}), 400
    
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"success": False, "message": "DB 연결 실패"}), 500
        
        cursor = conn.cursor()
        cursor.execute("SELECT USER_SEQ, USER_PW FROM USERS WHERE USER_ID = :1", (user_id,))
        row = cursor.fetchone()
        
        if row:
            user_seq, stored_pw = row
            if isinstance(stored_pw, str):
                stored_pw = stored_pw.encode('utf-8')
            
            if bcrypt.checkpw(user_pw.encode('utf-8'), stored_pw):
                cursor.close()
                conn.close()
                return jsonify({"success": True, "user_seq": user_seq})
        
        cursor.close()
        conn.close()
        return jsonify({"success": False, "message": "로그인 정보가 올바르지 않습니다"})
        
    except Exception as e:
        print(f"❌ 로그인 오류: {e}")
        return jsonify({"success": False, "message": "서버 오류"}), 500

@app.route('/signup', methods=['POST'])
def signup():
    """회원가입"""
    data = request.json
    user_id = data.get('user_id')
    user_pw = data.get('user_pw')
    
    if not user_id or not user_pw:
        return jsonify({"success": False, "message": "ID와 PW를 입력하세요"}), 400
    
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"success": False, "message": "DB 연결 실패"}), 500
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM USERS WHERE USER_ID = :1", (user_id,))
        
        if cursor.fetchone()[0] > 0:
            cursor.close()
            conn.close()
            return jsonify({"success": False, "message": "이미 존재하는 ID입니다"})
        
        hashed_pw = bcrypt.hashpw(user_pw.encode('utf-8'), bcrypt.gensalt())
        cursor.execute("INSERT INTO USERS (USER_ID, USER_PW) VALUES (:1, :2)",
                      (user_id, hashed_pw.decode('utf-8')))
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({"success": True})
        
    except Exception as e:
        print(f"❌ 회원가입 오류: {e}")
        return jsonify({"success": False, "message": "서버 오류"}), 500

@app.route('/get_columns', methods=['GET'])
def get_columns():
    """전체 컬럼 목록 조회 (컬럼 선택 기능용)"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"success": False, "error": "DB 연결 실패"}), 500
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name, column_name, data_type
            FROM all_tab_columns
            WHERE owner = 'MIMICIV'
            ORDER BY table_name, column_id
        """)
        
        rows = cursor.fetchall()
        columns = [{"table": r[0], "column": r[1], "type": r[2]} for r in rows]
        
        cursor.close()
        conn.close()
        
        return jsonify({"success": True, "columns": columns})
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/submit_procedure', methods=['POST'])
def submit_procedure():
    """eCRF 시술 데이터 제출"""
    data = request.json
    required_fields = ['patient_id', 'hadm_id', 'procedure_icd', 'procedure_date']
    
    # 필수 필드 검증
    for field in required_fields:
        if not data.get(field):
            return jsonify({"success": False, "message": f"{field}는 필수입니다"}), 400
    
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"success": False, "message": "DB 연결 실패"}), 500
        
        cursor = conn.cursor()
        
        # 중복 검사
        cursor.execute("""
            SELECT COUNT(*) FROM PROCEDURES 
            WHERE PATIENT_ID = :1 AND HADM_ID = :2 
            AND PROCEDURE_ICD = :3 AND PROCEDURE_DATE = :4
        """, (data['patient_id'], data['hadm_id'], data['procedure_icd'], data['procedure_date']))
        
        if cursor.fetchone()[0] > 0:
            cursor.close()
            conn.close()
            return jsonify({"success": False, "message": "이미 등록된 시술입니다"})
        
        # 데이터 삽입
        cursor.execute("""
            INSERT INTO PROCEDURES (PATIENT_ID, HADM_ID, PROCEDURE_ICD, PROCEDURE_DATE)
            VALUES (:1, :2, :3, TO_DATE(:4, 'YYYY-MM-DD'))
        """, (data['patient_id'], data['hadm_id'], data['procedure_icd'], data['procedure_date']))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({"success": True, "message": "시술 데이터가 등록되었습니다"})
        
    except Exception as e:
        print(f"❌ eCRF 제출 오류: {e}")
        return jsonify({"success": False, "message": f"서버 오류: {str(e)}"}), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)

