import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import cx_Oracle
from langchain_openai import ChatOpenAI
import re
import bcrypt

from column_manager import ColumnManager, column_manager

print("🔥 모듈 import 완료")

app = Flask(__name__)
CORS(app, origins="*", supports_credentials=True)

# 환경변수 검증 추가
if not OPENAI_API_KEY:
    raise ValueError("❌ OPENAI_API_KEY 환경변수가 설정되지 않았습니다!")

# LLM만 초기화 (ChromaDB 제거)
llm = ChatOpenAI(
    model="gpt-4o",
    temperature=0,
    openai_api_key=os.getenv("OPENAI_API_KEY")
)


def get_db_connection():
    """Oracle Cloud Autonomous Database 연결"""
    try:
        wallet_location = os.getenv("WALLET_LOCATION", "/app/wallet")
        wallet_password = os.getenv("WALLET_PASSWORD", "")
        os.environ["TNS_ADMIN"] = wallet_location

        service_name = os.getenv("ORACLE_SERVICE_NAME", "oraclefinalproject_high")
        username = os.getenv("ORACLE_USER", "ADMIN")
        password = os.getenv("ORACLE_PW")

        conn = cx_Oracle.connect(
            user=username,
            password=password,
            dsn=service_name,
            encoding="UTF-8"
        )

        print(f"✅ Oracle Cloud DB 연결 성공: {service_name}")
        return conn

    except cx_Oracle.Error as e:
        error_obj, = e.args
        print(f"❌ 데이터베이스 연결 오류: {error_obj.message}")
        return None
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {str(e)}")
        return None


def detect_user_intent(user_msg):
    """사용자 의도 감지"""
    msg_lower = user_msg.lower().strip()
    
    swear_keywords = ['씨발', '병신', '개새끼', '지랄', '좆']
    greeting_keywords = ['안녕', '하이', '헬로', 'hi', 'hello']
    positive_keywords = ['고마워', '감사', '잘했어', '최고', '완벽']
    
    return {
        'swear': any(k in msg_lower for k in swear_keywords),
        'greeting': any(k in msg_lower for k in greeting_keywords),
        'positive': any(k in msg_lower for k in positive_keywords)
    }


def extract_sql_and_guide(llm_answer):
    """LLM 응답에서 SQL 추출"""
    answer = llm_answer.replace("```sql", "").replace("```", "").strip()
    sql_match = re.search(r"(SELECT[\s\S]+?)(?:$|\n\n|\Z)", answer, re.IGNORECASE)
    sql = sql_match.group(1).strip() if sql_match else ""
    guide = answer
    return guide, sql


def execute_sql(sql):
    """SQL 실행"""
    conn = get_db_connection()
    if not conn:
        return {"success": False, "error": "데이터베이스 연결 실패"}
    
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        col_names = [i[0] for i in cursor.description] if cursor.description else []
        result = [dict(zip(col_names, row)) for row in rows]
        return {"success": True, "result": result, "columns": col_names}
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        cursor.close()
        conn.close()


# CORS preflight 명시적 처리
@app.route('/login', methods=['OPTIONS'])
def login_options():
    response = jsonify({'status': 'ok'})
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'POST,OPTIONS')
    return response

@app.route('/login', methods=['POST'])
def login():
    data = request.json
    user_id = data.get('user_id')
    user_pw = data.get('user_pw')
    
    if not user_id or not user_pw:
        return jsonify({"success": False, "message": "ID와 PW를 모두 입력하세요."}), 400
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"success": False, "message": "데이터베이스 연결 오류."}), 500
        
        cursor = conn.cursor()
        sql = "SELECT USER_SEQ, USER_PW FROM USERS WHERE USER_ID = :1"
        cursor.execute(sql, (user_id,))
        row = cursor.fetchone()
        
        if row:
            user_seq, stored_hashed_pw = row
            if isinstance(stored_hashed_pw, str):
                stored_hashed_pw = stored_hashed_pw.encode('utf-8')
            
            if bcrypt.checkpw(user_pw.encode('utf-8'), stored_hashed_pw):
                return jsonify({"success": True, "user_seq": user_seq})
            else:
                return jsonify({"success": False, "message": "로그인 정보가 올바르지 않습니다."})
        else:
            return jsonify({"success": False, "message": "로그인 정보가 올바르지 않습니다."})
            
    except cx_Oracle.Error as e:
        error_obj, = e.args
        error_msg = f"❌ 데이터베이스 연결 오류: {error_obj.message}"
        print(error_msg)
        app.logger.error(error_msg)  # 추가
        return None
    except Exception as e:
        error_msg = f"❌ 예상치 못한 오류: {str(e)}"
        print(error_msg)
        app.logger.error(error_msg)  # 추가
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.route('/signup', methods=['POST'])
def signup():
    data = request.json
    user_id = data.get('user_id')
    user_pw = data.get('user_pw')
    
    if not user_id or not user_pw:
        return jsonify({"success": False, "message": "ID와 PW를 모두 입력하세요."}), 400
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"success": False, "message": "데이터베이스 연결 오류."}), 500
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM USERS WHERE USER_ID = :1", (user_id,))
        
        if cursor.fetchone()[0] > 0:
            return jsonify({"success": False, "message": "이미 존재하는 ID입니다."})
        
        hashed_pw = bcrypt.hashpw(user_pw.encode('utf-8'), bcrypt.gensalt())
        sql = "INSERT INTO USERS (USER_ID, USER_PW) VALUES (:1, :2)"
        cursor.execute(sql, (user_id, hashed_pw.decode('utf-8')))
        conn.commit()
        
        return jsonify({"success": True})
        
    except cx_Oracle.Error as e:
        error_obj, = e.args
        print(f"Oracle 오류 (회원가입): {error_obj.message}")
        if conn:
            conn.rollback()
        return jsonify({"success": False, "message": "서버 오류: 데이터베이스 문제."}), 500
    except Exception as e:
        print(f"예상치 못한 오류 (회원가입): {e}")
        if conn:
            conn.rollback()
        return jsonify({"success": False, "message": "서버 오류: 알 수 없는 문제."}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.route('/change_pw', methods=['POST'])
def change_pw():
    data = request.json
    user_id = data.get('user_id')
    old_pw = data.get('old_pw')
    new_pw = data.get('new_pw')
    
    if not user_id or not old_pw or not new_pw:
        return jsonify({'success': False, 'message': '입력값이 부족합니다.'}), 400

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({'success': False, 'message': '데이터베이스 연결 오류.'}), 500
        
        cursor = conn.cursor()
        cursor.execute("SELECT USER_PW FROM USERS WHERE USER_ID = :1", (user_id,))
        row = cursor.fetchone()
        
        if not row:
            return jsonify({'success': False, 'message': '사용자 정보가 없습니다.'}), 400

        stored_hashed_pw = row[0]
        if isinstance(stored_hashed_pw, str):
            stored_hashed_pw = stored_hashed_pw.encode('utf-8')
        
        if not bcrypt.checkpw(old_pw.encode('utf-8'), stored_hashed_pw):
            return jsonify({'success': False, 'message': '기존 비밀번호가 일치하지 않습니다.'})

        new_hashed_pw = bcrypt.hashpw(new_pw.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        cursor.execute("UPDATE USERS SET USER_PW = :1 WHERE USER_ID = :2", (new_hashed_pw, user_id))
        conn.commit()
        
        return jsonify({'success': True, 'message': '비밀번호가 성공적으로 변경되었습니다.'})
        
    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({'success': False, 'message': '서버 오류: ' + str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.route('/chat', methods=['POST'])
def chat():
    data = request.json
    user_msg = data['message']
    chat_history = data.get('chat_history', [])[-5:]

    # 의도 감지
    intent_check = detect_user_intent(user_msg)
    
    if intent_check['swear']:
        return jsonify({
            "sql": "",
            "db_result": [],
            "all_result": [],
            "db_error": None,
            "report_text": "부적절한 표현은 자제 부탁드립니다.",
            "columns": []
        })
    elif intent_check['greeting']:
        return jsonify({
            "sql": "",
            "db_result": [],
            "all_result": [],
            "db_error": None,
            "report_text": "안녕하세요! 무엇을 도와드릴까요?",
            "columns": []
        })
    elif intent_check['positive']:
        return jsonify({
            "sql": "",
            "db_result": [],
            "all_result": [],
            "db_error": None,
            "report_text": "감사합니다. 더 궁금하신 게 있으신가요?",
            "columns": []
        })

    # 선택된 컬럼 가져오기
    selected_cols = column_manager.get_selected_columns()
    col_hint = f"\n사용 가능 컬럼: {', '.join(selected_cols)}" if selected_cols else ""

    # 프롬프트 생성 (RAG 제거, 직접 프롬프트만)
    prompt_template = f"""당신은 MIMIC-IV 의료 데이터베이스 전문가입니다.

{col_hint}

대화 기록:
{chat_history}

사용자 질문: {user_msg}

Oracle SQL 쿼리를 생성하세요. SELECT 문만 작성하고, 컬럼명은 대문자로 작성하세요."""

    # LLM 호출
    llm_answer = llm.invoke(prompt_template).content
    guide, sql = extract_sql_and_guide(llm_answer)

    # SQL 실행
    if sql:
        db_result = execute_sql(sql)
        
        if db_result['success']:
            all_rows = db_result['result']
            preview_rows = all_rows[:200]
            columns = db_result['columns']
            error = None
            
            # 리포트 생성
            if len(all_rows) > 0:
                report_prompt = f"""결과를 한 줄로 요약하세요.
                
결과 개수: {len(all_rows)}개
컬럼: {', '.join(columns)}"""
                
                report_resp = llm.invoke(report_prompt)
                summary_text = getattr(report_resp, "content", str(report_resp)).strip()
                report_text = f"{summary_text}\n(자세한 정보와 표는 '결과창'에서 확인하세요.)"
            else:
                report_text = "결과가 없습니다."
        else:
            error = db_result.get('error')
            all_rows = []
            preview_rows = []
            columns = []
            report_text = f"SQL 실행 오류: {error}"
    else:
        sql = ""
        error = "SQL 생성 실패"
        all_rows = []
        preview_rows = []
        columns = []
        report_text = "SQL을 생성하지 못했습니다."

    return jsonify({
        "sql": sql,
        "db_result": preview_rows,
        "all_result": all_rows,
        "db_error": error,
        "report_text": report_text,
        "columns": columns
    })


@app.route('/download_csv', methods=['POST'])
def download_csv():
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


@app.route('/set_columns', methods=['POST'])
def set_columns():
    """컬럼 선택 설정"""
    data = request.json
    selected = data.get('selected_columns', [])
    column_manager.set_selected_columns(selected)
    return jsonify({"success": True})


@app.route('/get_columns', methods=['GET'])
def get_columns():
    """선택된 컬럼 조회"""
    selected = column_manager.get_selected_columns()
    return jsonify({"selected_columns": selected})


@app.route('/')
def index():
    return jsonify({
        "status": "online",
        "message": "GPTify API Server is running!",
        "endpoints": ["/chat", "/login", "/signup"]
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)



