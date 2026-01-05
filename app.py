"""
Railway 배포용 Flask 애플리케이션
- Oracle Cloud Wallet 자동 설정
- cx_Oracle 사용
- ChromaDB 제거 (메모리 최적화)
- Health Check 엔드포인트
"""

from flask import Flask, request, jsonify, session
from flask_cors import CORS
import cx_Oracle
import os
import json
from datetime import datetime
import openai
from dotenv import load_dotenv
import base64

# 환경변수 로드
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'your-secret-key-here')
CORS(app, resources={
    r"/*": {
        "origins": ["https://gregarious-dasik-eb3f31.netlify.app"],
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"],
        "supports_credentials": True
    }
})

# OpenAI API 키 설정
openai.api_key = os.getenv('OPENAI_API_KEY')

# Oracle Wallet 자동 설정
def setup_wallet_from_env():
    """환경변수에서 Base64 인코딩된 Wallet 파일들을 디코딩하여 생성"""
    wallet_dir = '/app/wallet'
    os.makedirs(wallet_dir, exist_ok=True)
    
    try:
        # cwallet.sso (필수)
        cwallet_b64 = os.getenv('CWALLET_SSO_B64')
        if cwallet_b64:
            with open(f'{wallet_dir}/cwallet.sso', 'wb') as f:
                f.write(base64.b64decode(cwallet_b64))
            print("✅ cwallet.sso 생성 완료")
        
        # tnsnames.ora (필수)
        tnsnames_b64 = os.getenv('TNSNAMES_ORA_B64')
        if tnsnames_b64:
            with open(f'{wallet_dir}/tnsnames.ora', 'w') as f:
                f.write(base64.b64decode(tnsnames_b64).decode('utf-8'))
            print("✅ tnsnames.ora 생성 완료")
        
        # sqlnet.ora (필수)
        sqlnet_b64 = os.getenv('SQLNET_ORA_B64')
        if sqlnet_b64:
            with open(f'{wallet_dir}/sqlnet.ora', 'w') as f:
                f.write(base64.b64decode(sqlnet_b64).decode('utf-8'))
            print("✅ sqlnet.ora 생성 완료")
        
        # 파일 권한 설정
        os.chmod(wallet_dir, 0o755)
        for file in os.listdir(wallet_dir):
            os.chmod(os.path.join(wallet_dir, file), 0o644)
        
        return True
    except Exception as e:
        print(f"❌ Wallet 파일 생성 실패: {e}")
        return False

# 앱 시작 시 Wallet 설정
wallet_setup_done = False

def get_db_connection():
    """Oracle DB 연결 (cx_Oracle + Wallet)"""
    global wallet_setup_done
    
    # Wallet 설정 (최초 1회만)
    if not wallet_setup_done:
        setup_wallet_from_env()
        wallet_setup_done = True
    
    try:
        dsn = cx_Oracle.makedsn(
            os.getenv('ORACLE_HOST'),
            os.getenv('ORACLE_PORT', '1522'),
            service_name=os.getenv('ORACLE_SERVICE_NAME')
        )
        
        connection = cx_Oracle.connect(
            user=os.getenv('ORACLE_USER'),
            password=os.getenv('ORACLE_PASSWORD'),
            dsn=dsn
        )
        
        print("✅ Oracle 연결 성공")
        return connection
    except Exception as e:
        print(f"❌ Oracle 연결 실패: {e}")
        raise

def get_all_columns():
    """DB에서 전체 컬럼 정보 조회 (ChromaDB 대신 직접 조회)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                table_name,
                column_name,
                data_type,
                nullable,
                data_length
            FROM all_tab_columns
            WHERE owner = 'MIMICIV'
            ORDER BY table_name, column_id
        """
        
        cursor.execute(query)
        columns = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # 컬럼 정보를 문자열로 포맷
        column_info = []
        for col in columns:
            table_name, col_name, data_type, nullable, length = col
            info = f"{table_name}.{col_name} ({data_type}"
            if length:
                info += f"({length})"
            info += ", " + ("NULL" if nullable == 'Y' else "NOT NULL") + ")"
            column_info.append(info)
        
        return "\n".join(column_info[:500])  # 최대 500개 컬럼만 반환
        
    except Exception as e:
        print(f"❌ 컬럼 정보 조회 실패: {e}")
        return "컬럼 정보를 불러올 수 없습니다."

def generate_sql_with_gpt(user_question, selected_columns=None):
    """GPT-4를 사용하여 자연어를 SQL로 변환"""
    try:
        # 컬럼 정보 가져오기
        if selected_columns:
            column_context = "\n".join(selected_columns)
        else:
            column_context = get_all_columns()
        
        system_prompt = f"""당신은 MIMIC-IV 의료 데이터베이스의 SQL 전문가입니다.

사용 가능한 컬럼 정보:
{column_context}

규칙:
1. Oracle SQL 문법 사용
2. 테이블명에 MIMICIV. 스키마 접두사 사용
3. ROWNUM으로 결과 제한
4. 안전한 쿼리만 생성 (SELECT만 허용)
5. 한글 질문을 정확한 SQL로 변환

예시:
질문: "심부전 환자 5명 보여줘"
SQL: SELECT * FROM MIMICIV.PATIENTS WHERE ROWNUM <= 5"""

        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_question}
            ],
            temperature=0.3,
            max_tokens=500
        )
        
        sql_query = response.choices[0].message.content.strip()
        
        # SQL 코드 블록 제거
        if "```sql" in sql_query:
            sql_query = sql_query.split("```sql")[1].split("```")[0].strip()
        elif "```" in sql_query:
            sql_query = sql_query.split("```")[1].split("```")[0].strip()
        
        return sql_query
        
    except Exception as e:
        print(f"❌ SQL 생성 실패: {e}")
        raise

def execute_sql_query(sql_query):
    """SQL 쿼리 실행 및 결과 반환"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # SELECT만 허용
        if not sql_query.strip().upper().startswith('SELECT'):
            raise ValueError("SELECT 쿼리만 실행 가능합니다")
        
        cursor.execute(sql_query)
        
        # 컬럼명 가져오기
        columns = [desc[0] for desc in cursor.description]
        
        # 결과 가져오기 (최대 100개)
        rows = cursor.fetchmany(100)
        
        cursor.close()
        conn.close()
        
        # JSON 직렬화 가능한 형태로 변환
        results = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                value = row[i]
                # datetime 객체 처리
                if hasattr(value, 'isoformat'):
                    value = value.isoformat()
                # Decimal 처리
                elif hasattr(value, '__float__'):
                    value = float(value)
                row_dict[col] = value
            results.append(row_dict)
        
        return {
            'columns': columns,
            'rows': results,
            'count': len(results)
        }
        
    except Exception as e:
        print(f"❌ SQL 실행 실패: {e}")
        raise

# ============= API 엔드포인트 =============

@app.route('/health', methods=['GET'])
def health_check():
    """헬스체크 엔드포인트"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        cursor.close()
        conn.close()
        
        return jsonify({
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now().isoformat()
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/chat', methods=['POST'])
def chat():
    """자연어 질의를 SQL로 변환하고 실행"""
    try:
        data = request.get_json()
        user_message = data.get('message', '')
        selected_columns = data.get('selected_columns', None)
        
        if not user_message:
            return jsonify({'error': '메시지를 입력해주세요'}), 400
        
        # 1. SQL 생성
        sql_query = generate_sql_with_gpt(user_message, selected_columns)
        
        # 2. SQL 실행
        result = execute_sql_query(sql_query)
        
        return jsonify({
            'success': True,
            'sql': sql_query,
            'result': result,
            'message': f"{result['count']}개의 결과를 찾았습니다."
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '쿼리 처리 중 오류가 발생했습니다.'
        }), 500

@app.route('/columns', methods=['GET'])
def get_columns():
    """전체 컬럼 목록 조회"""
    try:
        column_info = get_all_columns()
        return jsonify({
            'success': True,
            'columns': column_info.split('\n')
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/tables', methods=['GET'])
def get_tables():
    """전체 테이블 목록 조회"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT table_name, num_rows
            FROM all_tables
            WHERE owner = 'MIMICIV'
            ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        cursor.close()
        conn.close()
        
        table_list = [
            {'name': t[0], 'rows': t[1] if t[1] else 0}
            for t in tables
        ]
        
        return jsonify({
            'success': True,
            'tables': table_list,
            'count': len(table_list)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/', methods=['GET'])
def index():
    """루트 엔드포인트"""
    return jsonify({
        'service': 'GPTify API',
        'status': 'running',
        'version': '2.0 (Railway)',
        'endpoints': {
            'health': '/health',
            'chat': '/chat (POST)',
            'columns': '/columns',
            'tables': '/tables'
        }
    })

if __name__ == '__main__':
    # 로컬 개발 환경
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

