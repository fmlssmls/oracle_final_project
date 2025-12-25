import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import cx_Oracle
# === Oracle 클라이언트 경로 설정 ===
# cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_23_8") # 이건 제가 가끔 안 돌아갈 때가 있어서 추가한 거
from langchain_openai import ChatOpenAI
from langchain_chroma import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
from langchain.memory import ConversationBufferMemory
from langchain.prompts import PromptTemplate
from langchain.callbacks import LangChainTracer
import re
import time
from datetime import datetime
# === 평가지표 관련 import 추가 ===
import tiktoken  # OpenAI 토큰 계산
from evaluation_module import (
    evaluate_and_save, get_query_stats, sql_evaluator, estimate_token_usage,
    record_token_usage, get_token_statistics, token_callback,
    sql_result_cache, run_sql_query_cached
)
from evaluation_module import start_multiturn_session, get_individual_evaluation_result, evaluate_new_rag_metrics, evaluate_langsmith_rag_metrics
from column_manager import ColumnManager, column_manager



# === 평가지표 관련 import 추가 ===
import bcrypt
import json
print("🔥 평가 모듈 import 완료")

app = Flask(__name__)
# 🔥 CORS 설정 강화
CORS(app, resources={
    r"/*": {
        "origins": "*",
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "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")
ORACLE_USER = os.getenv("ORACLE_USER", "SYSTEM")
ORACLE_PW = os.getenv("ORACLE_PW", "oracle_4U")
ORACLE_HOST = os.getenv("ORACLE_HOST", "localhost")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE = os.getenv("ORACLE_SERVICE", "xe")

# app.py에서 수정
os.environ["LANGCHAIN_TRACING_V2"] = "true"  # LANG**CHAIN**
os.environ["LANGCHAIN_API_KEY"] = "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"
os.environ["LANGCHAIN_PROJECT"] = "model5"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"


with open('guide_map.json', encoding='utf-8') as f:
    GUIDE_MAP = json.load(f)


def infer_intent(question):
    q = question.lower()

    # 임상시험 관련
    if any(w in q for w in
           ['임상시험', 'clinical trial', 'inclusion', 'exclusion', '제외', 'ae', 'adr', 'susar', '포함기준', '제외기준']):
        return '임상시험'

    # 검사/바이탈 관련 (미생물/감염 포함)
    if any(w in q for w in ['혈압', '맥박', '체온', '혈당', 'wbc', 'hb', 'glucose', 'chart', 'lab', '검사', '바이탈',
                            '미생물', '감염', '균', 'infection', 'microbe', '항생제내성', '감수성']):
        return '검사/바이탈'

    # 진단/시술 관련
    if any(w in q for w in ['진단', 'icd', '코드', '시술', 'procedure', '수술', 'diagnosis', 'drg', '진단명', '시술명']):
        return '진단/시술'

    # 약물/투약 관련 (수액/투여 포함)
    if any(w in q for w in ['약', '투약', 'drug', '처방', 'medication', '항생제', 'prescription',
                            '수액', '투여', 'infusion', 'fluid', 'input', '약물']):
        return '약물/투약'

    # 나머지는 모두 환자/입원으로 분류 (ICU/재원 포함)
    return '환자/입원'

def load_schema_and_guide(intent):
    guide_item = GUIDE_MAP.get(intent, GUIDE_MAP.get('기본', {}))
    schema_files = guide_item.get("schema", ["schema_patients.txt"])
    guide = guide_item.get("guide", "")
    context = ""
    for fname in schema_files:
        try:
            with open(fname, encoding='utf-8') as f:
                context += f"\n[{fname}]\n" + f.read() + "\n"
        except FileNotFoundError:
            continue
    return context, guide

langsmith_tracer = LangChainTracer(project_name="model5")
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, api_key=OPENAI_API_KEY,
                callbacks=[token_callback, langsmith_tracer])
embedding = HuggingFaceEmbeddings(model_name="intfloat/multilingual-e5-large")
vectordb = Chroma(
    persist_directory="./chroma_db",
    embedding_function=embedding
)

prompt = PromptTemplate(
    input_variables=["context", "guide", "chat_history", "question", "column_instruction"],
    template="""
[데이터 context]
{context}

[분석/SQL 변환 가이드라인]
{guide}

{column_instruction}

FETCT 대신 ROWNUM을 사용할 것.

반드시 ORACLE SQL 쿼리문을 반환할 것
행 수를 제한 할 때에는 rownum을 사용할 것
반드시 주석 처리 없이 SQL 쿼리문만 반환할 것

ROWNUM <= 100 적용 기준:
1. 단순 SELECT (JOIN 없음): WHERE절에 직접 추가
2. JOIN이 있는 경우: JOIN 조건 뒤, 다른 WHERE 조건과 AND로 연결
3. GROUP BY가 있는 경우: GROUP BY 전에 적용 (그룹화 전 데이터 제한)
4. 서브쿼리 사용: 가장 바깥쪽 쿼리에 적용하여 최종 결과 100개 보장

⚠️ 중요: 올바른 테이블명 사용
- PATIENTS (환자 정보)
- ADMISSIONS (입원 정보)  
- ICUSTAYS (중환자실)
- 절대 schema_patients, schema_admissions 같은 이름 사용 금지

[대화 내용]
{chat_history}

[사용자 질문]
{question}
"""
)


class ChatDebugger:
    def __init__(self):
        self.start_time = time.time()
        self.step_count = 0

    def log(self, message, status="INFO"):
        self.step_count += 1
        elapsed = time.time() - self.start_time
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        status_icon = "🔍" if status == "START" else "✅" if status == "SUCCESS" else "❌" if status == "ERROR" else "⚠️"
        print(f"{status_icon} [{timestamp}] [{elapsed:.3f}s] Step {self.step_count}: {message}")



def extract_faq_from_context(context):
    faq_list = []
    faq_pairs = re.findall(r"Q[:：](.*?)\nA[:：](.*?)(?=\nQ[:：]|\Z)", context, re.DOTALL)
    for q, a in faq_pairs:
        faq_list.append("Q:" + q.strip() + "\nA:" + a.strip())
    return faq_list

def hybrid_search(query, vectordb, keyword_corpus, top_k=3):
    vector_results = vectordb.similarity_search(query, k=top_k*2)
    keyword_hits = []
    for context in keyword_corpus:
        if any(w in context for w in query.split() if len(w) > 1):
            keyword_hits.append(context)
    seen = set()
    merged = []
    for doc in vector_results:
        key = doc.page_content
        if key not in seen:
            merged.append(doc)
            seen.add(key)
    for context in keyword_hits:
        if context not in seen:
            from types import SimpleNamespace
            merged.append(SimpleNamespace(page_content=context))
            seen.add(context)
        if len(merged) >= top_k:
            break
    return merged[:top_k]

memory = ConversationBufferMemory(memory_key="chat_history", k=5, return_messages=True)

def detect_user_intent(msg):
    greetings = ['gd', 'ㅎㅇ', '하이', '하이염', '하이룽', 'hi', 'hello', '헬로', 'hello!', '안녕', '안뇽', '여보세요', '방가', '반가워', '반갑', '굿모닝', '굿밤','헬루', '굿이브닝', '헬로우', '하위', '반모', '헬멧', '헤이', 'yo', '욥', '밥먹었니', '오하요', '모닝', '잘자', '잘잤어', 'good morning', 'good night', 'bye', 'see you', '잘가', 'goodbye']
    positive = ['고마워', '감사', '땡큐', '최고', '잘했어', '굿', '짱', '행복', '기쁘', '즐거워', '재밌', '잘한다', '수고', '예쁘다', '멋지다', '귀엽', '힐링', '사랑', 'good', 'nice', 'very good', 'best', 'awesome', 'thanks', 'thank you']
    negative = ['싫어', '짜증', '피곤', '힘들', '귀찮', '지루', '걱정', '우울', '불안', '별로', '안좋', '지쳐', '힘드네', '답답', '무서워', '아파', '나빠', '짜증나', '시러', '우울해', '현타', '불편', '힘드렁', '서럽', '슬프']
    comfort = ['위로', '위로해줘', '응원', '격려', '힘내', '괜찮', '걱정마', '다 잘될', '파이팅', '힘내자', '토닥', '힘들때', '고생', '괜찮아', '괜차나', '안아줘']
    tired = ['피곤', '지침', '졸려', '졸립', '힘들', '에휴', '휴', '진빠져', '녹초', '피곤해', '진짜 힘들']
    anxiety = ['불안', '걱정', '긴장', '떨려', '불안감', '스트레스', '쫄림', '걱정된다', '두려움']
    slang = ['ㄱㅅ', 'ㄱㄱ', 'ㄱㄷ', 'ㄴㄱ', 'ㄴㄴ', 'ㅇㅇ', 'ㅈㅅ', 'ㅊㅋ', 'ㅋㅋ', 'ㅎㅎ', 'ㅇㅋ', 'ㅇㅈ', 'ㄹㅇ', 'ㅅㅂ', 'ㅅㄱ', 'ㅇㄱㄹㅇ', 'ㅈㄴ', 'ㅁㅊ', 'ㄷㅊ', 'ㄴㅇㅅ', 'ㄹㄷ', 'ㄷㅇ', 'ㄱㄹ', '빠이', '개꿀', 'ㄴㄷㅆ', 'ㅊㅊ', '인싸', '아싸', '쏘쿨', '존맛', '쩐다', '씹덕', '오지네', '짱짱', '레전드', '핵꿀', '대박', '간지', '킹받네', '킹정', '킹왕짱', '웃프다', '스불재', '아오','zz', '쪼아요', '트수', '만렙', '빠방']
    swear = ['씨발', 'ㅅㅂ', 'ㅂㅅ', '병신', '좆', 'ㅗ', 'ㅉ', '개새', 'fuck', 'shit', 'fuck you', 'ㅄ', '염병', '꺼져', '지랄', '젠장', '빡쳐', '미친', '병맛', '개빡쳐', '염병', '병1신', '개노답']
    misspell = ['안냐세요', '감사합니닼', '졸립', '피곤해여', '굳모닝', '굿모닝', '감샤', '하위', '고먀워', '땡뀨', '졸리', '뻐큐', '귀차나', '졸립다', '졸려', '머해', 'ㅈㅅ', 'ㅅㄱ']
    msg_lower = msg.lower()
    result = {
        'greeting': any(word in msg_lower for word in greetings),
        'positive': any(word in msg_lower for word in positive),
        'negative': any(word in msg_lower for word in negative),
        'comfort': any(word in msg_lower for word in comfort),
        'tired': any(word in msg_lower for word in tired),
        'anxiety': any(word in msg_lower for word in anxiety),
        'swear': any(word in msg_lower for word in swear),
        'slang': any(word in msg_lower for word in slang),
        'misspell': any(word in msg_lower for word in misspell)
    }
    return result

def run_sql_query(sql):
    # print(f"🔍 [DEBUG] 원본 SQL: '{sql}'")
    # print(f"🔍 [DEBUG] 원본 길이: {len(sql)}")
    # print(f"🔍 [DEBUG] 원본 repr: {repr(sql)}")

    sql = sql.strip()

    # 세미콜론 처리
    while sql.endswith(';'):
        sql = sql[:-1].strip()

    sql = sql.strip()
    # print(f"🔍 [DEBUG] 처리된 SQL: '{sql}'")
    # print(f"🔍 [DEBUG] 처리된 길이: {len(sql)}")
    # print(f"🔍 [DEBUG] 처리된 repr: {repr(sql)}")
    print("생성 SQL:", sql)

    if not sql.lower().startswith("select"):
        return {"success": False, "error": "SELECT 쿼리만 실행 가능합니다."}

    dsn = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE)
    conn = cx_Oracle.connect(ORACLE_USER, ORACLE_PW, dsn)
    cursor = conn.cursor()
    try:
        # print(f"🔍 [DEBUG] Oracle 실행 직전 SQL: '{sql}'")
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


def extract_sql_and_guide(llm_answer):
    answer = llm_answer.replace("```sql", "").replace("```", "").strip()
    sql_match = re.search(r"(SELECT[\s\S]+?)(?:$|\n\n|\Z)", answer, re.IGNORECASE)
    sql = sql_match.group(1).strip() if sql_match else ""
    guide = answer
    return guide, sql

# === 추가 ===
# Oracle 연결 및 쿼리 실행 함수 추가
def get_oracle_connection():
    """평가 모듈용 Oracle 연결 (get_db_connection 별칭)"""
    return get_db_connection()

def execute_oracle_query(sql, limit=200):
    """Oracle 쿼리 실행 및 결과 반환 (제한된 개수와 전체 데이터 모두 반환)"""
    try:
        conn = get_oracle_connection()  # Oracle 연결 객체 생성
        cursor = conn.cursor()  # 커서 객체 생성
        cursor.execute(sql)  # SQL 쿼리 실행

        columns = [desc[0] for desc in cursor.description]  # 컬럼명 리스트 생성
        all_rows = cursor.fetchall()  # 모든 결과 행 가져오기

        result = []  # 결과를 저장할 리스트 초기화
        for row in all_rows:  # 각 행에 대해 반복
            row_dict = {}  # 행 데이터를 저장할 딕셔너리 초기화
            for i, value in enumerate(row):  # 각 컬럼 값에 대해 반복
                row_dict[columns[i]] = value  # 컬럼명을 키로 하는 딕셔너리 생성
            result.append(row_dict)  # 결과 리스트에 행 딕셔너리 추가

        cursor.close()  # 커서 닫기
        conn.close()  # 연결 닫기

        return {
            "success": True,  # 성공 여부
            "data": result[:limit],  # 제한된 개수 (미리보기용)
            "all_data": result,      # 전체 데이터 (CSV 다운로드용)
            "columns": columns  # 컬럼명 리스트
        }

    except Exception as e:  # 예외 발생 시
        return {
            "success": False,  # 실패 여부
            "error": str(e)  # 오류 메시지
        }
# === 추가 ===

@app.route('/chat', methods=['POST'])
def chat():
    # 🔥 전체 턴 시간 측정 시작 (질문 입력 시점)
    turn_start_time = time.time()

    # 🔥 디버깅 코드 추가
    debugger = ChatDebugger()
    debugger.log("채팅 요청 시작", "START")

    eval_result = None
    evaluation_completed = False  # 🔥 평가 완료 플래그
    data = request.json
    user_msg = data['message']

    # === 추가 ===
    gold_sql = request.json.get('gold_sql', '')
    # print(f"🔍 [DEBUG] gold_sql 원본 타입: {type(gold_sql).__name__}")
    # print(f"🔍 [DEBUG] gold_sql 원본 값: {repr(gold_sql)}")
    # print(f"🔍 [DEBUG] request.json 전체: {request.json}")
    print("\n","\n",f"🔍 [INFO] === SQL 처리 시작 ===")
    print(f"  └ 질문: \"{user_msg}\"")
    if gold_sql:
        preview = str(gold_sql)[:50] + "..." if len(str(gold_sql)) > 50 else str(gold_sql)
        print(f"  └ Gold SQL: {preview} ({type(gold_sql).__name__}/{len(str(gold_sql))}자)")
    else:
        print(f"  └ Gold SQL: 없음")
    # === 추가 ===
    chat_history = data.get('chat_history', [])
    chat_history = chat_history[-5:]

    # 🔥 디버깅 코드 추가
    debugger.log("의도 분석 시작", "START")

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
    elif intent_check['negative'] or intent_check['tired'] or intent_check['anxiety']:
        return jsonify({
            "sql": "",
            "db_result": [],
            "all_result": [],
            "db_error": None,
            "report_text": "많이 지치셨나 봐요. 궁금한 점이 있다면 도와드릴게요.",
            "columns": []
        })
    elif intent_check['comfort']:
        return jsonify({
            "sql": "",
            "db_result": [],
            "all_result": [],
            "db_error": None,
            "report_text": "힘들 땐 잠시 쉬어가는 것도 좋아요. 필요하시면 언제든 말씀해주세요.",
            "columns": []
        })
    elif intent_check['slang'] or intent_check['misspell']:
        return jsonify({
            "sql": "",
            "db_result": [],
            "all_result": [],
            "db_error": None,
            "report_text": "메시지를 잘 이해했어요! 다른 것도 궁금하신가요?",
            "columns": []
        })

    # 핵심 처리부 - 수정된 버전
    intent = infer_intent(user_msg)
    context, guide = load_schema_and_guide(intent)

    # 🔥 컬럼 강제 지시문 생성 (새로 추가)
    column_instruction = column_manager.generate_column_instruction(intent)

    docs = vectordb.similarity_search(user_msg, k=3)

    # FAQ는 VectorDB 검색 결과에서 추출 (기존 형식 유지)
    retrieved_context = "\n\n".join([d.page_content if hasattr(d, 'page_content') else d for d in docs])
    faq_corpus = extract_faq_from_context(retrieved_context)

    # === LLM 전달 정보 디버깅 추가 (기존 형식 유지) ===
    # print(f"\n{'=' * 60}")
    # print(f"🔍 [DEBUG] LLM 전달 정보 확인")
    # print(f"{'=' * 60}")
    # print(f"📊 Intent: {intent}")
    # print(f"📊 VectorDB 문서 수: {len(docs)}")
    # print(f"📊 FAQ corpus 크기: {len(faq_corpus)}")

    if docs:
        # print(f"\n📋 검색된 문서들:")
        for i, doc in enumerate(docs):
            # print(f"\n   {'=' * 50}")
            # print(f"   📄 문서 {i + 1}")
            # print(f"   {'=' * 50}")

            # 메타데이터 정보
            if hasattr(doc, "metadata"):
                source = doc.metadata.get('source', 'unknown')
                doc_type = doc.metadata.get('type', 'unknown')
                table = doc.metadata.get('table', '')
                # print(f"   📂 출처: {source}")
                # print(f"   🏷️  타입: {doc_type}")
                # if table:
                    # print(f"   📋 테이블: {table}")

            # 문서 내용
            if hasattr(doc, "page_content"):
                content = doc.page_content
                # print(f"   📏 길이: {len(content)}자")
                # print(f"   ─── 내용 시작 ───")

    #             # 처음 500자를 줄별로 출력 (최대 5줄)
    #             lines = content[:500].split('\n')[:5]
    #             for line_num, line in enumerate(lines, 1):
    #                 if line.strip():  # 빈 줄 제외
    #                     print(f"   {line_num:2d}│ {line}")
    #
    #             if len(content) > 500:
    #                 print(f"   ...│ (총 {len(content)}자 중 처음 500자만 표시)")
    #             print(f"   ─── 내용 끝 ───")
    #         else:
    #             content = str(doc)
    #             print(f"   📏 길이: {len(content)}자")
    #             print(f"   ─── 내용 ───")
    #             print(f"   {content[:500]}...")
    #             print(f"   ─── 끝 ───")
    # else:
    #     print("❌ 검색된 문서가 없습니다!")

    # 🔥 핵심 변경: VectorDB 검색 결과만 사용 (중복 제거)
    context = retrieved_context  # 원래 context 대신 검색 결과만!

    # print(f"\n📂 Schema 파일 정보:")
    # print(f"   └ Schema context 길이: {len(context)}자")  # 이제 작아짐!
    # print(f"   └ Guide 길이: {len(guide)}자")
    # if context:
    #     print(f"   ─── Schema 내용 미리보기 ───")
    #     lines = context[:500].split('\n')[:5]  # 처음 5줄
    #     for line_num, line in enumerate(lines, 1):
    #         if line.strip():
    #             print(f"   {line_num:2d}│ {line}")
    #     if len(context) > 500:
    #         print(f"   ...│ (총 {len(context)}자 중 처음 500자만 표시)")
    #     print(f"   ─── Schema 미리보기 끝 ───")

    # 🔥 핵심 변경: 중복 합치기 제거
    final_context = context + "\n\n" + retrieved_context if retrieved_context else context

    # === [CONTEXT] 레벨: 컨텍스트 정보 ===
    # print(f"\n🔍 [CONTEXT] === 컨텍스트 준비 완료 ===")
    # print(f"  └ 최종 컨텍스트: {len(final_context)}자")  # 대폭 감소!
    # print(f"  └ 사용자 질문: {len(user_msg)}자")

    # === [TOKEN] 레벨: 토큰 예측 ===
    # print(f"\n🔍 [TOKEN] === 토큰 사용량 예측 ===")
    total_estimated_tokens = estimate_token_usage(final_context + "\n" + user_msg)
    estimated_tokens = {
        'context_tokens': estimate_token_usage(final_context),
        'question_tokens': estimate_token_usage(user_msg),
        'total_prompt_tokens': total_estimated_tokens
    }
    context_tokens = estimated_tokens['context_tokens']
    question_tokens = estimated_tokens['question_tokens']
    total_estimated = estimated_tokens['total_prompt_tokens']
    # print(f"  └ 예상: 컨텍스트 {context_tokens} + 질문 {question_tokens} = 총 {total_estimated} 토큰")

    column_instruction = column_manager.generate_column_instruction(intent)

    print(f"\n🔍 [DEBUG] 컬럼 지시문 확인:")
    print(f"  └ Intent: {intent}")
    print(f"  └ Column instruction: {repr(column_instruction)}")
    print(f"  └ Column instruction 길이: {len(column_instruction)}")

    # 실제 사용자 설정 확인
    column_info = column_manager.get_columns_for_intent(intent)
    print(f"  └ Essential 컬럼: {column_info['essential']}")
    print(f"  └ 사용자 선택 컬럼: {column_info['user_selected']}")

    prompt_text = prompt.format(
        context=final_context,
        guide=guide,
        chat_history=chat_history,
        question=user_msg,
        column_instruction=column_instruction  # 🔥 이 줄 추가
    )

    # === LLM 호출 및 토큰 추적 ===
    print("\n", f"🔍 [LLM] === LLM 처리 시작 ===")
    debugger.log("LLM 호출 시작", "START")
    try:
        llm_answer = llm.invoke(prompt_text, config={"callbacks": [token_callback, langsmith_tracer]}).content
        debugger.log("LLM 호출 완료", "SUCCESS")
        print(f"  └ LLM 응답 완료")

        # 토큰 사용량 기록
        token_record = record_token_usage(
            user_question=user_msg,
            generated_sql="",  # SQL 추출 전이므로 빈 값
            response_text=llm_answer,
            estimated_tokens=estimated_tokens,
            actual_usage=None,  # LangChain은 실제 토큰 정보 제공 안함
            execution_success=False  # 아직 SQL 실행 전
        )
        # print(f"🔍 [TOKEN] 토큰 사용량 기록 완료")

    except Exception as llm_error:
        print(f"❌ [TOKEN] LLM 호출 실패: {llm_error}")

        # LLM 실패 시에도 토큰 기록
        record_token_usage(
            user_question=user_msg,
            generated_sql="",
            response_text="",
            estimated_tokens=estimated_tokens,
            actual_usage=None,
            execution_success=False
        )

        return jsonify({
            "sql": "",
            "db_result": [],
            "all_result": [],
            "db_error": None,
            "report_text": f"LLM 처리 중 오류가 발생했습니다: {str(llm_error)[:100]}",
            "columns": []
        })
    # === LLM 호출 및 토큰 추적 끝 ===
    debugger.log("SQL 추출 시작", "START")

    guide_text, sql = extract_sql_and_guide(llm_answer)
    # === SQL 추출 후 토큰 기록 업데이트 ===
    # print(f"🔍 [TOKEN] SQL 추출 완료: {sql[:50] if sql else 'None'}...")

    # 토큰 기록 업데이트 (SQL 포함)
    if sql:
        updated_token_record = record_token_usage(
            user_question=user_msg,
            generated_sql=sql,
            response_text=llm_answer,
            estimated_tokens=estimated_tokens,
            actual_usage=None,
            execution_success=False  # 아직 실행 전
        )
    # === 토큰 기록 업데이트 끝 ===

    if not sql:
        # 🔥 디버깅 코드 추가
        debugger.log("SQL 없음으로 조기 종료", "SKIP")
        return jsonify({
            "db_error": None,
            "report_text": guide_text.strip(),
            "db_result": [],
            "columns": []
        })

    # 🔥 디버깅 코드 추가
    debugger.log("SQL 실행 시작", "START")

    # app.py의 chat() 함수에서 SQL 실행 부분 구조 변경

    # ===============================================
    # 기존 문제 구조 (중복 호출)
    # ===============================================
    # if db_result["success"]:
    #     # 성공 처리 + evaluate_and_save 호출
    # else:
    #     # 실패 처리 + evaluate_and_save 호출  ← 중복!

    # ===============================================
    # 새로운 구조 (한 번만 호출)
    # ===============================================

    # SQL 실행
    db_result = run_sql_query(sql)

    # 🔥 공통 변수 초기화
    exec_success = False
    result_count = 0
    error = None
    all_rows = []
    preview_rows = []
    columns = []
    report_text = guide_text
    rag_evaluation = {}

    # 🔥 성공/실패에 따른 데이터 준비만
    if db_result["success"]:
        print(f"✅ SQL 실행 성공: {len(db_result.get('result', []))}행")

        # 성공 시 데이터 준비
        exec_success = True
        all_rows = db_result.get("result", [])
        columns = db_result.get("columns", [])
        preview_rows = all_rows[:100]
        result_count = len(all_rows)
        error = None

        # 성공 시 요약 텍스트 생성
        if preview_rows:
            report_prompt = f"""아래 표는 사용자의 질의에 대한 결과입니다.
        딱 한 줄로 결과 의미만 설명하세요.
        SQL, 칼럼설명, 예시 등은 답변에 포함하지 마세요.
        예시) 20대 남성 골절 진단 환자 명단입니다.

        컬럼: {', '.join(columns)}
        """
            report_resp = llm.invoke(report_prompt)
            summary_text = getattr(report_resp, "content", str(report_resp)).strip()
            report_text = f"{summary_text}\n(자세한 정보와 표는 '결과창'에서 확인하세요.)"
        else:
            report_text = "결과가 없습니다."
    else:
        report_text = db_result.get("error", "")

        # 실패 시 데이터 준비
        exec_success = False
        result_count = 0
        error = db_result.get('error')
        all_rows = []
        preview_rows = []
        columns = []

        # 실패 시 의미있는 리포트 생성
        if sql:
            report_text = f"생성된 SQL: {sql}\n실행 오류: {db_result.get('error', '알 수 없는 오류')}"

            # 오류 타입별 개선 제안
            error_msg = str(db_result.get('error', '')).lower()
            if "column" in error_msg or "invalid identifier" in error_msg:
                report_text += "\n\n💡 제안: 존재하지 않는 컬럼을 참조했습니다. 스키마를 확인해보세요."
            elif "table" in error_msg:
                report_text += "\n\n💡 제안: 존재하지 않는 테이블을 참조했습니다. 테이블명을 확인해보세요."
            elif "syntax" in error_msg:
                report_text += "\n\n💡 제안: SQL 문법 오류가 있습니다. 구문을 검토해보세요."
            elif "ora-" in error_msg:
                report_text += "\n\n💡 제안: Oracle 데이터베이스 오류입니다. 오류 코드를 확인해보세요."
        else:
            report_text = "SQL 생성에 실패했습니다. 질문을 다시 명확히 해주세요."

    # 🔥 RAG 평가 (성공/실패 무관하게 수행)
    if context and context.strip():
        try:
            # 도메인 특화 RAG 평가
            domain_rag = evaluate_new_rag_metrics(user_msg, context, sql)
            # LangSmith 표준 RAG 평가
            langsmith_rag = evaluate_langsmith_rag_metrics(user_msg, context, sql)
            # 두 평가 결과 통합
            rag_evaluation = {**domain_rag, **langsmith_rag}

            print(f"🔍 [RAG] === RAG 평가 완료 (도메인 + LangSmith) ===")
            # print(f"  └ 도메인 메트릭: {list(domain_rag.keys())}")
            # print(f"  └ LangSmith 메트릭: {list(langsmith_rag.keys())}")
        except Exception as e:
            print(f"⚠️ RAG 평가 실패: {e}")
            rag_evaluation = {}

    # RAG 평가 결과를 sql_evaluator에 저장
    sql_evaluator.last_rag_evaluation = rag_evaluation

    # 🔥 토큰 정보 추출
    # 🔥 토큰 정보 추출 - 강화된 방식
    token_info = None
    print(f"🔍 [APP_TOKEN] 토큰 추출 시도...")

    if hasattr(token_callback, 'last_token_usage') and token_callback.last_token_usage:
        token_info = token_callback.last_token_usage
        print(f"✅ [APP_TOKEN] 콜백에서 토큰 추출: {token_info}")
    else:
        print(f"❌ [APP_TOKEN] 콜백 토큰 없음, LangSmith에서 추출 시도...")
        # LangSmith API로 직접 조회 (대안)
        try:
            from langsmith import Client
            client = Client()
            # 최근 실행의 토큰 정보 조회
            print(f"⚠️ [APP_TOKEN] LangSmith 직접 조회는 구현 필요")
        except:
            pass

    # 🔥 target_sql_result 준비
    target_sql_result = []
    if gold_sql and gold_sql.strip():
        try:
            target_db_result = run_sql_query(gold_sql)
            if target_db_result["success"]:
                target_sql_result = target_db_result.get("result", [])
        except Exception as e:
            print(f"⚠️ 정답 SQL 실행 실패: {e}")

    # 🔥 🔥 🔥 핵심: 한 번만 evaluate_and_save 호출
    try:
        print(f"🔍 [APP_DEBUG] evaluate_and_save 통합 호출")
        # print(f"  └ exec_success: {exec_success}")
        # print(f"  └ result_count: {result_count}")
        # print(f"  └ error: {error}")

        eval_result = evaluate_and_save(
            user_question=user_msg,
            generated_sql=sql,
            gold_sql=gold_sql if (gold_sql and isinstance(gold_sql, str) and gold_sql.strip()) else None,
            exec_success=exec_success,  # 🔥 성공/실패 상태
            result_count=result_count,  # 🔥 결과 행 수
            error=error,  # 🔥 오류 정보 (있으면)
            context=context,
            actual_usage=token_info,
            generated_sql_result=all_rows,  # 🔥 생성 SQL 결과
            target_sql_result=target_sql_result,  # 🔥 정답 SQL 결과
            rag_evaluation=rag_evaluation,
            turn_start_time=turn_start_time
            # skip_multiturn 파라미터 제거! 더이상 필요없음
        )

        print(f"🔍 [APP_DEBUG] evaluate_and_save 통합 호출 완료")

    except Exception as e:
        print(f"❌ [APP_DEBUG] evaluate_and_save 실패: {e}")
        import traceback
        traceback.print_exc()
        eval_result = None

    # 🔥 디버깅 코드
    debugger.log("전체 처리 완료", "SUCCESS")

    # 최종 응답 반환
    return jsonify({
        "sql": sql,
        "db_result": preview_rows,
        "all_result": all_rows,  # 🔥 실패 시 빈 배열
        "db_error": error,  # 🔥 실패 시 오류 메시지
        "report_text": report_text,
        "columns": columns,
        "evaluation": {
            "individual_result": get_individual_evaluation_result(),
            "basic_metrics": eval_result,
            "session_status": {
                "has_session": bool(
                    sql_evaluator.multiturn_manager and sql_evaluator.multiturn_manager.current_session),
                "session_id": sql_evaluator.multiturn_manager.current_session.session_id if sql_evaluator.multiturn_manager and sql_evaluator.multiturn_manager.current_session else None
            }
        }
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


def get_db_connection():
    """Oracle Cloud Autonomous Database 연결 (Wallet 사용)"""
    try:
        # Wallet 경로 - Render 환경 변수에서 가져옴
        wallet_location = os.getenv("WALLET_LOCATION", "./wallet")
        wallet_password = os.getenv("WALLET_PASSWORD", "")

        # TNS_ADMIN 환경 변수 설정 (Wallet 폴더 위치)
        os.environ["TNS_ADMIN"] = wallet_location

        # Oracle Cloud 연결 정보
        service_name = os.getenv("ORACLE_SERVICE_NAME", "oraclefinalproject_high")
        username = os.getenv("ORACLE_USER", "ADMIN")
        password = os.getenv("ORACLE_PW")

        # DSN 방식으로 연결
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
        print(f"Oracle 오류 (로그인): {error_obj.message}")
        return jsonify({"success": False, "message": "서버 오류: 데이터베이스 문제."}), 500
    except Exception as e:
        print(f"예상치 못한 오류 (로그인): {e}")
        return jsonify({"success": False, "message": "서버 오류: 알 수 없는 문제."}), 500
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
        if conn: conn.rollback()
        return jsonify({'success': False, 'message': '서버 오류: ' + str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


# 1. 파일 업로드 (공통/1단계)
@app.route('/upload', methods=['POST'])
def upload():
    files = request.files.getlist('files')
    # 실제 파일 저장/검증/전처리 로직 구현 필요
    if not files:
        return jsonify({'success': False, 'message': '업로드된 파일이 없습니다.'})
    # 파일 저장 예시
    for file in files:
        file.save(f'./uploads/{file.filename}')
    return jsonify({'success': True, 'message': '파일 업로드 성공!'})

@app.route('/upload_step1_data', methods=['POST'])
def upload_step1_data():
    files = request.files.getlist('files')
    if not files:
        return jsonify({'success': False, 'message': '1단계 업로드 파일 없음'})
    for file in files:
        file.save(f'./uploads/step1_{file.filename}')
    return jsonify({'success': True, 'message': '1단계 파일 업로드 완료!'})

# 2. 데이터 전처리
@app.route('/preprocess_data', methods=['POST'])
def preprocess_data():
    data = request.json
    handle_missing = data.get('handle_missing')
    normalize_data = data.get('normalize_data')
    # 실제 전처리 로직(예: pandas로 결측치 대체, 정규화 등)
    # ...
    return jsonify({'success': True, 'message': '전처리 완료'})

# 3. 분석 실행
@app.route('/analyze', methods=['POST'])
def analyze():
    data = request.json
    analysis_type = data.get('analysis_type')
    variables = data.get('variables', [])
    # 실제 분석 로직(요약, 결측치, 상관관계, etc)
    results = [{
        'tool': analysis_type,
        'variables': variables,
        'results': {"예시": "여기에 결과 데이터"},
        'chart': None  # 차트는 base64 인코딩 이미지
    }]
    return jsonify({'success': True, 'results': results})

@app.route('/run_eda', methods=['POST'])
def run_eda():
    data = request.json
    variables = data.get('variables', [])
    # 실제 EDA 결과 생성
    results = [{
        'tool': 'EDA',
        'variables': variables,
        'results': {"설명": "탐색적 데이터 분석 결과 예시"},
        'chart': None
    }]
    return jsonify({'success': True, 'results': results})

# 4. 모델 학습 및 평가
@app.route('/train_model', methods=['POST'])
def train_model():
    data = request.json
    model_type = data.get('model_type')
    target_variable = data.get('target_variable')
    feature_variables = data.get('feature_variables', [])
    # 실제 모델 학습 및 평가 로직
    results = [{
        'tool': model_type,
        'variables': feature_variables,
        'results': {"예측결과": "여기에 학습/평가 결과"},
        'chart': None
    }]
    return jsonify({'success': True, 'results': results})

# 5. 최종 리포트 생성
@app.route('/generate_report', methods=['GET'])
def generate_report():
    # 실제 리포트 텍스트/마크다운/이미지 등 생성
    report_content = "# 분석 리포트\n\n분석 요약 및 결과가 여기에 출력됩니다."
    return jsonify({'success': True, 'report_content': report_content})


# === column_manager 관련 코드 추가 ===
@app.route('/api/get_column_settings', methods=['GET'])
def get_column_settings():
    """컬럼 설정 데이터 반환"""
    try:
        all_intents = column_manager.get_all_intents()
        settings_data = {}

        for intent in all_intents:
            settings_data[intent] = column_manager.get_columns_for_intent(intent)

        return jsonify(settings_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/save_column_settings', methods=['POST'])
def save_column_settings():
    try:
        data = request.json
        success = column_manager.save_user_settings(data)

        if success:
            return jsonify({"success": True, "message": "설정이 저장되었습니다."})
        else:
            return jsonify({"success": False, "message": "저장에 실패했습니다."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
# === column_manager 관련 코드 추가 ===


# === 평가 관련 엔드포인트 추가 ===
@app.route('/individual_evaluation', methods=['POST'])
def individual_evaluation():
    try:
        data = request.json or {}
        provided_gold_sql = data.get('gold_sql', '')
        user_question = data.get('user_question', '')

        # 멀티턴 관리자 확인
        if not hasattr(sql_evaluator, 'multiturn_manager') or not sql_evaluator.multiturn_manager:
            return jsonify({
                "success": False,
                "error": "멀티턴 평가 관리자가 초기화되지 않았습니다."
            })

        manager = sql_evaluator.multiturn_manager

        # 🔥 현재 세션 상태 디버깅
        print(f"🔍 [DEBUG] 개별평가 요청 - 현재 세션: {manager.current_session}")
        if manager.current_session:
            print(f"🔍 [DEBUG] 세션 ID: {manager.current_session.session_id}")
            print(f"🔍 [DEBUG] 세션 상태: {manager.current_session.status}")
            print(f"🔍 [DEBUG] 턴 수: {len(manager.current_session.turns)}")

        # 결과 생성 (없으면 강제 생성)
        if manager.current_session:
            result_text = manager.generate_multiturn_evaluation_report()
        else:
            # 파일에서 최근 세션 로드해서 결과 생성
            try:
                if os.path.exists(manager.session_file):
                    with open(manager.session_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    sessions = data.get('multiturn_sessions', [])
                    if sessions:
                        # 가장 최근 세션으로 결과 생성
                        latest_session = sessions[-1]
                        from evaluation_module import MultiTurnSession
                        session_obj = MultiTurnSession(latest_session['session_id'], latest_session['max_turns'])
                        session_obj.turns = latest_session['turns']
                        session_obj.status = latest_session['status']
                        result_text = manager._format_individual_evaluation_report(session_obj)
                    else:
                        result_text = "📋 평가할 세션이 없습니다. 새 세션을 시작하세요."
                else:
                    result_text = "📋 세션 파일이 없습니다. 새 세션을 시작하세요."
            except Exception as file_error:
                result_text = f"📋 세션 파일 로드 실패: {file_error}"

        # 결과가 여전히 비어있으면 기본 메시지
        if not result_text or result_text.strip() == "":
            result_text = "📋 멀티턴 개별 평가 대기 중\n\n활성화된 멀티턴 세션이 없습니다."

        return jsonify({
            "success": True,
            "result": result_text,
            "message": "멀티턴 개별 평가 완료",
            "debug_info": {
                "has_manager": bool(manager),
                "has_current_session": bool(manager.current_session),
                "session_id": manager.current_session.session_id if manager.current_session else None,
                "session_file_exists": os.path.exists(manager.session_file)
            }
        })

    except Exception as e:
        print(f"❌ 개별평가 오류: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": f"멀티턴 개별평가 중 오류 발생: {str(e)}"
        })


# === 3. 새로운 엔드포인트 추가: 세션 시작 ===
@app.route('/start_session', methods=['POST'])
def start_session():
    """새로운 멀티턴 세션 시작 API"""
    try:
        data = request.json or {}
        max_turns = data.get('max_turns', 5)  # 기본 5턴

        # 턴 수 검증
        if not isinstance(max_turns, int) or max_turns < 1 or max_turns > 20:
            return jsonify({
                "success": False,
                "error": "턴 수는 1~20 사이의 숫자여야 합니다."
            })

        # 멀티턴 관리자 확인
        if not hasattr(sql_evaluator, 'multiturn_manager') or not sql_evaluator.multiturn_manager:
            return jsonify({
                "success": False,
                "error": "멀티턴 평가 관리자가 초기화되지 않았습니다."
            })

        # 새 세션 시작
        session_id = start_multiturn_session(max_turns=max_turns)

        if session_id:
            return jsonify({
                "success": True,
                "session_id": session_id,
                "max_turns": max_turns,
                "message": f"새 멀티턴 세션이 시작되었습니다: {session_id} ({max_turns}턴)"
            })
        else:
            return jsonify({
                "success": False,
                "error": "세션 시작에 실패했습니다."
            })

    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"세션 시작 중 오류 발생: {str(e)}"
        })

@app.route('/overall_evaluation', methods=['POST'])
def overall_evaluation():
    """
    멀티턴 전체 평가 엔드포인트 - 완료된 세션들의 통계 분석만
    진행 중 세션이 있으면 경고 메시지 표시
    """
    try:
        # 멀티턴 관리자 확인
        if not hasattr(sql_evaluator, 'multiturn_manager') or not sql_evaluator.multiturn_manager:
            return jsonify({
                "success": False,
                "error": "멀티턴 평가 관리자가 초기화되지 않았습니다."
            })

        # === 🔥 핵심 개선: 진행 중 세션 확인 ===
        current_session = sql_evaluator.multiturn_manager.current_session
        has_active_session = (current_session and current_session.status == "진행중")

        if has_active_session:
            # 진행 중 세션이 있으면 경고와 함께 제한된 통계만 제공
            warning_msg = f"⚠️  현재 진행 중인 세션이 있습니다: {current_session.session_id}\n"
            warning_msg += f"   (Turn {len(current_session.turns)}/{current_session.max_turns})\n\n"
            warning_msg += "완전한 전체 평가를 위해서는 현재 세션을 완료하세요.\n"
            warning_msg += "(정답 달성 또는 턴 제한 도달)\n\n"
            warning_msg += "--- 기존 완료 세션들의 제한된 통계 ---\n\n"

            # 완료된 세션들만으로 통계 생성
            result = sql_evaluator.multiturn_manager.generate_multiturn_aggregate_report()

            return jsonify({
                "success": True,
                "result": warning_msg + result,
                "message": "제한된 전체 평가 (진행 중 세션 있음)",
                "has_active_session": True,  # 🔥 활성 세션 플래그
                "active_session_id": current_session.session_id
            })
        else:
            # 진행 중 세션이 없으면 정상적인 전체 평가
            result = sql_evaluator.multiturn_manager.generate_multiturn_aggregate_report()

            return jsonify({
                "success": True,
                "result": result,
                "message": "멀티턴 전체 평가 완료",
                "has_active_session": False  # 🔥 비활성 세션 플래그
            })

    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"멀티턴 전체평가 중 오류 발생: {str(e)}",
            "has_active_session": False
        })


# === 5. 기존 session_status 엔드포인트 업데이트 (있는 경우) ===
@app.route('/session_status', methods=['GET'])
def session_status():
    """현재 멀티턴 세션 상태 조회 API"""
    try:
        if not hasattr(sql_evaluator, 'multiturn_manager') or not sql_evaluator.multiturn_manager:
            return jsonify({
                "success": False,
                "error": "멀티턴 평가 관리자가 초기화되지 않았습니다."
            })

        current_session = sql_evaluator.multiturn_manager.current_session

        if current_session and current_session.status == "진행중":
            return jsonify({
                "success": True,
                "has_session": True,
                "session_info": {
                    "session_id": current_session.session_id,
                    "status": current_session.status,
                    "turns": len(current_session.turns),
                    "max_turns": current_session.max_turns
                }
            })
        else:
            return jsonify({
                "success": True,
                "has_session": False,
                "session_info": None
            })

    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"세션 상태 조회 중 오류: {str(e)}",
            "has_session": False
        })

# === 4. 기존 finish_session 엔드포인트 수정 (있는 경우) ===
@app.route('/finish_session', methods=['POST'])
def finish_session():
    """현재 멀티턴 세션 완료 API"""
    try:
        if not hasattr(sql_evaluator, 'multiturn_manager') or not sql_evaluator.multiturn_manager:
            return jsonify({
                "success": False,
                "error": "멀티턴 평가 관리자가 초기화되지 않았습니다."
            })

        current_session = sql_evaluator.multiturn_manager.current_session

        if current_session and current_session.status == "진행중":
            session_id = current_session.session_id
            sql_evaluator.multiturn_manager.finish_current_session()

            return jsonify({
                "success": True,
                "message": f"세션 {session_id}이 완료되었습니다.",
                "finished_session_id": session_id
            })
        else:
            return jsonify({
                "success": False,
                "error": "완료할 활성 세션이 없습니다."
            })

    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"세션 완료 중 오류: {str(e)}"
        })


@app.route('/token_statistics', methods=['GET'])
def token_statistics():
    """토큰 사용량 통계 조회 API"""
    try:
        stats = get_token_statistics()
        return jsonify({
            "success": True,
            "data": stats
        })
    except Exception as e:
        print(f"❌ 토큰 통계 조회 오류: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        })


@app.route('/evaluation_export', methods=['GET'])
def evaluation_export():
    """평가 결과 CSV 내보내기"""
    try:
        import csv
        import io
        from datetime import datetime

        # 모든 평가 결과 조회
        evaluations = sql_evaluator.get_all_evaluations()

        if not evaluations:
            return jsonify({
                "success": False,
                "error": "내보낼 평가 결과가 없습니다."
            })

        # CSV 생성
        output = io.StringIO()
        fieldnames = [
            'id', 'timestamp', 'user_question', 'generated_sql',
            'syntax_correct', 'execution_success', 'result_count',
            'has_error', 'exact_match', 'execution_match'
        ]

        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for eval_data in evaluations:
            row = {
                'id': eval_data.get('id', ''),
                'timestamp': eval_data.get('timestamp', ''),
                'user_question': eval_data.get('user_question', ''),
                'generated_sql': eval_data.get('generated_sql', ''),
                'syntax_correct': eval_data.get('syntax_correct', ''),
                'execution_success': eval_data.get('execution_success', ''),
                'result_count': eval_data.get('result_count', 0),
                'has_error': eval_data.get('has_error', ''),
                'exact_match': eval_data.get('gold_comparison', {}).get('exact_match', ''),
                'execution_match': eval_data.get('gold_comparison', {}).get('execution_match', '')
            }
            writer.writerow(row)

        output.seek(0)

        return output.getvalue(), 200, {
            'Content-Type': 'text/csv; charset=utf-8',
            'Content-Disposition': f'attachment; filename="evaluation_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"'
        }

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        })


@app.route('/end_session', methods=['POST'])
def end_session():
    """멀티턴 세션 수동 종료"""
    try:
        if (hasattr(sql_evaluator, 'multiturn_manager') and
                sql_evaluator.multiturn_manager and
                sql_evaluator.multiturn_manager.current_session):

            session_id = sql_evaluator.multiturn_manager.current_session.session_id
            sql_evaluator.multiturn_manager.finish_current_session()

            return jsonify({
                "success": True,
                "message": f"세션 {session_id}이 종료되었습니다."
            })
        else:
            return jsonify({
                "success": False,
                "message": "종료할 활성 세션이 없습니다."
            })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        })

# === 평가 엔드포인트 끝 ===

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
