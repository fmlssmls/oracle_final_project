# evaluation_module.py 전체 파일 (멀티턴 버전 v2 - 새로운 요구사항 반영)

# === SQL 평가 기능 + 토큰 사용량 추적 통합 모듈 ===

import os  # 파일 시스템 접근
import sys  # 시스템 경로 관리
import json  # JSON 파일 처리
from datetime import datetime  # 시간 정보
import cx_Oracle  # Oracle DB 연결
import re  # 정규표현식
from langsmith import Client
from langsmith.run_helpers import traceable
from langchain.callbacks.base import BaseCallbackHandler  # LangChain 콜백
import hashlib
import time
from typing import Dict, Any, Optional

# === [1] 평가 모듈 import ===
sys.path.append('.')
from evaluation import Evaluator, eval_exec_match
from process_sql import get_sql, Schema

# === 🔥 SParC 공식 함수들 추가 ===

# 함수 정규화 함수들
FORMATTING_FUNCTIONS = ('lower', 'upper', 'trim', 'ltrim', 'rtrim')

# === 도메인 매핑 정의 ===
DOMAIN_KEYWORDS = {
    'patients': ['환자', '나이', '성별', '입원', '퇴원', '사망', 'patient', 'age', 'gender', 'admit', 'discharge'],
    'diagproc': ['진단', '질병', 'ICD', '시술', '수술', 'diagnosis', 'procedure', 'surgery', 'disease'],
    'drugs': ['약물', '처방', '투약', '용량', '항생제', 'drug', 'medication', 'prescription', 'dose'],
    'events': ['검사', '수치', '측정', '모니터링', '혈압', '맥박', 'lab', 'chart', 'vital', 'test'],
    'trial': ['시험', '연구', '임상', '치료효과', 'trial', 'clinical', 'research', 'study']
}

SQL_COMPLEXITY_WEIGHTS = {
    'select': 1, 'from': 1, 'where': 2, 'join': 3, 'group': 3, 'order': 2,
    'having': 4, 'union': 4, 'intersect': 4, 'except': 4
}

# === 평가 관련 상수 정의 ===
STANDARD_CLAUSES = [
    'select', 'select(no AGG)', 'where', 'where(no OP)',
    'group(no Having)', 'group', 'order', 'and/or', 'IUEN', 'keywords'
]


# === 스키마 관련 함수들 ===
def extract_schema_dict_from_txt():
    """txt 파일들로부터 스키마 딕셔너리 생성"""
    schema_files = [
        "schema_patients.txt",
        "schema_diagproc.txt",
        "schema_drugs.txt",
        "schema_events.txt",
        "schema_trial.txt"
    ]

    combined_schema = {}

    for file_path in schema_files:
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # CREATE TABLE 문에서 테이블과 컬럼 추출
                table_matches = re.findall(r'CREATE TABLE (\w+)\s*\((.*?)\);', content, re.DOTALL | re.IGNORECASE)

                for table_name, columns_text in table_matches:
                    table_name = table_name.lower()

                    # 컬럼명 추출 (컬럼명 컬럼타입 형태)
                    column_matches = re.findall(r'(\w+)\s+[A-Za-z0-9_\(\),\s]+', columns_text)
                    columns = [col.lower() for col in column_matches if
                               col.lower() not in ['constraint', 'primary', 'foreign', 'key', 'references']]

                    if columns:
                        combined_schema[table_name] = columns

            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                continue

    print(f"✅ 스키마 파일 로딩 완료: {len(combined_schema)}개 테이블")
    return combined_schema


# === Oracle SQL 정규화 ===
def normalize_oracle_sql_for_comparison(sql_str):
    """Oracle SQL을 SParC 평가용으로 정규화"""
    # 1. 기본 전처리
    sql = str(sql_str).strip()
    if not sql:
        return ""

    # 2. 대소문자 통일 (대문자로)
    sql = sql.upper()

    # 3. 세미콜론 완전 제거
    while sql.endswith(';'):
        sql = sql[:-1].strip()
    sql = re.sub(r';\s*;+', '', sql)  # 연속 세미콜론
    sql = re.sub(r';\s*$', '', sql)  # 끝 세미콜론

    # 4. 불필요한 공백 정리
    sql = re.sub(r'\s+', ' ', sql).strip()

    # 5. Oracle 특수 구문 정리
    # 테이블명에 스키마 제거 (GPTify.PATIENTS -> PATIENTS)
    sql = re.sub(r'\bGPTify\.', '', sql, flags=re.IGNORECASE)

    # ROWNUM 조건 정리 (성능 최적화용이므로 평가에서 제외)
    sql = re.sub(r'\s+WHERE\s+rownum\s*<=\s*\d+\s*$', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\s+AND\s+rownum\s*<=\s*\d+', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'rownum\s*<=\s*\d+\s+AND\s+', '', sql, flags=re.IGNORECASE)

    return sql


# === 토큰 관리 클래스 ===
class TokenCallback(BaseCallbackHandler):
    """LangChain 토큰 사용량 추적을 위한 콜백 클래스"""

    def __init__(self):
        self.total_tokens = 0
        self.prompt_tokens = 0
        self.completion_tokens = 0

    def on_llm_start(self, serialized, prompts, **kwargs):
        pass

    def on_llm_end(self, response, **kwargs):
        if hasattr(response, 'llm_output') and response.llm_output:
            token_usage = response.llm_output.get('token_usage', {})
            self.total_tokens = token_usage.get('total_tokens', 0)
            self.prompt_tokens = token_usage.get('prompt_tokens', 0)
            self.completion_tokens = token_usage.get('completion_tokens', 0)


# 전역 토큰 콜백 인스턴스
token_callback = TokenCallback()


def estimate_token_usage(text):
    """텍스트의 대략적인 토큰 수 추정"""
    if not text:
        return 0
    # 대략적으로 한국어는 글자당 1.5토큰, 영어는 단어당 1.3토큰으로 추정
    korean_chars = len(re.findall(r'[가-힣]', text))
    english_words = len(re.findall(r'\b[a-zA-Z]+\b', text))
    other_chars = len(text) - korean_chars - sum(len(word) for word in re.findall(r'\b[a-zA-Z]+\b', text))

    estimated_tokens = int(korean_chars * 1.5 + english_words * 1.3 + other_chars * 0.5)
    return max(estimated_tokens, 1)


def record_token_usage(user_question, generated_sql, response_text, estimated_tokens, actual_usage=None,
                       execution_success=False):
    """토큰 사용량을 기록하는 함수"""
    try:
        token_record = {
            "timestamp": datetime.now().isoformat(),
            "user_question": user_question[:100],  # 처음 100자만 저장
            "generated_sql": generated_sql[:200] if generated_sql else "",
            "response_length": len(response_text) if response_text else 0,
            "estimated_tokens": estimated_tokens,
            "actual_tokens": actual_usage.get('total_tokens') if actual_usage else None,
            "execution_success": execution_success
        }

        # 토큰 로그 파일에 저장
        token_log_file = "token_usage_log.json"
        try:
            if os.path.exists(token_log_file):
                with open(token_log_file, 'r', encoding='utf-8') as f:
                    token_logs = json.load(f)
            else:
                token_logs = []

            token_logs.append(token_record)

            # 최근 1000개만 유지
            if len(token_logs) > 1000:
                token_logs = token_logs[-1000:]

            with open(token_log_file, 'w', encoding='utf-8') as f:
                json.dump(token_logs, f, indent=2, ensure_ascii=False)

        except Exception as file_error:
            print(f"토큰 로그 파일 저장 실패: {file_error}")

        return token_record

    except Exception as e:
        print(f"토큰 사용량 기록 실패: {e}")
        return None


def get_token_statistics():
    """토큰 사용 통계 조회"""
    token_log_file = "token_usage_log.json"
    try:
        if not os.path.exists(token_log_file):
            return {"total_calls": 0, "total_estimated_tokens": 0}

        with open(token_log_file, 'r', encoding='utf-8') as f:
            token_logs = json.load(f)

        total_calls = len(token_logs)
        total_estimated = sum(log.get('estimated_tokens', 0) for log in token_logs)
        successful_executions = sum(1 for log in token_logs if log.get('execution_success', False))

        return {
            "total_calls": total_calls,
            "total_estimated_tokens": total_estimated,
            "successful_executions": successful_executions,
            "success_rate": successful_executions / total_calls if total_calls > 0 else 0
        }

    except Exception as e:
        print(f"토큰 통계 조회 실패: {e}")
        return {"error": str(e)}


# === SQL 실행 결과 비교 ===
def compare_execution_results(generated_sql, target_sql, cache):
    """생성된 SQL과 정답 SQL의 실행 결과를 비교"""
    try:
        print(f"🔍 [EXEC_MATCH] 실행 결과 비교 시작")

        # 생성된 SQL 실행
        generated_result = run_sql_query_cached(generated_sql, cache)
        if not generated_result["success"]:
            print(f"❌ 생성 SQL 실행 실패: {generated_result.get('error')}")
            return False

        # 정답 SQL 실행
        target_result = run_sql_query_cached(target_sql, cache)
        if not target_result["success"]:
            print(f"❌ 정답 SQL 실행 실패: {target_result.get('error')}")
            return False

        # 🔍 결과 비교
        generated_rows = generated_result["result"]
        target_rows = target_result["result"]

        if len(generated_rows) != len(target_rows):
            print(f"🔍 [EXEC_MATCH] 행 수 불일치: {len(generated_rows)} vs {len(target_rows)}")
            return False

        # 각 행을 정렬하여 비교
        generated_sorted = sorted([tuple(row.values()) for row in generated_rows])
        target_sorted = sorted([tuple(row.values()) for row in target_rows])

        is_match = generated_sorted == target_sorted

        print(f"🔍 [EXEC_MATCH] 실행 결과 비교: {'✅ 일치' if is_match else '❌ 불일치'}")
        print(f"🔍 [EXEC_MATCH] 생성: {len(generated_rows):,}행 vs 정답: {len(target_rows):,}행")

        return is_match

    except Exception as e:
        print(f"❌ [EXEC_MATCH] 실행 결과 비교 오류: {e}")
        return False


# === SQL 결과 캐싱 클래스 ===
class SQLResultCache:
    """SQL 실행 결과를 캐싱하는 클래스"""

    def __init__(self, max_size: int = 1000, ttl_seconds: int = 3600):
        """
        Args:
            max_size: 최대 캐시 크기 (쿼리 개수)
            ttl_seconds: 캐시 유효 시간 (초, 기본 1시간)
        """
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.hit_count = 0  # 캐시 히트 횟수
        self.miss_count = 0  # 캐시 미스 횟수

    def _generate_cache_key(self, sql: str) -> str:
        """SQL 문자열로부터 캐시 키 생성"""
        # SQL을 정규화 (공백, 대소문자 통일)
        normalized_sql = ' '.join(sql.strip().lower().split())
        # MD5 해시로 짧은 키 생성
        return hashlib.md5(normalized_sql.encode()).hexdigest()

    def _is_expired(self, cache_entry: Dict[str, Any]) -> bool:
        """캐시 항목이 만료되었는지 확인"""
        return time.time() - cache_entry['timestamp'] > self.ttl_seconds

    def _cleanup_expired(self):
        """만료된 캐시 항목들 정리"""
        current_time = time.time()
        expired_keys = [
            key for key, entry in self.cache.items()
            if current_time - entry['timestamp'] > self.ttl_seconds
        ]
        for key in expired_keys:
            del self.cache[key]

    def get(self, sql: str) -> Optional[Dict[str, Any]]:
        """캐시에서 SQL 실행 결과 조회"""
        cache_key = self._generate_cache_key(sql)

        if cache_key in self.cache:
            entry = self.cache[cache_key]
            if not self._is_expired(entry):
                self.hit_count += 1
                return entry['result']
            else:
                # 만료된 항목 제거
                del self.cache[cache_key]

        self.miss_count += 1
        return None

    def put(self, sql: str, result: Dict[str, Any]):
        """SQL 실행 결과를 캐시에 저장"""
        # 캐시 크기 관리
        if len(self.cache) >= self.max_size:
            # 가장 오래된 항목부터 제거
            oldest_key = min(self.cache.keys(),
                             key=lambda k: self.cache[k]['timestamp'])
            del self.cache[oldest_key]

        cache_key = self._generate_cache_key(sql)
        self.cache[cache_key] = {
            'result': result,
            'timestamp': time.time()
        }

    def get_stats(self) -> Dict[str, Any]:
        """캐시 통계 반환"""
        total_requests = self.hit_count + self.miss_count
        hit_rate = self.hit_count / total_requests if total_requests > 0 else 0

        return {
            'cache_size': len(self.cache),
            'max_size': self.max_size,
            'hit_count': self.hit_count,
            'miss_count': self.miss_count,
            'hit_rate': hit_rate,
            'ttl_seconds': self.ttl_seconds
        }

    def clear(self):
        """캐시 전체 삭제"""
        self.cache.clear()
        self.hit_count = 0
        self.miss_count = 0


def run_sql_query_cached(sql, cache):
    """캐시를 사용하여 SQL 쿼리 실행"""
    try:
        # 캐시 확인
        cached_result = cache.get(sql)
        if cached_result is not None:
            return cached_result

        # 캐시에 없으면 실제 실행
        result = run_sql_query_direct(sql)

        # 성공한 결과만 캐시에 저장
        if result.get("success", False):
            cache.put(sql, result)

        return result

    except Exception as e:
        return {
            "success": False,
            "error": f"캐시 SQL 실행 오류: {str(e)}",
            "result": []
        }


def run_sql_query_direct(sql):
    """SQL 쿼리를 직접 실행"""
    try:
        ORACLE_USER = os.getenv("ORACLE_USER", "GPTify")
        ORACLE_PW = os.getenv("ORACLE_PW", "oracle_4U")
        ORACLE_HOST = os.getenv("ORACLE_HOST", "138.2.63.245")
        ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
        ORACLE_SERVICE = os.getenv("ORACLE_SERVICE", "srvinv.sub03250142080.kdtvcn.oraclevcn.com")

        dsn = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE)
        conn = cx_Oracle.connect(user=ORACLE_USER, password=ORACLE_PW, dsn=dsn)
        cursor = conn.cursor()

        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        result = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                row_dict[columns[i]] = value
            result.append(row_dict)

        cursor.close()
        conn.close()

        return {
            "success": True,
            "result": result,
            "row_count": len(result)
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "result": []
        }


# 모듈 레벨에서 전역 캐시 객체 생성 (앱 실행 동안 유지)
sql_result_cache = SQLResultCache(
    max_size=100,  # 최대 1000개 쿼리 결과 캐시
    ttl_seconds=600  # 1시간 동안 유효
)

print("🎯 SQL 결과 캐싱 시스템 초기화 완료!")


def normalize_col_unit_semantic(col_unit1, col_unit2, schema):
    """
    두 컬럼 단위를 의미적으로 비교 (함수 무시)
    (agg_id, col_id, distinct) 형태 처리
    """
    if not col_unit1 or not col_unit2 or len(col_unit1) < 2 or len(col_unit2) < 2:
        return col_unit1 == col_unit2

    agg1, col1, distinct1 = col_unit1[0], col_unit1[1], col_unit1[2] if len(col_unit1) > 2 else False
    agg2, col2, distinct2 = col_unit2[0], col_unit2[1], col_unit2[2] if len(col_unit2) > 2 else False

    # 집계함수와 DISTINCT는 동일해야 함
    if agg1 != agg2 or distinct1 != distinct2:
        return False

    # 컬럼 ID가 완전히 같으면 True
    if col1 == col2:
        return True

    # 스키마에서 실제 컬럼명 추출하여 비교
    col1_name = extract_column_name_from_id(col1)
    col2_name = extract_column_name_from_id(col2)

    return col1_name == col2_name


def extract_column_name_from_id(col_id):
    """
    컬럼 ID에서 실제 컬럼명 추출
    __prescriptions.drug__ → drug
    __all__ → *
    """
    if not isinstance(col_id, str):
        return str(col_id)

    if isinstance(col_id, str):
        col_id = col_id.strip('_')
    else:
        col_id = str(col_id)

    # __all__ 처리
    if col_id == 'all':
        return '*'

    # 테이블.컬럼 형태에서 컬럼명만 추출
    if '.' in col_id:
        parts = col_id.split('.')
        return parts[-1].strip('_')

    return col_id


def normalize_val_unit_semantic(val_unit1, val_unit2, schema):
    """
    두 값 단위를 의미적으로 비교
    (unit_op, col_unit1, col_unit2) 형태 처리
    """
    if not val_unit1 or not val_unit2 or len(val_unit1) < 2 or len(val_unit2) < 2:
        return val_unit1 == val_unit2

    op1, col1, col2_1 = val_unit1[0], val_unit1[1], val_unit1[2] if len(val_unit1) > 2 else None
    op2, col1_2, col2_2 = val_unit2[0], val_unit2[1], val_unit2[2] if len(val_unit2) > 2 else None

    # 연산자는 동일해야 함
    if op1 != op2:
        return False

    # 첫 번째 컬럼 단위 비교
    if not normalize_col_unit_semantic(col1, col1_2, schema):
        return False

    # 두 번째 컬럼 단위 비교 (있는 경우)
    if col2_1 is None and col2_2 is None:
        return True
    elif col2_1 is not None and col2_2 is not None:
        return normalize_col_unit_semantic(col2_1, col2_2, schema)
    else:
        return False  # 하나만 None인 경우


# === 중첩 SQL 관련 함수들 ===
def get_nestedSQL(sql):
    """중첩 SQL(서브쿼리) 추출 함수"""
    nested = []
    for cond_unit in sql['from']['conds'][::2] + sql['where'][::2] + sql['having'][::2]:
        if len(cond_unit) >= 5:
            if type(cond_unit[3]) is dict:
                nested.append(cond_unit[3])
            if type(cond_unit[4]) is dict:
                nested.append(cond_unit[4])

    if sql['intersect'] is not None:
        nested.append(sql['intersect'])
    if sql['except'] is not None:
        nested.append(sql['except'])
    if sql['union'] is not None:
        nested.append(sql['union'])

    return nested


def has_agg(unit):
    """단위에 집계 함수가 있는지 확인하는 함수"""
    AGG_OPS = ('none', 'max', 'min', 'count', 'sum', 'avg')
    return unit[0] != AGG_OPS.index('none')


def count_agg(units):
    """집계 함수 개수 세기 함수"""
    return len([unit for unit in units if has_agg(unit)])


def count_component1(sql):
    """기본 컴포넌트 개수 세기 함수 (SParC 공식)"""
    count = 0
    WHERE_OPS = ('not', 'between', '=', '>', '<', '>=', '<=', '!=', 'in', 'like', 'is', 'exists')

    if len(sql['where']) > 0:
        count += 1
    if len(sql['groupBy']) > 0:
        count += 1
    if len(sql['orderBy']) > 0:
        count += 1
    if sql['limit'] is not None:
        count += 1
    if len(sql['from']['table_units']) > 0:
        count += len(sql['from']['table_units']) - 1

    # OR 개수 추가
    ao = sql['from']['conds'][1::2] + sql['where'][1::2] + sql['having'][1::2]
    count += len([token for token in ao if token == 'or'])

    # LIKE 개수 추가
    cond_units = sql['from']['conds'][::2] + sql['where'][::2] + sql['having'][::2]
    count += len([cond_unit for cond_unit in cond_units
                  if len(cond_unit) > 1 and cond_unit[1] == WHERE_OPS.index('like')])

    return count


def count_component2(sql):
    """고급 컴포넌트 개수 세기 함수 (중첩 SQL 개수)"""
    nested = get_nestedSQL(sql)
    return len(nested)


def count_others(sql):
    """기타 복잡도 요소 개수 세기 함수"""
    count = 0

    # 집계 함수 개수 계산
    agg_count = count_agg(sql['select'][1])
    agg_count += count_agg(sql['where'][::2])
    agg_count += count_agg(sql['groupBy'])

    if len(sql['orderBy']) > 0:
        order_val_units = sql['orderBy'][1] if len(sql['orderBy']) > 1 else []
        for val_unit in order_val_units:
            if val_unit and len(val_unit) > 1:
                if val_unit[1] and has_agg(val_unit[1]):
                    agg_count += 1
                if len(val_unit) > 2 and val_unit[2] and has_agg(val_unit[2]):
                    agg_count += 1

    agg_count += count_agg(sql['having'])

    if agg_count > 1:
        count += 1

    # SELECT 컬럼 개수
    if len(sql['select'][1]) > 1:
        count += 1

    # WHERE 조건 개수
    if len(sql['where']) > 1:
        count += 1

    # GROUP BY 절 개수
    if len(sql['groupBy']) > 1:
        count += 1

    return count


def create_empty_sql_structure():
    """SParC 방식으로 파싱 실패 시 사용할 빈 SQL 구조 생성"""
    return {
        "except": None,
        "from": {
            "conds": [],
            "table_units": []
        },
        "groupBy": [],
        "having": [],
        "intersect": None,
        "limit": None,
        "orderBy": [],
        "select": [
            False,
            []
        ],
        "union": None,
        "where": []
    }


# === 멀티턴 세션 클래스 ===
class MultiTurnSession:
    """멀티턴 대화 세션을 관리하는 클래스"""

    def __init__(self, session_id, max_turns=5):
        self.session_id = session_id
        self.max_turns = max_turns
        self.turns = []
        self.status = "진행중"
        self.created_at = datetime.now().isoformat()
        self.completed_at = None
        self.total_tokens = 0
        self.session_token_history = []
        self.session_start_time = time.time()
        self.session_end_time = None
        self.session_duration = None

    def add_turn(self, turn_data):
        """새로운 턴 추가"""
        turn_data['turn_number'] = len(self.turns) + 1
        turn_data['timestamp'] = datetime.now().isoformat()
        self.turns.append(turn_data)

        # 🔥 턴 제한에 도달했을 때만 완료
        if len(self.turns) >= self.max_turns:
            self.status = "완료"
            self.completed_at = datetime.now().isoformat()

    def get_efficiency(self):
        """효율성 계산 (현재는 의미 없음, 호환성 유지)"""
        return 0.0  # 자동 완료 없으므로 효율성 개념 없음

    def to_dict(self):
        """딕셔너리로 변환 (JSON 저장용)"""
        session_duration = getattr(self, 'session_duration', None)
        if session_duration is None:
            # session_start_time과 session_end_time으로 계산 시도
            start_time = getattr(self, 'session_start_time', None)
            end_time = getattr(self, 'session_end_time', None)
            if start_time and end_time:
                session_duration = end_time - start_time
            else:
                session_duration = 0.0
        print(f"🔍 [TO_DICT] {self.session_id}: session_duration = {session_duration}")

        return {
            'session_id': self.session_id,
            'max_turns': self.max_turns,
            'turns': self.turns,
            'status': self.status,
            'created_at': self.created_at,
            'completed_at': self.completed_at,
            'efficiency': self.get_efficiency(),
            # 🔥 추가 필요
            'total_tokens': getattr(self, 'total_tokens', 0),
            'session_token_history': getattr(self, 'session_token_history', []),
            # 🔥 시간 필드 추가
            'session_start_time': getattr(self, 'session_start_time', None),
            'session_end_time': getattr(self, 'session_end_time', None),
            'session_duration': getattr(self, 'session_duration', 0.0) if hasattr(self, 'session_duration') and getattr(
                self, 'session_duration') is not None else 0.0
        }


class ClauseProgressAnalyzer:
    """🔥 SParC 공식 평가 로직 기반 Clause별 진행 상황 분석기"""

    def __init__(self, evaluator, schema):
        self.evaluator = evaluator
        self.schema = schema

    def analyze_clause_progress(self, generated_sql, target_sql):
        """
        생성된 SQL과 목표 SQL을 비교하여 각 절의 완성도 계산
        🔥 SParC 공식 평가 함수들을 직접 사용 (0 또는 1만 반환)
        """
        try:
            # 🔥 SQL 정규화 후 파싱 (exact match와 동일하게)
            normalized_generated = normalize_oracle_sql_for_comparison(generated_sql)
            normalized_target = normalize_oracle_sql_for_comparison(target_sql)

            generated_parsed = get_sql(self.schema, normalized_generated)
            target_parsed = get_sql(self.schema, normalized_target)

            # 🔥 핵심 변경: Evaluator의 공식 함수들 직접 사용
            from evaluation import eval_select, eval_where, eval_group, eval_having, eval_order, eval_and_or, \
                eval_nested, eval_IUEN, eval_keywords

            # SParC 공식 절별 평가 수행
            select_scores = eval_select(generated_parsed, target_parsed, self.schema)
            where_scores = eval_where(generated_parsed, target_parsed, self.schema)
            group_scores = eval_group(generated_parsed, target_parsed)
            having_scores = eval_having(generated_parsed, target_parsed)
            order_scores = eval_order(generated_parsed, target_parsed, self.schema)
            and_or_scores = eval_and_or(generated_parsed, target_parsed)
            iuen_scores = eval_IUEN(generated_parsed, target_parsed)
            keyword_scores = eval_keywords(generated_parsed, target_parsed)

            # 🔥 SParC 방식: 0 또는 1만 반환 (완전 이진 평가)
            def calculate_binary_score(label_total, pred_total, cnt, cnt_wo_agg=None):
                """SParC 공식 점수 계산 로직"""
                if label_total == 0 and pred_total == 0:
                    return None  # 사용하지 않은 절은 평가 제외
                elif pred_total != label_total:
                    return 0  # 개수 불일치 → 0점
                elif cnt == pred_total:
                    return 1  # 완전 일치 → 1점
                else:
                    return 0  # 부분 일치 → 0점

            # 각 절별 점수 계산
            clause_progress = {}

            # SELECT 절
            if len(select_scores) >= 3:
                clause_progress['select'] = calculate_binary_score(select_scores[0], select_scores[1], select_scores[2])
            if len(select_scores) >= 4:
                clause_progress['select(no AGG)'] = calculate_binary_score(select_scores[0], select_scores[1],
                                                                           select_scores[3])

            # WHERE 절
            if len(where_scores) >= 3:
                clause_progress['where'] = calculate_binary_score(where_scores[0], where_scores[1], where_scores[2])
            if len(where_scores) >= 4:
                clause_progress['where(no OP)'] = calculate_binary_score(where_scores[0], where_scores[1],
                                                                         where_scores[3])

            # GROUP BY 절
            if len(group_scores) >= 3:
                clause_progress['group(no Having)'] = calculate_binary_score(group_scores[0], group_scores[1],
                                                                             group_scores[2])
            if len(having_scores) >= 3:
                clause_progress['group'] = calculate_binary_score(having_scores[0], having_scores[1], having_scores[2])

            # ORDER BY 절
            if len(order_scores) >= 3:
                clause_progress['order'] = calculate_binary_score(order_scores[0], order_scores[1], order_scores[2])

            # AND/OR 절
            if len(and_or_scores) >= 3:
                clause_progress['and/or'] = calculate_binary_score(and_or_scores[0], and_or_scores[1], and_or_scores[2])

            # IUEN (INTERSECT/UNION/EXCEPT/NESTED)
            if len(iuen_scores) >= 3:
                clause_progress['IUEN'] = calculate_binary_score(iuen_scores[0], iuen_scores[1], iuen_scores[2])

            # Keywords
            if len(keyword_scores) >= 3:
                clause_progress['keywords'] = calculate_binary_score(keyword_scores[0], keyword_scores[1],
                                                                     keyword_scores[2])

            return clause_progress

        except Exception as e:
            print(f"❌ [CLAUSE_PROGRESS] 절별 진행도 분석 실패: {e}")
            # 실패 시 모든 절을 0점으로 처리
            return {clause: 0 for clause in STANDARD_CLAUSES}


# === 멀티턴 평가 관리자 클래스 ===
class MultiTurnEvaluationManager:
    """멀티턴 평가를 관리하는 클래스"""

    def __init__(self, sql_evaluator):
        self.sql_evaluator = sql_evaluator
        self.current_session = None
        self.session_file = "multiturn_sessions.json"
        self.clause_analyzer = ClauseProgressAnalyzer(sql_evaluator.evaluator, sql_evaluator.schema)

    def start_new_session(self, max_turns=5):
        """새로운 멀티턴 세션 시작"""
        session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.current_session = MultiTurnSession(session_id, max_turns)
        print(f"🎯 [MULTITURN] 새 세션 시작: {session_id} (최대 {max_turns}턴)")
        return session_id

    def add_turn_to_session(self, user_question, generated_sql, target_sql=None, token_usage=None):
        """현재 세션에 새로운 턴 추가"""
        if not self.current_session:
            print("❌ [MULTITURN] 활성 세션이 없음. 새 세션을 시작합니다.")
            self.start_new_session()

        turn_data = {
            'user_question': user_question,
            'generated_sql': generated_sql,
            'target_sql': target_sql or "",
            'exact_match': False,
            'execution_match': False,
            'clause_progress': {}
        }

        # 기본 평가 수행
        if generated_sql and target_sql:
            try:
                # Exact Match 평가
                normalized_generated = normalize_oracle_sql_for_comparison(generated_sql)
                normalized_target = normalize_oracle_sql_for_comparison(target_sql)

                if normalized_generated == normalized_target:
                    turn_data['exact_match'] = True
                    print("✅ [MULTITURN] Exact Match 성공")
                else:
                    print("❌ [MULTITURN] Exact Match 실패")

                # Execution Match 평가
                exec_match = compare_execution_results(generated_sql, target_sql, sql_result_cache)
                turn_data['execution_match'] = exec_match
                print(f"🔍 [MULTITURN] Execution Match: {'✅ 성공' if exec_match else '❌ 실패'}")

                # 절별 진행도 분석
                clause_progress = self.clause_analyzer.analyze_clause_progress(generated_sql, target_sql)
                turn_data['clause_progress'] = clause_progress
                print(f"🔍 [MULTITURN] 절별 진행도 분석 완료")

            except Exception as e:
                print(f"❌ [MULTITURN] 평가 중 오류: {e}")

        # RAG 평가 결과 추가
        if hasattr(self.sql_evaluator, 'last_rag_evaluation') and self.sql_evaluator.last_rag_evaluation:
            turn_data.update(self.sql_evaluator.last_rag_evaluation)

        # 세션 토큰 누적
        if token_usage and 'total_tokens' in token_usage:
            self.current_session.total_tokens += token_usage['total_tokens']
            self.current_session.session_token_history.append({
                'turn': len(self.current_session.turns) + 1,
                'tokens': token_usage['total_tokens']
            })

        # SQL 파싱 및 평가 수행
        try:
            # 🔥 SParC 방식 파싱 시도 (엄격한 처리)
            if generated_sql and generated_sql.strip():
                try:
                    normalized_generated = normalize_oracle_sql_for_comparison(generated_sql)
                    parsed_generated = get_sql(self.sql_evaluator.schema, normalized_generated)
                    turn_data['parsing_success'] = True
                    print("✅ [PARSING] 생성 SQL 파싱 성공")

                except Exception as parse_error:
                    # 🔥 SParC 방식: 파싱 실패 시 즉시 실패 처리
                    turn_data['parsing_success'] = False
                    turn_data['parsing_error_detail'] = str(parse_error)
                    turn_data['exact_match'] = False
                    turn_data['execution_match'] = False
                    print(f"❌ [PARSING] 생성 SQL 파싱 실패: {parse_error}")
                    print("🔍 [SPARC_MODE] 파싱 실패로 인해 모든 평가 0점 처리")

                    # 파싱 실패 시 빈 SQL 구조 생성 (SParC 방식)
                    parsed_generated = {
                        "except": None,
                        "from": {"conds": [], "table_units": []},
                        "groupBy": [],
                        "having": [],
                        "intersect": None,
                        "limit": None,
                        "orderBy": [],
                        "select": [False, []],
                        "union": None,
                        "where": []
                    }

            # 🔥 정답 SQL도 동일하게 엄격하게 처리
            if target_sql and target_sql.strip():
                try:
                    normalized_target = normalize_oracle_sql_for_comparison(target_sql)
                    parsed_target = get_sql(self.sql_evaluator.schema, normalized_target)
                    print("✅ [PARSING] 정답 SQL 파싱 성공")

                except Exception as target_parse_error:
                    print(f"❌ [PARSING] 정답 SQL 파싱 실패: {target_parse_error}")
                    # 정답 SQL 파싱 실패 시도 빈 구조로 대체
                    parsed_target = create_empty_sql_structure()

        except Exception as overall_parsing_error:
            print(f"❌ [PARSING] 전체 파싱 과정 실패: {overall_parsing_error}")
            turn_data['parsing_success'] = False
            turn_data['parsing_error_detail'] = str(overall_parsing_error)

        # 턴 추가
        self.current_session.add_turn(turn_data)

        # 세션이 완료되면 저장
        if self.current_session.status == "완료":
            self.current_session.session_end_time = time.time()
            self.current_session.session_duration = self.current_session.session_end_time - self.current_session.session_start_time
            self.save_session()
            print(f"🎉 [MULTITURN] 세션 완료 및 저장: {self.current_session.session_id}")

        return len(self.current_session.turns)

    def save_session(self):
        """현재 세션을 파일에 저장"""
        if not self.current_session:
            return

        try:
            # 기존 세션들 로드
            if os.path.exists(self.session_file):
                with open(self.session_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            else:
                data = {"multiturn_sessions": []}

            # 현재 세션 추가
            sessions = data.get("multiturn_sessions", [])

            # 동일한 session_id가 있으면 업데이트, 없으면 추가
            session_dict = self.current_session.to_dict()
            existing_index = None
            for i, session in enumerate(sessions):
                if session.get('session_id') == self.current_session.session_id:
                    existing_index = i
                    break

            if existing_index is not None:
                sessions[existing_index] = session_dict
                print(f"📝 [SAVE] 기존 세션 업데이트: {self.current_session.session_id}")
            else:
                sessions.append(session_dict)
                print(f"📝 [SAVE] 새 세션 추가: {self.current_session.session_id}")

            # 파일에 저장
            data["multiturn_sessions"] = sessions
            with open(self.session_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            print(f"✅ [SAVE] 세션 저장 완료: {len(sessions)}개 세션")

            # 🔥 저장 검증
            try:
                with open(self.session_file, 'r', encoding='utf-8') as f:
                    verify_data = json.load(f)
                verify_sessions = verify_data.get('multiturn_sessions', [])
                print(f"✅ [SAVE_VERIFY] 저장 검증 성공: {len(verify_sessions)}개 세션 확인")
            except Exception as verify_error:
                print(f"❌ [SAVE_VERIFY] 저장 검증 실패: {verify_error}")

        except Exception as e:
            print(f"❌ 세션 저장 실패: {e}")
            import traceback
            traceback.print_exc()


# === [5] SQL 평가 메인 클래스 ===
class SQLEvaluationModule:
    # SQL 평가 + 토큰 추적 통합 메인 클래스

    def __init__(self):
        # SQL 평가 관련 변수
        self.last_rag_evaluation = {}

        self.schema = None
        self.evaluator = None
        self.kmaps = None

        # 파일 경로 설정
        self.gold_file = "gold_queries.json"
        self.generated_file = "generated_queries.json"
        self.evaluation_file = "evaluation_results.json"
        self.token_log_file = "token_usage_test.json"

        # Schema 파일 목록 (설정 가능)
        self.schema_files = [
            "schema_patients.txt",
            "schema_diagproc.txt",
            "schema_drugs.txt",
            "schema_events.txt",
            "schema_trial.txt"
        ]

        # 결과 저장 변수 (멀티턴용으로 수정)
        self.last_aggregate_result = ""

        # 토큰 추적 관련 변수
        self.token_log_file = "token_usage_log.json"
        self.session_tokens = {
            "total_tokens": 0,
            "api_calls": 0,
            "session_start": datetime.now().isoformat()
        }
        self.current_token_info = {}

        self.initialize()

    def initialize(self):
        """평가 모듈 초기화"""
        try:
            # === 스키마 초기화 ===
            print("🔍 스키마 초기화 시작")

            schema_dict = extract_schema_dict_from_txt()

            if schema_dict:
                self.schema = Schema(schema_dict)
                print(f"✅ 스키마 생성 완료: {len(schema_dict)}개 테이블")
            else:
                print("❌ 스키마 딕셔너리가 비어있음")
                self.schema = None

            # === Evaluator 초기화 ===
            try:
                self.evaluator = Evaluator(self.schema)
                print("✅ Evaluator 초기화 완료")
            except Exception as eval_error:
                print(f"❌ Evaluator 초기화 실패: {eval_error}")
                self.evaluator = None

            # === Foreign key map 초기화 ===
            self.kmaps = self.build_foreign_key_map_for_tables()
            self.db = "mimic"

            # === 멀티턴 평가 관리자 초기화 ===
            try:
                self.multiturn_manager = MultiTurnEvaluationManager(self)
                print("✅ 멀티턴 평가 관리자 초기화 완료")
            except Exception as e:
                print(f"❌ 멀티턴 평가 관리자 초기화 실패: {e}")
                self.multiturn_manager = None

        except Exception as e:
            print(f"❌ 평가 모듈 초기화 실패: {e}")

    def build_foreign_key_map_for_tables(self):
        """테이블 간의 외래키 관계를 매핑하는 딕셔너리 생성"""
        # MIMIC-IV 데이터베이스의 주요 외래키 관계
        foreign_key_map = {
            "patients": {
                "subject_id": ["admissions.subject_id", "chartevents.subject_id", "prescriptions.subject_id"]
            },
            "admissions": {
                "subject_id": ["patients.subject_id"],
                "hadm_id": ["chartevents.hadm_id", "prescriptions.hadm_id", "diagnoses_icd.hadm_id"]
            },
            "chartevents": {
                "subject_id": ["patients.subject_id"],
                "hadm_id": ["admissions.hadm_id"],
                "itemid": ["d_items.itemid"]
            },
            "prescriptions": {
                "subject_id": ["patients.subject_id"],
                "hadm_id": ["admissions.hadm_id"]
            },
            "diagnoses_icd": {
                "subject_id": ["patients.subject_id"],
                "hadm_id": ["admissions.hadm_id"],
                "icd_code": ["d_icd_diagnoses.icd_code"]
            },
            "d_icd_diagnoses": {
                "icd_code": ["diagnoses_icd.icd_code"]
            },
            "d_items": {
                "itemid": ["chartevents.itemid"]
            }
        }
        return foreign_key_map

    def evaluate_and_save(self, user_question, generated_sql, gold_sql=None, exec_success=False, result_count=0):
        """통합 평가 함수 - 기본 평가 + 멀티턴 평가 모두 수행"""
        try:
            print(f"🔍 [EVAL] 통합 평가 시작")
            print(f"  └ 질문: {user_question[:50]}...")
            print(f"  └ 생성 SQL: {generated_sql[:50] if generated_sql else 'None'}...")
            print(f"  └ 정답 SQL: {gold_sql[:50] if gold_sql else 'None'}...")

            # === 🔥 문제 1: 멀티턴 평가 먼저 수행 ===
            try:
                if self.multiturn_manager and generated_sql:
                    turn_number = self.multiturn_manager.add_turn_to_session(
                        user_question=user_question,
                        generated_sql=generated_sql,
                        target_sql=gold_sql,
                        token_usage=getattr(self, 'current_token_info', {})
                    )
                    print(f"✅ [EVAL] 멀티턴 평가 완료 (턴 {turn_number})")
                else:
                    print(f"⚠️ [EVAL] 멀티턴 관리자 없음 또는 SQL 없음")
            except Exception as multiturn_error:
                print(f"❌ [EVAL] 멀티턴 평가 실패: {multiturn_error}")

            # === 🔥 문제 2: 기본 평가 수행 ===
            try:
                # 평가 결과 딕셔너리 생성
                eval_result = {
                    "user_question": user_question,
                    "generated_sql": generated_sql,
                    "gold_sql": gold_sql or "",
                    "timestamp": datetime.now().isoformat(),
                    "execution_success": exec_success,
                    "result_count": result_count
                }

                # 🔥 문제 3: SQL이 있을 때만 파싱 및 평가 수행
                if generated_sql and generated_sql.strip():
                    try:
                        # SQL 파싱 시도
                        normalized_sql = normalize_oracle_sql_for_comparison(generated_sql)
                        parsed_sql = get_sql(self.schema, normalized_sql)
                        eval_result['syntax_correct'] = True
                        eval_result['sql_normalized'] = normalized_sql
                        print(f"✅ [EVAL] SQL 파싱 성공")

                        # 🔥 문제 4: Gold SQL과 비교 평가
                        if gold_sql and gold_sql.strip():
                            try:
                                # Exact Match 평가
                                normalized_gold = normalize_oracle_sql_for_comparison(gold_sql)
                                if normalized_sql == normalized_gold:
                                    eval_result['exact_match'] = True
                                    print(f"✅ [EVAL] Exact Match 성공")
                                else:
                                    eval_result['exact_match'] = False
                                    print(f"❌ [EVAL] Exact Match 실패")

                                # 🔥 문제 5: Execution Match 평가
                                try:
                                    exec_match = compare_execution_results(generated_sql, gold_sql, sql_result_cache)
                                    eval_result['execution_match'] = exec_match
                                    print(f"🔍 [EVAL] Execution Match: {'✅ 성공' if exec_match else '❌ 실패'}")
                                except Exception as exec_error:
                                    print(f"❌ [EVAL] Execution Match 평가 실패: {exec_error}")
                                    eval_result['execution_match'] = False

                            except Exception as gold_error:
                                print(f"❌ [EVAL] Gold SQL 처리 실패: {gold_error}")
                                eval_result['exact_match'] = False
                                eval_result['execution_match'] = False

                    except Exception as parsing_error:
                        print(f"❌ [EVAL] SQL 파싱 실패: {parsing_error}")
                        eval_result['syntax_correct'] = False
                        eval_result['parsing_error'] = str(parsing_error)
                else:
                    print(f"⚠️ [EVAL] 생성된 SQL이 없음")
                    eval_result['syntax_correct'] = False

                # 🔥 문제 6: 평가 결과 저장
                try:
                    self.save_evaluation_result(eval_result)
                    print(f"✅ [EVAL] 평가 결과 저장 완료")
                except Exception as save_eval_error:
                    print(f"⚠️ [EVAL] 평가 결과 저장 실패: {save_eval_error}")

                # 🔥 문제 7: 출력 함수들 - 안전하게 호출
                try:
                    # 멀티턴 개별 평가 결과 출력 (콘솔용)
                    if hasattr(self, 'print_individual_evaluation'):
                        self.print_individual_evaluation(eval_result)
                        print(f"✅ [EVAL] 개별 평가 출력 완료")
                except Exception as individual_error:
                    print(f"⚠️ [EVAL] 개별 평가 출력 실패: {individual_error}")

                try:
                    # 전체 평가 통계 출력 (콘솔용)
                    if hasattr(self, 'print_aggregate_evaluation'):
                        self.print_aggregate_evaluation()
                        print(f"✅ [EVAL] 전체 평가 출력 완료")
                except Exception as aggregate_error:
                    print(f"⚠️ [EVAL] 전체 평가 출력 실패: {aggregate_error}")

                print(f"🎉 [EVAL] 전체 평가 과정 완료")
                return eval_result

            except Exception as eval_error:
                print(f"❌ [EVAL] 기본 평가 수행 실패: {eval_error}")
                import traceback
                traceback.print_exc()

                # 🔥 개선: 평가 실패 시에도 기본 결과 반환
                return {
                    "user_question": user_question,
                    "generated_sql": generated_sql,
                    "gold_sql": gold_sql or "",
                    "eval_error": str(eval_error),
                    "timestamp": datetime.now().isoformat(),
                    "syntax_correct": False,
                    "execution_success": exec_success,
                    "result_count": result_count
                }

        except Exception as overall_error:
            print(f"❌ [EVAL] 전체 과정 실패: {overall_error}")
            import traceback
            traceback.print_exc()

            # 최종 안전망 - 최소한의 결과라도 반환
            return {
                "user_question": user_question,
                "generated_sql": generated_sql,
                "overall_error": str(overall_error),
                "timestamp": datetime.now().isoformat()
            }

    def save_evaluation_result(self, eval_result):
        """평가 결과를 파일에 저장"""
        try:
            # 기존 평가 결과들 로드
            if os.path.exists(self.evaluation_file):
                with open(self.evaluation_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    evaluations = data
                else:
                    evaluations = data.get("evaluations", [])
            else:
                evaluations = []

            # 새 평가 결과 추가
            evaluations.append(eval_result)

            # 최근 1000개만 유지
            if len(evaluations) > 1000:
                evaluations = evaluations[-1000:]

            # 파일에 저장
            with open(self.evaluation_file, 'w', encoding='utf-8') as f:
                json.dump({"evaluations": evaluations}, f, indent=2, ensure_ascii=False)

        except Exception as e:
            print(f"❌ 평가 결과 저장 실패: {e}")

    def calculate_aggregate_scores(self):
        # 전체 평가 통계 계산 (기존 코드 유지, 보조 함수로 활용)
        try:
            if not os.path.exists(self.evaluation_file):
                return None

            with open(self.evaluation_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if isinstance(data, list):
                results = data
            elif isinstance(data, dict):
                results = data.get("evaluations", [])
            else:
                print("❌ 평가 파일 형식 오류")
                return None

            if not results:
                return None

            # 기본 통계 초기화
            total_count = len(results)
            exact_match_count = sum(1 for r in results if r.get("exact_match", False))
            exact_match_rate = (exact_match_count / total_count) * 100 if total_count > 0 else 0

            # 통계 정보 반환
            return {
                "total_count": total_count,
                "exact_match_rate": exact_match_rate,
                "results": results
            }

        except Exception as e:
            print(f"❌ 전체 평가 계산 실패: {e}")
            return None


def get_difficulty_from_sql(self, sql_string):
    """SQL 난이도 판정 - ROWNUM 제외하여 순수 SQL 복잡도로 평가"""
    try:
        if not sql_string or sql_string.strip() == '':
            return "Easy"

        # 🔥 핵심 수정: 난이도 분류용으로 ROWNUM 제거
        # 성능용 ROWNUM을 제거하고 순수 SQL 논리로만 난이도 평가
        clean_sql = self._remove_rownum_for_difficulty_analysis(sql_string)

        # 정규화된 SQL로 파싱
        normalized_sql = normalize_oracle_sql_for_comparison(clean_sql)
        parsed_sql = get_sql(self.sql_evaluator.schema, normalized_sql)

        if self.sql_evaluator.evaluator:
            hardness = self.sql_evaluator.evaluator.eval_hardness(parsed_sql)
            return hardness.capitalize()
        else:
            return self._calculate_hardness_direct(parsed_sql)

    except Exception as e:
        print(f"❌ SQL 난이도 판정 오류: {e}")
        return "Easy"


def _remove_rownum_for_difficulty_analysis(self, sql_string):
    """난이도 분석용으로 ROWNUM 관련 조건을 제거"""
    try:
        sql = sql_string.strip()

        # 1. 단순한 WHERE rownum <= N 패턴 제거
        sql = re.sub(r'\s+WHERE\s+rownum\s*<=\s*\d+\s*$', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\s+AND\s+rownum\s*<=\s*\d+', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'rownum\s*<=\s*\d+\s+AND\s+', '', sql, flags=re.IGNORECASE)

        # 2. 서브쿼리 외부의 WHERE rownum <= N 제거
        # SELECT * FROM (...) WHERE rownum <= 100 패턴
        sql = re.sub(r'\)\s+WHERE\s+rownum\s*<=\s*\d+\s*$', ')', sql, flags=re.IGNORECASE)

        # 3. 복잡한 WHERE 절에서 rownum 조건만 제거
        # WHERE condition1 AND rownum <= 100 AND condition2 같은 경우
        sql = re.sub(r'\s+AND\s+rownum\s*<=\s*\d+\s+AND\s+', ' AND ', sql, flags=re.IGNORECASE)

        # 4. WHERE 절이 rownum만 있었던 경우 WHERE 자체 제거
        sql = re.sub(r'\s+WHERE\s*$', '', sql, flags=re.IGNORECASE)

        # 5. 불필요한 공백 정리
        sql = re.sub(r'\s+', ' ', sql).strip()

        return sql

    except Exception as e:
        print(f"❌ ROWNUM 제거 중 오류: {e}")
        return sql_string


def _calculate_hardness_direct(self, parsed_sql):
    """직접 난이도 계산 (Evaluator 없을 때 백업)"""
    try:
        component1_count = count_component1(parsed_sql)
        component2_count = count_component2(parsed_sql)
        others_count = count_others(parsed_sql)

        if component1_count <= 1 and others_count == 0 and component2_count == 0:
            return "Easy"
        elif (others_count <= 2 and component1_count <= 1 and component2_count == 0) or \
                (component1_count <= 2 and others_count < 2 and component2_count == 0):
            return "Medium"
        elif (others_count <= 2 and component1_count <= 2 and component2_count <= 1) or \
                (component1_count <= 3 and others_count <= 2 and component2_count == 0) or \
                (component1_count <= 1 and others_count == 0 and component2_count <= 1):
            return "Hard"
        else:
            return "Extra"

    except Exception as e:
        print(f"❌ 직접 난이도 계산 실패: {e}")
        return "Easy"


# === [6] 멀티턴 집계 평가 관리자 ===
class MultiTurnAggregateEvaluationManager:
    """멀티턴 평가 결과 집계 및 분석 관리자"""

    def __init__(self, sql_evaluator):
        self.sql_evaluator = sql_evaluator
        self.session_file = "multiturn_sessions.json"

    def generate_multiturn_evaluation_report(self):
        """멀티턴 평가 종합 리포트 생성"""
        try:
            if not os.path.exists(self.session_file):
                return "📋 멀티턴 세션 파일이 없습니다."

            with open(self.session_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            sessions = data.get('multiturn_sessions', [])
            if not sessions:
                return "📋 저장된 멀티턴 세션이 없습니다."

            # 현재 활성 세션 정보
            current_session_info = ""
            if (hasattr(self.sql_evaluator, 'multiturn_manager') and
                    self.sql_evaluator.multiturn_manager and
                    self.sql_evaluator.multiturn_manager.current_session):
                current = self.sql_evaluator.multiturn_manager.current_session
                current_session_info = f"""
🎯 **현재 활성 세션**
- 세션 ID: {current.session_id}
- 상태: {current.status}
- 진행 턴: {len(current.turns)}/{current.max_turns}
- 토큰 사용량: {current.total_tokens:,} tokens

"""

            # 집계 분석
            report = f"""{current_session_info}📊 **멀티턴 평가 종합 리포트**

🔢 **기본 통계**
- 총 세션 수: {len(sessions)}
- 완료된 세션: {sum(1 for s in sessions if s.get('status') == '완료')}
- 진행중인 세션: {sum(1 for s in sessions if s.get('status') == '진행중')}

"""

            # 정확도 통계
            exact_match_stats = self._calculate_exact_match_accuracy(sessions)
            execution_stats = self._calculate_execution_accuracy(sessions)

            if isinstance(exact_match_stats, dict) and 'all' in exact_match_stats:
                report += f"""🎯 **정확도 통계**
- 전체 Exact Match: {exact_match_stats['all']:.1%}
- 전체 Execution Match: {execution_stats.get('all', 0):.1%}

"""

            # 턴별 성능
            clause_progress = self._calculate_clause_progress_by_turn(sessions)
            if clause_progress:
                report += "📈 **절별 진행도 (턴별 평균)**\n"
                for turn_num, scores in clause_progress.items():
                    if turn_num != 'final' and scores:
                        valid_scores = [v for v in scores.values() if v is not None]
                        if valid_scores:
                            avg_score = sum(valid_scores) / len(valid_scores)
                            report += f"- Turn {turn_num}: {avg_score:.1%}\n"

            # 최종 성능 (마지막 턴)
            if 'final' in clause_progress:
                final_scores = clause_progress['final']
                valid_final = [v for v in final_scores.values() if v is not None]
                if valid_final:
                    final_avg = sum(valid_final) / len(valid_final)
                    report += f"\n🏆 **최종 성능** (마지막 턴): {final_avg:.1%}\n"

            # 최근 3개 세션 요약
            recent_sessions = sessions[-3:]
            report += f"\n📝 **최근 세션들**\n"
            for session in recent_sessions:
                session_id = session.get('session_id', 'Unknown')
                status = session.get('status', 'Unknown')
                turns_count = len(session.get('turns', []))
                max_turns = session.get('max_turns', 5)
                tokens = session.get('total_tokens', 0)

                report += f"- {session_id}: {status} ({turns_count}/{max_turns} 턴, {tokens:,} tokens)\n"

            return report

        except Exception as e:
            return f"❌ 멀티턴 리포트 생성 실패: {e}"

    def _calculate_clause_progress_by_turn(self, sessions):
        """턴별 절 진행도 계산"""
        try:
            if not sessions:
                return {}

            # 모든 세션의 최대 턴 수 찾기
            session_turn_counts = [len(s.get('turns', [])) for s in sessions if s.get('turns')]
            if not session_turn_counts:
                return {}

            max_turns = max(session_turn_counts)
            result = {}

            # 턴별 계산
            for turn_num in range(1, max_turns + 1):
                clause_data = {clause: [] for clause in STANDARD_CLAUSES}

                for session in sessions:
                    turns = session.get('turns', [])
                    if len(turns) >= turn_num:
                        turn = turns[turn_num - 1]
                        clause_progress = turn.get('clause_progress', {})
                        for clause in STANDARD_CLAUSES:
                            score = clause_progress.get(clause, None)
                            clause_data[clause].append(score)

                # 평균 계산
                result[turn_num] = {}
                for clause in STANDARD_CLAUSES:
                    scores = clause_data[clause]
                    if scores:  # 유효한 점수가 있는 경우
                        result[turn_num][clause] = sum(scores) / len(scores)
                    else:  # 모든 값이 None인 경우
                        result[turn_num][clause] = None

            # Final 계산 (모든 세션의 마지막 턴)
            final_scores = {clause: [] for clause in STANDARD_CLAUSES}
            for session in sessions:
                turns = session.get('turns', [])
                if turns:
                    last_turn = turns[-1]
                    clause_progress = last_turn.get('clause_progress', {})
                    for clause in STANDARD_CLAUSES:
                        score = clause_progress.get(clause, None)
                        if score is not None:
                            final_scores[clause].append(score)

            # Final 평균 계산
            result['final'] = {}
            for clause in STANDARD_CLAUSES:
                scores = final_scores[clause]
                if scores:
                    result['final'][clause] = sum(scores) / len(scores)
                else:
                    result['final'][clause] = None

            return result

        except Exception as e:
            print(f"❌ 절별 진행도 계산 실패: {e}")
            return {}

    def _calculate_execution_accuracy(self, sessions):
        """실행 정확도 계산"""
        stats = {}

        if not sessions:
            return {'all': 0.0}

        # 빈 턴이 있는 세션 필터링
        valid_sessions = [s for s in sessions if s.get('turns')]
        if not valid_sessions:
            return {'all': 0.0}

        session_turn_counts = [len(s.get('turns', [])) for s in valid_sessions]
        max_turns = max(session_turn_counts) if session_turn_counts else 5

        # 턴별 계산
        for turn_num in range(1, max_turns + 1):
            success_count = 0
            total_count = 0

            for session in sessions:
                turns = session.get('turns', [])
                if len(turns) >= turn_num:
                    total_count += 1
                    if turns[turn_num - 1].get('execution_match', False):
                        success_count += 1

            stats[turn_num] = {
                'rate': success_count / total_count if total_count > 0 else 0.0,
                'success': success_count,
                'total': total_count
            }

        # All 계산
        all_success = sum([s['success'] for s in stats.values()])
        all_total = sum([s['total'] for s in stats.values()])
        stats['all'] = all_success / all_total if all_total > 0 else 0.0

        return stats

    def _calculate_exact_match_accuracy(self, sessions):
        """정확 일치 정확도 계산"""
        stats = {}

        if not sessions:
            return {'all': 0.0}

        valid_sessions = [s for s in sessions if s.get('turns')]
        if not valid_sessions:
            return {'all': 0.0}

        session_turn_counts = [len(s.get('turns', [])) for s in valid_sessions]
        max_turns = max(session_turn_counts) if session_turn_counts else 5

        # 턴별 계산
        for turn_num in range(1, max_turns + 1):
            success_count = 0
            total_count = 0

            for session in sessions:
                turns = session.get('turns', [])
                if len(turns) >= turn_num:
                    total_count += 1
                    if turns[turn_num - 1].get('exact_match', False):
                        success_count += 1

            stats[turn_num] = {
                'rate': success_count / total_count if total_count > 0 else 0.0,
                'success': success_count,
                'total': total_count
            }

        # All 계산
        all_success = sum([s['success'] for s in stats.values()])
        all_total = sum([s['total'] for s in stats.values()])
        stats['all'] = all_success / all_total if all_total > 0 else 0.0

        return stats

    def _format_individual_evaluation_report(self, session):
        """개별 세션 평가 리포트 형식화"""
        try:
            session_id = session.get('session_id', 'Unknown')
            status = session.get('status', 'Unknown')
            turns = session.get('turns', [])
            max_turns = session.get('max_turns', 5)
            total_tokens = session.get('total_tokens', 0)

            report = f"""📋 **개별 세션 평가: {session_id}**

🎯 **세션 정보**
- 상태: {status}
- 진행 턴: {len(turns)}/{max_turns}
- 총 토큰 사용량: {total_tokens:,} tokens

"""

            if turns:
                report += "📊 **턴별 결과**\n"
                for i, turn in enumerate(turns, 1):
                    question = turn.get('user_question', '')[:50]
                    exact_match = turn.get('exact_match', False)
                    execution_match = turn.get('execution_match', False)

                    exact_icon = "✅" if exact_match else "❌"
                    exec_icon = "✅" if execution_match else "❌"

                    report += f"Turn {i}: {question}... (Exact: {exact_icon}, Exec: {exec_icon})\n"

            return report

        except Exception as e:
            return f"❌ 개별 평가 리포트 생성 실패: {e}"


# === [7] 전역 인스턴스 생성 ===
sql_evaluator = SQLEvaluationModule()


# === [8] 호환성 함수들 ===
def evaluate_and_save(user_question, generated_sql, gold_sql=None, exec_success=False, result_count=0):
    """전역 평가 함수 (하위 호환성 유지)"""
    return sql_evaluator.evaluate_and_save(
        user_question=user_question,
        generated_sql=generated_sql,
        gold_sql=gold_sql,
        exec_success=exec_success,
        result_count=result_count
    )


def get_query_stats():
    """쿼리 통계 조회 (하위 호환성)"""
    return sql_evaluator.calculate_aggregate_scores()


def start_multiturn_session(max_turns=5):
    """멀티턴 세션 시작 (하위 호환성)"""
    if sql_evaluator.multiturn_manager:
        return sql_evaluator.multiturn_manager.start_new_session(max_turns)
    else:
        print("❌ 멀티턴 관리자가 없습니다.")
        return None


def get_individual_evaluation_result():
    """개별 평가 결과 조회 (하위 호환성)"""
    if (sql_evaluator.multiturn_manager and
            hasattr(sql_evaluator.multiturn_manager, 'current_session') and
            sql_evaluator.multiturn_manager.current_session):

        aggregate_manager = MultiTurnAggregateEvaluationManager(sql_evaluator)
        return aggregate_manager._format_individual_evaluation_report(
            sql_evaluator.multiturn_manager.current_session.to_dict()
        )
    else:
        return "📋 활성화된 멀티턴 세션이 없습니다."


def evaluate_new_rag_metrics(user_question, generated_sql, context_quality=0.8, relevance_score=0.9):
    """RAG 메트릭 평가 (하위 호환성)"""
    rag_evaluation = {
        "context_quality": context_quality,
        "relevance_score": relevance_score,
        "user_question_length": len(user_question),
        "generated_sql_length": len(generated_sql) if generated_sql else 0,
        "evaluation_timestamp": datetime.now().isoformat()
    }

    # 전역 평가자의 RAG 평가 결과에 저장
    sql_evaluator.last_rag_evaluation = rag_evaluation
    return rag_evaluation


def evaluate_langsmith_rag_metrics(context_data, retrieval_quality=0.85):
    """LangSmith RAG 메트릭 평가 (하위 호환성)"""
    langsmith_evaluation = {
        "retrieval_quality": retrieval_quality,
        "context_data_size": len(str(context_data)) if context_data else 0,
        "langsmith_timestamp": datetime.now().isoformat()
    }

    # 기존 RAG 평가와 병합
    if hasattr(sql_evaluator, 'last_rag_evaluation'):
        sql_evaluator.last_rag_evaluation.update(langsmith_evaluation)
    else:
        sql_evaluator.last_rag_evaluation = langsmith_evaluation

    return langsmith_evaluation


def get_latest_aggregate_result():
    """최신 집계 결과 조회 (하위 호환성)"""
    if sql_evaluator.multiturn_manager:
        aggregate_manager = MultiTurnAggregateEvaluationManager(sql_evaluator)
        return aggregate_manager.generate_multiturn_evaluation_report()
    else:
        return getattr(sql_evaluator, 'last_aggregate_result', '전체 평가 결과가 없습니다.')


# 모듈 로딩 완료 메시지
print("🎉 SParC 공식 로직 적용 멀티턴 평가 모듈 v2 로딩 완료!")