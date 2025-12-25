################################
# 가정사항들:
#   1. SQL이 올바르다고 가정
#   2. 테이블명만 별칭(alias)을 가진다
#   3. intersect/union/except는 하나만 존재한다
#
# SQL 구조 정의:
# val: 값 타입 - 숫자(float)/문자열(str)/SQL문(dict)
# col_unit: 컬럼 단위 - (집계함수_id, 컬럼_id, DISTINCT여부(bool))
# val_unit: 값 단위 - (단위_연산자, 컬럼단위1, 컬럼단위2)
# table_unit: 테이블 단위 - (테이블_타입, 컬럼단위/SQL문)
# cond_unit: 조건 단위 - (NOT_연산자, 연산자_id, 값단위, 값1, 값2)
# condition: 조건 - [조건단위1, 'and'/'or', 조건단위2, ...]
# sql 구조: {
#   'select': (DISTINCT여부(bool), [(집계함수_id, 값단위), (집계함수_id, 값단위), ...])
#   'from': {'table_units': [테이블단위1, 테이블단위2, ...], 'conds': 조건}
#   'where': 조건
#   'groupBy': [컬럼단위1, 컬럼단위2, ...]
#   'orderBy': ('asc'/'desc', [값단위1, 값단위2, ...])
#   'having': 조건
#   'limit': None/제한값
#   'intersect': None/SQL문
#   'except': None/SQL문
#   'union': None/SQL문
# }
################################

# === 필수 라이브러리 임포트 ===
import json  # JSON 파일 처리를 위한 모듈
import sqlite3  # SQLite 데이터베이스 연결을 위한 모듈
from nltk import word_tokenize  # NLTK의 단어 토큰화 함수
import re

# === SQL 관련 상수 정의 ===
# SQL 절 키워드들 정의
CLAUSE_KEYWORDS = ('select', 'from', 'where', 'group', 'order', 'limit', 'intersect', 'union', 'except')
# JOIN 관련 키워드들 정의
JOIN_KEYWORDS = ('join', 'on', 'as')

# WHERE절 연산자들 정의 (인덱스로 접근) - Oracle 지원 추가
WHERE_OPS = ('not', 'between', '=', '>', '<', '>=', '<=', '!=', 'in', 'like', 'is', 'exists', 'is not null')
# 단위 연산자들 정의 (수학 연산)
UNIT_OPS = ('none', '-', '+', "*", '/')
# 집계 함수들 정의
AGG_OPS = ('none', 'max', 'min', 'count', 'sum', 'avg')
# 테이블 타입 정의
TABLE_TYPE = {
    'sql': "sql",  # 서브쿼리 타입
    'table_unit': "table_unit",  # 일반 테이블 타입
}

# 조건 연산자들 정의
COND_OPS = ('and', 'or')
# SQL 집합 연산자들 정의
SQL_OPS = ('intersect', 'union', 'except')
# 정렬 순서 정의
ORDER_OPS = ('desc', 'asc')

# === Oracle 전용 구문 지원 ===
# Oracle SQL 함수들 정의
ORACLE_FUNCTIONS = ('lower', 'upper', 'trim', 'substr', 'length', 'nvl', 'coalesce',
                    'to_char', 'to_date', 'to_number', 'round', 'trunc', 'abs', 'ceil', 'floor')
# Oracle FETCH 구문 키워드
FETCH_KEYWORDS = ('fetch', 'first', 'next', 'rows', 'only', 'with', 'ties')


# === 스키마 클래스 ===
class Schema:
    """
    테이블과 컬럼을 고유 식별자로 매핑하는 스키마 클래스
    MIMIC-IV 데이터베이스 구조를 처리하기 위한 핵심 클래스
    """

    def __init__(self, schema):
        # 원본 스키마 정보 저장
        self._schema = schema
        # 스키마를 ID 맵으로 변환
        self._idMap = self._map(self._schema)

    @property
    def schema(self):
        """원본 스키마 딕셔너리 반환"""
        return self._schema

    @property
    def idMap(self):
        """ID 매핑 딕셔너리 반환"""
        return self._idMap

    def _map(self, schema):
        """
        스키마를 ID 맵으로 변환하는 내부 메서드
        테이블명.컬럼명 형태로 고유 식별자 생성
        """
        # ID 맵 초기화 - 전체 컬럼을 나타내는 * 매핑
        idMap = {'*': "__all__"}

        # 🔥 Oracle 특별 키워드 추가
        idMap['rownum'] = "__oracle_rownum__"

        id = 1  # ID 카운터 초기화

        # 테이블.컬럼 형태 ID 생성
        for key, vals in schema.items():  # 각 테이블과 컬럼들에 대해
            for val in vals:  # 각 컬럼에 대해
                # 테이블명.컬럼명 형태로 ID 맵에 추가
                idMap[key.lower() + "." + val.lower()] = "__" + key.lower() + "." + val.lower() + "__"
                id += 1  # ID 증가

        # 테이블명만 있는 ID 생성
        for key in schema:  # 각 테이블에 대해
            # 테이블명만으로도 ID 맵에 추가
            idMap[key.lower()] = "__" + key.lower() + "__"
            id += 1  # ID 증가

        return idMap  # 완성된 ID 맵 반환


# === 스키마 관련 함수들 ===
def get_schema(db):
    """
    데이터베이스의 스키마를 가져오는 함수
    테이블명을 키로, 컬럼명 리스트를 값으로 하는 딕셔너리 반환
    """
    # 스키마를 저장할 딕셔너리
    schema = {}
    # SQLite 데이터베이스 연결
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    # 테이블명들 가져오기
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [str(table[0].lower()) for table in cursor.fetchall()]

    # 각 테이블의 컬럼 정보 수집
    for table in tables:
        cursor.execute("PRAGMA table_info({})".format(table))
        schema[table] = [str(col[1].lower()) for col in cursor.fetchall()]

    return schema


def get_schema_from_json(fpath):
    """JSON 파일에서 스키마를 가져오는 함수"""
    # JSON 파일 로드
    with open(fpath) as f:
        data = json.load(f)

    # 스키마 딕셔너리 생성
    schema = {}
    for entry in data:
        table = str(entry['table'].lower())
        cols = [str(col['column_name'].lower()) for col in entry['col_data']]
        schema[table] = cols

    return schema


# === 토큰화 함수 ===
def tokenize(string):
    """SQL 문자열을 토큰으로 분할하는 함수 (Oracle 호환성 강화)"""

    # === 1. 기본 전처리 ===
    string = str(string).strip()

    while string.endswith(';'):
        string = string[:-1].strip()

    string = string.replace("\'", "\"")  # 단일 따옴표를 이중 따옴표로 변경

    # === 2. 세미콜론 전처리 (토큰화 전에 완전 제거) ===
    # 끝부분 세미콜론들 모두 제거
    while string.endswith(';'):
        string = string[:-1].strip()

    # 중간에 있는 불필요한 세미콜론 처리
    string = re.sub(r';\s*;+', '', string)  # 연속 세미콜론
    string = re.sub(r';\s*$', '', string)  # 끝 세미콜론

    # === 3. Oracle 특수 구문 처리 ===
    # IS NOT NULL 처리 (Oracle 전용)
    string = string.replace(" is not null", " IS_NOT_NULL")
    string = string.replace(" IS NOT NULL", " IS_NOT_NULL")

    # FETCH FIRST 구문 처리 (Oracle 호환)
    fetch_pattern = r'FETCH\s+FIRST\s+(\d+)\s+ROWS?\s+ONLY'
    fetch_matches = re.finditer(fetch_pattern, string, re.IGNORECASE)

    for match in reversed(list(fetch_matches)):
        start, end = match.span()
        number = match.group(1)
        string = string[:start] + f" LIMIT {number}" + string[end:]

    # === 4. 기존 토큰화 로직 ===
    quote_idxs = [idx for idx, char in enumerate(string) if char == '"']
    assert len(quote_idxs) % 2 == 0, "Unexpected quote"

    vals = {}
    for i in range(len(quote_idxs) - 1, -1, -2):
        qidx1 = quote_idxs[i - 1]
        qidx2 = quote_idxs[i]
        val = string[qidx1: qidx2 + 1]
        key = "__val_{}_{}__".format(qidx1, qidx2)
        string = string[:qidx1] + key + string[qidx2 + 1:]
        vals[key] = val

    toks = [word.lower() for word in word_tokenize(string)]

    # 특수 토큰 복원
    for i in range(len(toks)):
        if toks[i] == 'is_not_null':
            toks[i] = 'is not null'
        elif toks[i] in vals:
            toks[i] = vals[toks[i]]

    # 연산자 결합 처리
    eq_idxs = [idx for idx, tok in enumerate(toks) if tok == "="]
    eq_idxs.reverse()
    prefix = ('!', '>', '<')
    for eq_idx in eq_idxs:
        if eq_idx > 0:
            pre_tok = toks[eq_idx - 1]
            if pre_tok in prefix:
                toks = toks[:eq_idx - 1] + [pre_tok + "="] + toks[eq_idx + 1:]

    return toks


def skip_semicolon(toks, start_idx):
    """세미콜론을 건너뛰는 함수 (강화된 버전)"""
    idx = start_idx

    # 모든 세미콜론과 공백을 건너뛰기
    while idx < len(toks):
        if toks[idx] == ";" or toks[idx] == "" or toks[idx].isspace():
            idx += 1
        else:
            break

    return idx


# === 별칭 관련 함수들 ===
def scan_alias(toks):
    """
    'as' 키워드와 암시적 별칭을 모두 스캔하여 별칭 맵을 구축하는 함수
    """
    alias = {}

    # 1. AS 키워드 있는 별칭 처리
    as_idxs = [idx for idx, tok in enumerate(toks) if tok.lower() == 'as']
    for idx in as_idxs:
        if idx > 0 and idx + 1 < len(toks):
            table_name = toks[idx - 1]
            alias_name = toks[idx + 1]
            alias[alias_name] = table_name

    # 2. AS 없는 암시적 별칭 처리 (FROM절에서)
    from_indices = [i for i, tok in enumerate(toks) if tok.lower() == 'from']

    for from_idx in from_indices:
        i = from_idx + 1
        while i < len(toks) - 1:
            current_tok = toks[i].lower()
            next_tok = toks[i + 1].lower()

            # 현재 토큰이 테이블명이고, 다음 토큰이 별칭일 조건
            if (current_tok not in CLAUSE_KEYWORDS and
                    next_tok not in CLAUSE_KEYWORDS and
                    next_tok not in ('as', ',', ')', ';', 'on', 'join') and
                    current_tok not in alias.values()):  # 이미 별칭이 아닌 경우

                # 다음다음 토큰이 절 키워드나 구분자면 별칭으로 간주
                if (i + 2 >= len(toks) or
                        toks[i + 2].lower() in CLAUSE_KEYWORDS or
                        toks[i + 2].lower() in (',', ')', ';', 'on', 'join')):
                    alias[next_tok] = current_tok
                    i += 2  # 테이블명과 별칭 모두 건너뛰기
                    continue

            i += 1

            # WHERE나 다른 절이 나오면 중단
            if current_tok in CLAUSE_KEYWORDS and current_tok != 'from':
                break

    return alias


def get_tables_with_alias(schema, toks):
    """
    SParC 공식 방식으로 스키마와 토큰에서 별칭을 포함한 테이블 맵을 생성하는 함수
    """
    # 별칭 스캔 (SParC 방식)
    tables = scan_alias(toks)

    # 스키마의 모든 테이블 추가
    for key in schema:
        # 별칭과 테이블명 충돌 확인
        assert key not in tables, "Alias {} has the same name in table".format(key)
        tables[key] = key  # 테이블명 -> 테이블명 매핑

    return tables


# === 파싱 함수들 ===
def parse_col(toks, start_idx, tables_with_alias, schema, default_tables=None):
    """컬럼 파싱 함수 (ROWNUM + 연산자 구분 강화)"""

    # 🔥 인덱스 범위 체크 추가
    if start_idx >= len(toks):
        raise Exception("Token index out of range")

    tok = toks[start_idx]

    if tok == "*":
        return start_idx + 1, schema.idMap[tok]

    # 🔥 ROWNUM 특별 처리 (대소문자 무관)
    if tok.upper() == "ROWNUM":
        return start_idx + 1, "__oracle_rownum__"

    # 🔥 연산자는 컬럼이 아님을 명시적으로 체크
    if tok in WHERE_OPS or tok in ['<=', '>=', '!=', '<', '>', '=', 'between', 'like', 'in']:
        raise Exception(f"Operator '{tok}' is not a valid column name")

    # 🔥 숫자 값도 컬럼이 아님
    try:
        float(tok)
        raise Exception(f"Numeric value '{tok}' is not a valid column name")
    except ValueError:
        pass  # 숫자가 아니면 계속 진행

    # === Oracle SQL 함수 처리 개선 ===
    if tok.lower() in ORACLE_FUNCTIONS:
        if start_idx + 1 < len(toks) and toks[start_idx + 1] == '(':
            # 괄호 안의 컬럼명 추출
            paren_count = 0
            current_idx = start_idx + 1
            inner_tokens = []

            while current_idx < len(toks):
                if toks[current_idx] == '(':
                    paren_count += 1
                elif toks[current_idx] == ')':
                    paren_count -= 1
                    if paren_count == 0:
                        break
                elif paren_count == 1 and toks[current_idx] != '(':
                    inner_tokens.append(toks[current_idx])
                current_idx += 1

            # 괄호 안의 컬럼을 재귀적으로 파싱
            if inner_tokens:
                try:
                    _, col_id = parse_col(inner_tokens, 0, tables_with_alias, schema, default_tables)
                    return current_idx + 1, col_id  # 원래 컬럼 ID 반환
                except:
                    pass

            # 실패시 * 처리
            return current_idx + 1, schema.idMap.get('*', 0)

    # 테이블.컬럼 형태 처리
    if '.' in tok:
        alias, col = tok.split('.')
        if alias in tables_with_alias:
            table = tables_with_alias[alias]
            key = table + "." + col
            if key in schema.idMap:
                return start_idx + 1, schema.idMap[key]
            else:
                raise Exception(f"Column {key} not found in schema")
        else:
            raise Exception(f"Table alias {alias} not found")


    # 기본 테이블에서 컬럼 찾기
    if default_tables:
        for alias in default_tables:
            table = tables_with_alias[alias]
            if tok in schema.schema[table]:
                key = table + "." + tok
                return start_idx + 1, schema.idMap[key]

        # 모든 테이블에서 검색 (최후 수단)
        for table_name in schema.schema:
            if tok in schema.schema[table_name]:
                key = table_name + "." + tok
                return start_idx + 1, schema.idMap[key]

    # 🔥 핵심 수정: SParC 방식으로 엄격하게 변경
    # UNKNOWN_COL_ 생성하지 않고 즉시 Exception 발생
    raise Exception(f"Error col: {tok}")


def parse_col_unit(toks, start_idx, tables_with_alias, schema, default_tables=None):
    """
    컬럼 단위를 파싱하는 함수 (집계 함수 포함)
    SParC 공식 방식 그대로 사용
    """
    # 초기화
    idx = start_idx
    len_ = len(toks)
    isBlock = False
    isDistinct = False

    # 괄호 처리
    if toks[idx] == '(':
        isBlock = True
        idx += 1

    # 집계 함수 처리
    if toks[idx] in AGG_OPS:
        agg_id = AGG_OPS.index(toks[idx])
        idx += 1
        assert idx < len_ and toks[idx] == '('
        idx += 1

        # DISTINCT 키워드 확인
        if toks[idx] == "distinct":
            idx += 1
            isDistinct = True

        # 컬럼 파싱
        idx, col_id = parse_col(toks, idx, tables_with_alias, schema, default_tables)
        assert idx < len_ and toks[idx] == ')'
        idx += 1
        return idx, (agg_id, col_id, isDistinct)

    # 일반 컬럼 처리
    if toks[idx] == "distinct":
        idx += 1
        isDistinct = True

    agg_id = AGG_OPS.index("none")
    idx, col_id = parse_col(toks, idx, tables_with_alias, schema, default_tables)

    # 블록 종료 처리
    if isBlock:
        assert toks[idx] == ')'
        idx += 1

    return idx, (agg_id, col_id, isDistinct)


def parse_val_unit(toks, start_idx, tables_with_alias, schema, default_tables=None):
    """
    값 단위를 파싱하는 함수 (연산자와 두 개의 컬럼 단위 포함) - 에러 핸들링 강화
    """
    # 초기화
    idx = start_idx
    len_ = len(toks)
    isBlock = False

    # 🔥 인덱스 범위 체크
    if idx >= len_:
        raise Exception("parse_val_unit: Token index out of range")

    # 괄호 처리
    if toks[idx] == '(':
        isBlock = True
        idx += 1

    # 변수 초기화
    col_unit1 = None
    col_unit2 = None
    unit_op = UNIT_OPS.index('none')

    try:
        # 첫 번째 컬럼 단위 파싱
        idx, col_unit1 = parse_col_unit(toks, idx, tables_with_alias, schema, default_tables)
    except Exception as e:
        # 🔥 컬럼 파싱 실패 시 더 자세한 정보 제공
        current_token = toks[idx] if idx < len_ else "END_OF_TOKENS"
        raise Exception(f"parse_val_unit failed at token '{current_token}' (index {idx}): {str(e)}")

    # 연산자 및 두 번째 컬럼 처리
    if idx < len_ and toks[idx] in UNIT_OPS:
        unit_op = UNIT_OPS.index(toks[idx])
        idx += 1
        idx, col_unit2 = parse_col_unit(toks, idx, tables_with_alias, schema, default_tables)

    # 블록 종료 처리
    if isBlock:
        if idx < len_ and toks[idx] == ')':
            idx += 1
        else:
            print(f"⚠️ 괄호 닫기 누락: expected ')' at index {idx}")

    return idx, (unit_op, col_unit1, col_unit2)


def parse_table_unit(toks, start_idx, tables_with_alias, schema):
    """
    테이블 단위를 파싱하는 함수
    SParC 공식 방식 그대로 사용
    """
    # 테이블명 추출
    idx = start_idx
    len_ = len(toks)
    key = tables_with_alias[toks[idx]]

    # AS 키워드 처리 (SParC 공식 방식)
    if idx + 1 < len_ and toks[idx + 1].lower() == "as":
        idx += 3  # 테이블명, as, 별칭 건너뛰기
    else:
        idx += 1  # 테이블명만 건너뛰기

    return idx, schema.idMap[key], key


def parse_value(toks, start_idx, tables_with_alias, schema, default_tables=None):
    """
    값을 파싱하는 함수 (문자열, 숫자, 서브쿼리, 컬럼)
    Oracle AS 키워드 처리 개선
    """
    # 초기화
    idx = start_idx
    len_ = len(toks)

    # 괄호 처리
    isBlock = False
    if toks[idx] == '(':
        isBlock = True
        idx += 1

    # 값 타입별 처리
    if toks[idx] == 'select':  # SELECT로 시작하면 서브쿼리
        idx, val = parse_sql(toks, idx, tables_with_alias, schema)
    elif "\"" in toks[idx]:  # 토큰이 문자열 값이면
        val = toks[idx]
        idx += 1
    else:
        # 숫자 변환 시도
        try:
            val = float(toks[idx])
            idx += 1
        except:
            # 숫자가 아니면 컬럼으로 처리
            end_idx = idx

            # === 핵심 수정: AS 키워드에서 중지하도록 개선 ===
            while end_idx < len_:
                current_token = toks[end_idx].lower()

                # AS 키워드를 만나면 즉시 중지
                if current_token == 'as':
                    break

                # 기타 중지 조건들
                if (current_token in (',', ')', ';') or
                        current_token in CLAUSE_KEYWORDS or
                        current_token in JOIN_KEYWORDS or
                        current_token in ('and', 'or')):
                    break

                end_idx += 1

            if end_idx > start_idx:
                # 해당 범위의 토큰들을 컬럼 단위로 파싱
                temp_idx, val = parse_col_unit(toks[start_idx:end_idx], 0, tables_with_alias, schema, default_tables)
                idx = end_idx
            else:
                raise Exception(f"Empty column range at index {start_idx}")

    # 블록 종료 처리
    if isBlock:
        if idx < len_ and toks[idx] == ')':
            idx += 1

    return idx, val


# process_sql.py의 parse_condition() 함수 수정
def parse_condition(toks, start_idx, tables_with_alias, schema, default_tables=None):
    """
    조건을 파싱하는 함수 (ROWNUM + Oracle 연산자 처리 개선 + 에러 핸들링 강화)
    """
    idx = start_idx
    len_ = len(toks)
    conds = []

    while idx < len_:
        try:
            # 🔥 값 단위 파싱 (ROWNUM 포함한 모든 컬럼/값 처리)
            idx, val_unit = parse_val_unit(toks, idx, tables_with_alias, schema, default_tables)
        except Exception as e:
            # 🔥 연산자를 컬럼으로 잘못 인식한 경우 처리
            error_msg = str(e)
            if "Operator" in error_msg and "is not a valid column name" in error_msg:
                print(f"⚠️ 연산자 파싱 오류 감지: {error_msg}")
                print(f"⚠️ 현재 토큰: {toks[idx] if idx < len(toks) else 'END'}")
                break
            else:
                # 다른 에러는 그대로 전파
                raise e

        not_op = False

        # NOT 연산자 처리
        if idx < len_ and toks[idx] == 'not':
            not_op = True
            idx += 1

        # 🔥 조건 연산자 처리 (Oracle 연산자 추가)
        if idx < len_ and toks[idx] == 'is not null':
            op_id = WHERE_OPS.index('is not null')
            idx += 1
            val1 = val2 = None
        else:
            # 일반 WHERE 연산자 처리
            if idx >= len_:
                break

            current_op = toks[idx]
            if current_op not in WHERE_OPS:
                print(f"⚠️ 지원하지 않는 연산자: {current_op}")
                print(f"⚠️ 지원되는 연산자: {WHERE_OPS}")
                # 🔥 지원하지 않는 연산자면 파싱 중단
                break

            op_id = WHERE_OPS.index(current_op)
            idx += 1
            val1 = val2 = None

            # 연산자별 값 처리
            if op_id == WHERE_OPS.index('between'):
                # BETWEEN 연산자: 두 개의 값 필요
                idx, val1 = parse_value(toks, idx, tables_with_alias, schema, default_tables)
                if idx < len_ and toks[idx] == 'and':
                    idx += 1
                    idx, val2 = parse_value(toks, idx, tables_with_alias, schema, default_tables)
            else:
                # 기타 연산자: 하나의 값만 필요
                idx, val1 = parse_value(toks, idx, tables_with_alias, schema, default_tables)
                val2 = None

        # 조건 단위 추가
        conds.append((not_op, op_id, val_unit, val1, val2))

        # 종료 조건 확인
        if idx < len_ and (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";") or toks[idx] in JOIN_KEYWORDS):
            break

        # AND/OR 연산자 처리
        if idx < len_ and toks[idx] in COND_OPS:
            conds.append(toks[idx])
            idx += 1

    return idx, conds


# === SQL 절 파서들 ===
def parse_select(toks, start_idx, tables_with_alias, schema, default_tables=None):
    """
    SELECT절을 파싱하는 함수
    Oracle alias 지원을 위해 최소한 수정
    """
    idx = start_idx
    len_ = len(toks)

    assert toks[idx] == 'select', "'select' not found"
    idx += 1
    isDistinct = False
    if idx < len_ and toks[idx] == 'distinct':
        idx += 1
        isDistinct = True
    val_units = []
    # === SELECT alias 저장을 위한 딕셔너리 추가 ===
    select_alias_map = {}

    # AS 키워드 건너뛰기 처리 추가
    while idx < len_ and toks[idx] not in CLAUSE_KEYWORDS:
        agg_id = AGG_OPS.index("none")
        if toks[idx] in AGG_OPS:
            agg_id = AGG_OPS.index(toks[idx])
            idx += 1
        idx, val_unit = parse_val_unit(toks, idx, tables_with_alias, schema, default_tables)
        val_units.append((agg_id, val_unit))

        # === 별칭 처리 (AS 있든 없든) ===
        if (idx < len_ and
                toks[idx] not in (',') and
                toks[idx].lower() not in CLAUSE_KEYWORDS):

            if toks[idx].lower() == 'as':
                # AS 키워드 있는 경우
                alias_name = toks[idx + 1].lower()
                select_alias_map[alias_name] = val_unit
                idx += 2
            else:
                # AS 키워드 없는 경우
                alias_name = toks[idx].lower()
                select_alias_map[alias_name] = val_unit
                idx += 1

        if idx < len_ and toks[idx] == ',':
            idx += 1

    return idx, (isDistinct, val_units), select_alias_map


def parse_from(toks, start_idx, tables_with_alias, schema):
    """
    FROM절을 파싱하는 함수
    SParC 공식 방식 그대로 사용
    """
    assert 'from' in toks[start_idx:], "'from' not found"

    len_ = len(toks)
    idx = toks.index('from', start_idx) + 1
    default_tables = []
    table_units = []
    conds = []

    while idx < len_:
        # 괄호 처리
        isBlock = False
        if toks[idx] == '(':
            isBlock = True
            idx += 1

        # 서브쿼리 또는 테이블 처리
        if toks[idx] == 'select':
            idx, sql = parse_sql(toks, idx, tables_with_alias, schema)
            table_units.append((TABLE_TYPE['sql'], sql))
        else:
            # JOIN 키워드 건너뛰기
            if idx < len_ and toks[idx] == 'join':
                idx += 1

            # 테이블 단위 파싱
            idx, table_unit, table_name = parse_table_unit(toks, idx, tables_with_alias, schema)
            table_units.append((TABLE_TYPE['table_unit'], table_unit))
            default_tables.append(table_name)

        # JOIN 조건 처리
        if idx < len_ and toks[idx] == "on":
            idx += 1
            idx, this_conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables)
            if len(conds) > 0:
                conds.append('and')
            conds.extend(this_conds)

        # 블록 종료 처리
        if isBlock:
            assert toks[idx] == ')'
            idx += 1

        # 종료 조건 확인
        if idx < len_ and (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
            break

    return idx, table_units, conds, default_tables


def parse_where(toks, start_idx, tables_with_alias, schema, default_tables):
    """WHERE절을 파싱하는 함수"""
    idx = start_idx
    len_ = len(toks)

    if idx >= len_ or toks[idx] != 'where':
        return idx, []

    idx += 1
    idx, conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables)
    return idx, conds


def parse_group_by(toks, start_idx, tables_with_alias, schema, default_tables):
    """GROUP BY절을 파싱하는 함수"""
    idx = start_idx
    len_ = len(toks)
    col_units = []

    if idx >= len_ or toks[idx] != 'group':
        return idx, col_units

    idx += 1
    assert toks[idx] == 'by'
    idx += 1

    # 절 키워드나 종료 문자가 나올 때까지 컬럼들 파싱
    while idx < len_ and not (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
        idx, col_unit = parse_col_unit(toks, idx, tables_with_alias, schema, default_tables)
        col_units.append(col_unit)
        if idx < len_ and toks[idx] == ',':
            idx += 1
        else:
            break

    return idx, col_units


def parse_order_by(toks, start_idx, tables_with_alias, schema, default_tables, select_alias_map=None):
    """
    ORDER BY절을 파싱하는 함수
    SELECT alias 지원 추가하되 SParC 공식 구조 유지
    """
    idx = start_idx
    len_ = len(toks)
    val_units = []
    order_type = 'asc'  # 기본 정렬 타입은 오름차순

    if idx >= len_ or toks[idx] != 'order':
        return idx, (order_type, val_units)

    idx += 1
    assert toks[idx] == 'by'
    idx += 1

    # 절 키워드나 종료 문자가 나올 때까지 컬럼들 파싱
    while idx < len_ and not (toks[idx] in CLAUSE_KEYWORDS or toks[idx] in (")", ";")):
        # === SELECT alias 처리 추가 ===
        if select_alias_map and toks[idx].lower() in select_alias_map:
            # alias를 val_unit으로 치환
            alias_name = toks[idx].lower()
            val_unit = select_alias_map[alias_name]
            idx += 1  # alias 토큰 건너뛰기
        else:
            # 일반적인 val_unit 파싱
            idx, val_unit = parse_val_unit(toks, idx, tables_with_alias, schema, default_tables)

        val_units.append(val_unit)

        # 정렬 순서 키워드 처리 (ASC/DESC)
        if idx < len_ and toks[idx] in ORDER_OPS:
            order_type = toks[idx]  # 정렬 타입 업데이트
            idx += 1

        if idx < len_ and toks[idx] == ',':
            idx += 1
        else:
            break

    return idx, (order_type, val_units)


def parse_having(toks, start_idx, tables_with_alias, schema, default_tables):
    """HAVING절을 파싱하는 함수"""
    idx = start_idx
    len_ = len(toks)

    if idx >= len_ or toks[idx] != 'having':
        return idx, []

    idx += 1
    idx, conds = parse_condition(toks, idx, tables_with_alias, schema, default_tables)
    return idx, conds


def parse_limit(toks, start_idx):
    """
    LIMIT절을 파싱하는 함수
    Oracle FETCH FIRST 구문도 지원
    """
    idx = start_idx
    len_ = len(toks)

    if idx < len_ and toks[idx] == 'limit':
        idx += 1
        if idx < len_:
            try:
                # LIMIT 값이 숫자인지 확인
                limit_val = int(toks[idx])
                idx += 1
                return idx, limit_val
            except (ValueError, IndexError):
                # 숫자가 아니면 기본값 1 사용
                return idx, 1

    return idx, None


def parse_sql(toks, start_idx, tables_with_alias, schema):
    """
    SQL을 파싱하는 메인 함수
    SELECT alias 지원 추가하되 SParC 공식 구조 유지
    """
    # SQL이 괄호로 묶여있는지 여부 (서브쿼리 표시)
    isBlock = False
    len_ = len(toks)
    idx = start_idx

    # SQL 구조를 저장할 딕셔너리
    sql = {}

    # 괄호 처리
    if toks[idx] == '(':
        isBlock = True
        idx += 1

    # FROM절 우선 파싱 (기본 테이블 정보 필요)
    from_end_idx, table_units, conds, default_tables = parse_from(toks, start_idx, tables_with_alias, schema)
    sql['from'] = {'table_units': table_units, 'conds': conds}

    # SELECT절 파싱 (alias_map도 받기)
    _, select_col_units, select_alias_map = parse_select(toks, idx, tables_with_alias, schema, default_tables)
    idx = from_end_idx
    sql['select'] = select_col_units

    # WHERE절 파싱
    idx, where_conds = parse_where(toks, idx, tables_with_alias, schema, default_tables)
    sql['where'] = where_conds

    # GROUP BY절 파싱
    idx, group_col_units = parse_group_by(toks, idx, tables_with_alias, schema, default_tables)
    sql['groupBy'] = group_col_units

    # HAVING절 파싱
    idx, having_conds = parse_having(toks, idx, tables_with_alias, schema, default_tables)
    sql['having'] = having_conds

    # ORDER BY절 파싱 (SELECT alias 지원)
    idx, order_col_units = parse_order_by(toks, idx, tables_with_alias, schema, default_tables, select_alias_map)
    sql['orderBy'] = order_col_units

    # LIMIT절 파싱
    idx, limit_val = parse_limit(toks, idx)
    sql['limit'] = limit_val

    # 세미콜론 및 괄호 처리
    idx = skip_semicolon(toks, idx)
    if isBlock:
        assert toks[idx] == ')'
        idx += 1
    idx = skip_semicolon(toks, idx)

    # 집합 연산 (INTERSECT/UNION/EXCEPT) 파싱
    for op in SQL_OPS:
        sql[op] = None

    if idx < len_ and toks[idx] in SQL_OPS:
        sql_op = toks[idx]
        idx += 1
        idx, IUE_sql = parse_sql(toks, idx, tables_with_alias, schema)
        sql[sql_op] = IUE_sql

    return idx, sql


# === 유틸리티 함수들 ===
def load_data(fpath):
    """JSON 파일에서 데이터를 로드하는 함수"""
    with open(fpath) as f:
        data = json.load(f)
    return data


def get_sql(schema, query):
    """
    SQL 쿼리 문자열을 파싱하여 구조화된 형태로 반환하는 함수
    외부에서 호출하는 메인 인터페이스
    """
    # SQL 토큰화
    toks = tokenize(query)

    # 테이블-별칭 매핑 생성
    tables_with_alias = get_tables_with_alias(schema.schema, toks)

    # SQL 파싱 실행
    _, sql = parse_sql(toks, 0, tables_with_alias, schema)

    return sql


def skip_semicolon(toks, start_idx):
    """
    세미콜론을 건너뛰는 함수
    SQL 끝에 있는 세미콜론들을 모두 건너뛰기
    """
    idx = start_idx
    while idx < len(toks) and toks[idx] == ";":
        idx += 1
    return idx