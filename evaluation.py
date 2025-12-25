# === SQL 평가 모듈 ===
import os
import argparse
import json
import cx_Oracle
from process_sql import Schema, get_sql


# === 전역 변수 및 설정 ===
LEVELS = ['easy', 'medium', 'hard', 'extra', 'all']
PARTIAL_TYPES = ['select', 'select(no AGG)', 'where', 'where(no OP)', 'group(no Having)',
                 'group', 'order', 'and/or', 'IUEN', 'keywords']
FORMATTING_FUNCTIONS = ('lower', 'upper', 'trim', 'ltrim', 'rtrim') # 의미 없는 포맷팅 함수들 정의

# SQL 키워드들
CLAUSE_KEYWORDS = ('select', 'from', 'where', 'group', 'order', 'limit', 'intersect', 'union', 'except')
JOIN_KEYWORDS = ('join', 'on', 'as')

# 연산자 및 함수 정의
WHERE_OPS = ('not', 'between', '=', '>', '<', '>=', '<=', '!=', 'in', 'like', 'is', 'exists')
UNIT_OPS = ('none', '-', '+', "*", '/')
AGG_OPS = ('none', 'max', 'min', 'count', 'sum', 'avg')

# 테이블 타입 정의
TABLE_TYPE = {
    'sql': "sql",
    'table_unit': "table_unit",
}

# 논리 연산자 정의
COND_OPS = ('and', 'or')
SQL_OPS = ('intersect', 'union', 'except')
ORDER_OPS = ('desc', 'asc')

# SQL 복잡도 분류 기준
HARDNESS = {
    "component1": ('where', 'group', 'order', 'join', 'or', 'like'),
    "component2": ('except', 'union', 'intersect')
}

# === 함수 정규화 기능 추가 ===
def normalize_column_id(col_id, schema):
    """
    함수 래핑된 컬럼을 기본 컬럼으로 정규화
    LOWER(drug) → drug 같은 처리
    """
    # col_id가 문자열이고 스키마에 있는 경우
    if isinstance(col_id, str) and col_id in schema.idMap:
        col_str = col_id.lower()
        # 함수 패턴 찾기: __tablename.lower(columnname)__
        for func in FORMATTING_FUNCTIONS:
            if func in col_str:
                # 함수를 제거한 기본 컬럼 ID 생성
                base_col = col_str.replace(func + '(', '').replace(')', '')
                if base_col in schema.idMap:
                    return schema.idMap[base_col]
    return col_id

def normalize_col_unit(col_unit, schema):
    """컬럼 단위 정규화 (집계함수, 컬럼ID, DISTINCT)"""
    if len(col_unit) >= 2:
        agg_id, col_id, distinct = col_unit[0], col_unit[1], col_unit[2] if len(col_unit) > 2 else False
        normalized_col_id = normalize_column_id(col_id, schema)
        return (agg_id, normalized_col_id, distinct)
    return col_unit

def normalize_val_unit(val_unit, schema):
    """값 단위 정규화 (단위연산자, 컬럼단위1, 컬럼단위2)"""
    if len(val_unit) >= 2:
        unit_op, col_unit1, col_unit2 = val_unit[0], val_unit[1], val_unit[2] if len(val_unit) > 2 else None
        normalized_col_unit1 = normalize_col_unit(col_unit1, schema) if col_unit1 else None
        normalized_col_unit2 = normalize_col_unit(col_unit2, schema) if col_unit2 else None
        return (unit_op, normalized_col_unit1, normalized_col_unit2)
    return val_unit


# === 수정된 함수 정규화 로직 ===

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

    col_id = col_id.strip('_')

    # __all__ 처리
    if col_id == 'all':
        return '*'

    # 테이블.컬럼 형태에서 컬럼명만 추출
    if '.' in col_id:
        parts = col_id.split('.')
        return parts[-1]  # 마지막 부분이 컬럼명

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

# === 조건 분석 함수들 ===
def condition_has_or(conds):
    # 조건 리스트에 OR 연산자가 포함되어 있는지 확인
    return 'or' in conds[1::2]


def condition_has_like(conds):
    # 조건 리스트에 LIKE 연산자가 포함되어 있는지 확인
    for cond in conds[::2]:
        if cond[1] == 9:
            return True
    return False


def condition_has_sql(conds):
    # 조건에 서브쿼리가 포함되어 있는지 확인
    for cond in conds[::2]:
        for val in cond[3:5]:
            if val is not None and type(val) is dict:
                return True
    return False


def val_has_op(val_unit):
    # 값 단위에 연산자가 있는지 확인
    return val_unit[0] != UNIT_OPS.index('none')


def has_agg(unit):
    # 집계 함수가 있는지 확인
    return unit[0] != AGG_OPS.index('none')


def accuracy(count, total):
    # 정확도 계산
    if count == total:
        return 1
    return 0


def recall(count, total):
    # 재현율 계산
    if count == total:
        return 1
    return 0


def F1(acc, rec):
    # F1 점수 계산
    if (acc + rec) == 0:
        return 0
    return (2. * acc * rec) / (acc + rec)


# === 스키마 추출 함수 ===
def extract_schema_dict_from_txt(schema_files):
    # 텍스트 파일들에서 스키마 정보 추출
    schema_dict = {}
    for schema_file in schema_files:
        if not os.path.exists(schema_file):
            continue

        with open(schema_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        current_table = None
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            if line.startswith('table:'):
                current_table = line.split(':', 1)[1].strip().lower()
                schema_dict[current_table] = []
            elif current_table and line:
                columns = [col.strip().lower() for col in line.split(',')]
                schema_dict[current_table].extend(columns)

    return schema_dict


def build_simple_foreign_key_map_from_files(schema_files):
    # 간단한 외래키 맵 생성 (다중 파일 지원)
    return {"mimic_iv": {}}


# === 기본 스키마 함수 ===
def get_default_mimic_schema():
    # 하드코딩된 MIMIC-IV 스키마 반환
    default_schema = {
        'patients': ['subject_id', 'gender', 'anchor_age', 'anchor_year', 'anchor_year_group', 'dod'],
        'admissions': ['subject_id', 'hadm_id', 'admittime', 'dischtime', 'deathtime', 'admission_type',
                       'admission_location', 'discharge_location', 'insurance', 'language', 'marital_status',
                       'ethnicity', 'edregtime', 'edouttime', 'hospital_expire_flag'],
        'chartevents': ['subject_id', 'hadm_id', 'stay_id', 'charttime', 'storetime', 'itemid', 'value',
                        'valuenum', 'valueuom', 'warning'],
        'labevents': ['labevent_id', 'subject_id', 'hadm_id', 'specimen_id', 'itemid', 'charttime',
                      'storetime', 'value', 'valuenum', 'valueuom', 'ref_range_lower', 'ref_range_upper',
                      'flag', 'priority', 'comments'],
        'icustays': ['subject_id', 'hadm_id', 'stay_id', 'first_careunit', 'last_careunit', 'intime',
                     'outtime', 'los'],
        'diagnoses_icd': ['subject_id', 'hadm_id', 'seq_num', 'icd_code', 'icd_version'],
        'procedures_icd': ['subject_id', 'hadm_id', 'seq_num', 'chartdate', 'icd_code', 'icd_version'],
        'prescriptions': ['subject_id', 'hadm_id', 'pharmacy_id', 'starttime', 'stoptime', 'drug_type',
                          'drug', 'formulary_drug_cd', 'gsn', 'ndc', 'prod_strength', 'dose_val_rx',
                          'dose_unit_rx', 'form_val_disp', 'form_unit_disp', 'route'],
        'd_items': ['itemid', 'label', 'abbreviation', 'linksto', 'category', 'unitname', 'param_type',
                    'lownormalvalue', 'highnormalvalue'],
        'd_labitems': ['itemid', 'label', 'fluid', 'category'],
        'd_icd_diagnoses': ['icd_code', 'icd_version', 'long_title'],
        'd_icd_procedures': ['icd_code', 'icd_version', 'long_title'],
        'transfers': ['subject_id', 'hadm_id', 'transfer_id', 'eventtype', 'careunit', 'intime', 'outtime'],
        'microbiologyevents': ['microevent_id', 'subject_id', 'hadm_id', 'micro_specimen_id', 'chartdate',
                               'charttime', 'spec_itemid', 'spec_type_desc', 'test_seq', 'storetime',
                               'test_itemid', 'test_name', 'org_itemid', 'org_name', 'isolate_num',
                               'quantity', 'ab_itemid', 'ab_name', 'dilution_text', 'dilution_comparison',
                               'dilution_value', 'interpretation', 'comments']
    }
    return Schema(default_schema)


def get_oracle_schema_info(db_name):
    # Oracle 데이터베이스에서 스키마 정보를 동적으로 가져오는 함수
    try:
        conn = get_oracle_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT table_name, column_name, data_type 
            FROM user_tab_columns 
            WHERE table_name IN (
                SELECT table_name FROM user_tables
                UNION
                SELECT view_name FROM user_views
            )
            ORDER BY table_name, column_id
        """)

        schema_data = cursor.fetchall()

        if not schema_data:
            print("Warning: No schema data found, using hardcoded MIMIC-IV schema")
            return get_default_mimic_schema()

        schema_dict = {}
        for table_name, column_name, data_type in schema_data:
            table_name = table_name.lower()
            column_name = column_name.lower()
            if table_name not in schema_dict:
                schema_dict[table_name] = []
            schema_dict[table_name].append(column_name)

        cursor.close()
        conn.close()

        return Schema(schema_dict)

    except Exception as e:
        print(f"Schema query error: {e}")
        print("Using hardcoded MIMIC-IV schema")
        return get_default_mimic_schema()


def get_oracle_connection():
    # Oracle 데이터베이스 연결 생성
    try:
        ORACLE_USER = os.getenv("ORACLE_USER", "SYSTEM")
        ORACLE_PW = os.getenv("ORACLE_PW", "oracle_4U")
        ORACLE_HOST = os.getenv("ORACLE_HOST", "localhost")
        ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
        ORACLE_SERVICE = os.getenv("ORACLE_SERVICE", "xe")

        dsn = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, sid=ORACLE_SERVICE)  # service_name을 sid로 변경!
        conn = cx_Oracle.connect(user=ORACLE_USER, password=ORACLE_PW, dsn=dsn)
        return conn
    except Exception as e:
        print(f"Oracle connection error: {e}")
        return None


# === 점수 계산 함수 ===
def get_scores(count, pred_total, label_total):
    """SParC 공식과 동일한 점수 계산 (완전 이진 평가)"""
    # print(f"🔍 [GET_SCORES] count={count}, pred_total={pred_total}, label_total={label_total}")

    # 🔥 핵심 추가: 둘 다 사용하지 않은 절은 평가에서 제외
    if pred_total == 0 and label_total == 0:
        # print(f"🔍 [GET_SCORES] 둘 다 사용하지 않음 → 평가 제외")
        return None, None, None

    if pred_total != label_total:
        # print(f"🔍 [GET_SCORES] total 불일치 → 0점")
        return 0, 0, 0
    elif count == pred_total:
        # print(f"🔍 [GET_SCORES] 완전일치 → 1점")
        return 1, 1, 1
    # print(f"🔍 [GET_SCORES] 부분일치 → 0점")
    return 0, 0, 0


# === SELECT절 평가 ===
def eval_select(pred, label, schema=None):
    """SELECT절 평가 (의미적 비교)"""
    pred_sel = pred['select'][1]
    label_sel = label['select'][1]
    pred_total = len(pred_sel)
    label_total = len(label_sel)
    cnt = 0
    cnt_wo_agg = 0

    if schema:
        # === 완전 일치 검사 (집계함수 포함) ===
        label_sel_copy = label_sel[:]

        for pred_unit in pred_sel:
            # 완전 일치 검사 (의미적 비교)
            for i, label_unit in enumerate(label_sel_copy):
                pred_agg, pred_val = pred_unit
                label_agg, label_val = label_unit
                if (pred_agg == label_agg and
                        normalize_val_unit_semantic(pred_val, label_val, schema)):
                    cnt += 1
                    label_sel_copy.pop(i)
                    break

        # === 집계함수 제외 검사 (별도 실행) ===
        label_wo_agg_copy = [val_unit for agg_id, val_unit in label_sel]

        for pred_unit in pred_sel:
            pred_val = pred_unit[1]

            for i, label_val in enumerate(label_wo_agg_copy):
                semantic_match = normalize_val_unit_semantic(pred_val, label_val, schema)

                if semantic_match:
                    cnt_wo_agg += 1
                    label_wo_agg_copy.pop(i)
                    break

    else:
        # === 기존 코드 (스키마 없을 때) ===
        label_sel_copy = label_sel[:]
        for unit in pred_sel:
            if unit in label_sel_copy:
                cnt += 1
                label_sel_copy.remove(unit)

        label_wo_agg = [val_unit for agg_id, val_unit in label_sel]
        for unit in pred_sel:
            if unit[1] in label_wo_agg:
                cnt_wo_agg += 1
                label_wo_agg.remove(unit[1])

    return label_total, pred_total, cnt, cnt_wo_agg


# === WHERE절 평가 ===
def eval_where(pred, label, schema=None):
    """WHERE절 평가 (빈 조건 처리 개선 + 함수 정규화)"""

    # WHERE절 조건 추출 (짝수 인덱스만)
    pred_conds = [unit for unit in pred['where'][::2]]
    label_conds = [unit for unit in label['where'][::2]]

    # print(f"🔍 [WHERE_EVAL_DEBUG] pred_conds 개수: {len(pred_conds)}")
    # print(f"🔍 [WHERE_EVAL_DEBUG] label_conds 개수: {len(label_conds)}")
    # print(f"🔍 [WHERE_EVAL_DEBUG] pred_conds: {pred_conds}")
    # print(f"🔍 [WHERE_EVAL_DEBUG] label_conds: {label_conds}")

    pred_total = len(pred_conds)
    label_total = len(label_conds)

    # === 빈 조건 처리 개선 ===
    if pred_total == 0 and label_total == 0:
        # 둘 다 WHERE절 없음 → 완벽 일치
        return 0, 0, 0, 0
    elif pred_total == 0 and label_total > 0:
        # 생성SQL에 필요한 WHERE절 누락 → 0점
        return label_total, pred_total, 0, 0
    elif pred_total > 0 and label_total == 0:
        # 생성SQL에 불필요한 WHERE절 추가 → 0점
        return label_total, pred_total, 0, 0

    # === 기존 로직: 함수 정규화 적용 ===
    if schema:
        # 조건 단위들을 정규화 (not_op, op_id, val_unit, val1, val2)
        pred_conds_normalized = []
        for cond in pred_conds:
            if len(cond) >= 3:
                not_op, op_id, val_unit = cond[0], cond[1], cond[2]
                normalized_val_unit = normalize_val_unit_semantic(val_unit, val_unit, schema)  # 임시로 자기 자신과 비교
                pred_conds_normalized.append((not_op, op_id, normalized_val_unit) + cond[3:])
            else:
                pred_conds_normalized.append(cond)

        label_conds_normalized = []
        for cond in label_conds:
            if len(cond) >= 3:
                not_op, op_id, val_unit = cond[0], cond[1], cond[2]
                normalized_val_unit = normalize_val_unit_semantic(val_unit, val_unit, schema)  # 임시로 자기 자신과 비교
                label_conds_normalized.append((not_op, op_id, normalized_val_unit) + cond[3:])
            else:
                label_conds_normalized.append(cond)
    else:
        pred_conds_normalized = pred_conds[:]
        label_conds_normalized = label_conds[:]

    # 연산자 제외 비교용 리스트 생성
    label_wo_op = [cond[2] for cond in label_conds_normalized if len(cond) > 2]

    cnt = 0
    cnt_wo_op = 0

    # 예측된 각 조건에 대해 정답과 비교
    for unit in pred_conds_normalized:
        # 완전 일치 검사
        if unit in label_conds_normalized:
            cnt += 1
            label_conds_normalized.remove(unit)

        # 연산자 제외 일치 검사
        if len(unit) > 2 and unit[2] in label_wo_op:
            cnt_wo_op += 1
            label_wo_op.remove(unit[2])

    return label_total, pred_total, cnt, cnt_wo_op


# === GROUP BY절 평가 (HAVING 제외) ===
def eval_group(pred, label):
    # GROUP BY 컬럼들 추출
    pred_cols = [unit[1] for unit in pred['groupBy']]
    label_cols = [unit[1] for unit in label['groupBy']]

    # 테이블명 제거 (컬럼명만 비교)
    pred_cols = [pred.split(".")[1] if "." in pred else pred for pred in pred_cols]
    label_cols = [label.split(".")[1] if "." in label else label for label in label_cols]

    pred_total = len(pred_cols)
    label_total = len(label_cols)
    cnt = 0

    # 예측된 각 컬럼에 대해 정답과 비교
    for col in pred_cols:
        if col in label_cols:
            cnt += 1
            label_cols.remove(col)

    return label_total, pred_total, cnt


# === HAVING절 평가 ===
def eval_having(pred, label):
    # GROUP BY와 HAVING을 함께 고려한 평가
    pred_total = label_total = cnt = 0

    # GROUP BY 존재 여부 확인
    if len(pred['groupBy']) > 0:
        pred_total = 1
    if len(label['groupBy']) > 0:
        label_total = 1

    # GROUP BY 컬럼 추출
    pred_cols = [unit[1] for unit in pred['groupBy']]
    label_cols = [unit[1] for unit in label['groupBy']]

    # 완전 일치 검사 (GROUP BY와 HAVING 모두)
    if pred_total == label_total == 1 and pred_cols == label_cols and pred['having'] == label['having']:
        cnt = 1

    return label_total, pred_total, cnt



def eval_order(pred, label, schema=None):
    """ORDER BY절 평가"""
    # ORDER BY 존재 여부 확인
    pred_total = label_total = cnt = 0

    if len(pred.get('orderBy', [])) > 1 and pred['orderBy'][1]:
        pred_total = 1
    if len(label.get('orderBy', [])) > 1 and label['orderBy'][1]:
        label_total = 1

    if pred_total == 0 and label_total == 0:
        return 0, 0, 0  # 둘 다 ORDER BY 없음

    # 정규화된 ORDER BY 비교
    if pred.get('orderBy') == label.get('orderBy'):
        cnt = 1

    return label_total, pred_total, cnt


def eval_and_or(pred, label):
    """AND/OR 연산자 평가"""

    def get_operators(sql):
        res = sql['from']['conds'][1::2] + sql['where'][1::2] + sql['having'][1::2]
        return res

    pred_ao = get_operators(pred)
    label_ao = get_operators(label)
    pred_ao = [o for o in pred_ao if o in ['and', 'or']]
    label_ao = [o for o in label_ao if o in ['and', 'or']]
    pred_total = len(pred_ao)
    label_total = len(label_ao)

    cnt = 0
    if pred_ao == label_ao:
        cnt = pred_total

    return label_total, pred_total, cnt


def eval_nested(pred, label):
    """중첩된 서브쿼리 평가"""
    label_total = pred_total = cnt = 0

    if pred is not None:
        pred_total = 1
    if label is not None:
        label_total = 1

    # 둘 다 서브쿼리가 있으면 재귀적으로 완전 일치 평가 수행
    if pred is not None and label is not None:
        cnt += Evaluator().eval_exact_match(pred, label)

    return label_total, pred_total, cnt


def eval_IUEN(pred, label):
    """INTERSECT/UNION/EXCEPT 평가"""
    # 세 가지 집합 연산을 개별적으로 평가한 후 결과를 통합
    lt1, pt1, cnt1 = eval_nested(pred['intersect'], label['intersect'])
    lt2, pt2, cnt2 = eval_nested(pred['except'], label['except'])
    lt3, pt3, cnt3 = eval_nested(pred['union'], label['union'])

    label_total = lt1 + lt2 + lt3
    pred_total = pt1 + pt2 + pt3
    cnt = cnt1 + cnt2 + cnt3

    return label_total, pred_total, cnt


def get_keywords(sql):
    """SQL 구조체를 분석하여 사용된 모든 키워드들을 집합으로 반환"""
    res = set()

    # 기본 SQL 절 키워드 확인
    if len(sql['where']) > 0:
        res.add('where')
    if len(sql['groupBy']) > 0:
        res.add('group')
    if len(sql['having']) > 0:
        res.add('having')

    # ORDER BY 키워드 확인
    if len(sql['orderBy']) > 0:
        res.add(sql['orderBy'][0])
        res.add('order')

    # 집합 연산 키워드 확인
    if sql['except'] is not None:
        res.add('except')
    if sql['union'] is not None:
        res.add('union')
    if sql['intersect'] is not None:
        res.add('intersect')

    # 논리 연산자 확인
    ao = sql['from']['conds'][1::2] + sql['where'][1::2] + sql['having'][1::2]
    if len([token for token in ao if token == 'or']) > 0:
        res.add('or')

    # 조건 연산자들 확인
    cond_units = sql['from']['conds'][::2] + sql['where'][::2] + sql['having'][::2]

    # NOT 키워드 확인
    if len([cond_unit for cond_unit in cond_units if cond_unit[0]]) > 0:
        res.add('not')

    # IN 키워드 확인
    val_units = []
    for cond_unit in cond_units:
        if cond_unit[2] is not None:
            val_units.append(cond_unit[2])
        if cond_unit[3] is not None and type(cond_unit[3]) != dict:
            val_units.append(cond_unit[3])
        if cond_unit[4] is not None and type(cond_unit[4]) != dict:
            val_units.append(cond_unit[4])

    for val_unit in val_units:
        if type(val_unit) == tuple:
            if val_unit[0] == 0:
                res.add('in')

    # LIKE 키워드 확인
    for cond_unit in cond_units:
        if cond_unit[1] == 9:
            res.add('like')

    return res


def eval_keywords(pred, label):
    """키워드 기반 평가"""
    pred_keywords = get_keywords(pred)
    label_keywords = get_keywords(label)
    pred_total = len(pred_keywords)
    label_total = len(label_keywords)
    cnt = len(pred_keywords & label_keywords)

    return label_total, pred_total, cnt


def extract_select_alias_mapping(pred_sql, label_sql):
    """
    SELECT절에서 alias → val_unit 매핑 추출
    ORDER BY에서 별칭 역추적용
    """
    pred_mapping = {}
    label_mapping = {}

    # 셍성 SQL의 SELECT alias 매핑 (파싱 결과에서 추출 시도)
    if 'select' in pred_sql:
        select_items = pred_sql['select'][1] if len(pred_sql['select']) > 1 else []
        for i, (agg_id, val_unit) in enumerate(select_items):
            # 간단한 매핑: 첫번째는 첫번째 컬럼, 두번째는 두번째 컬럼...
            pred_mapping[i] = (agg_id, val_unit)

    # 정답 SQL의 SELECT alias 매핑
    if 'select' in label_sql:
        select_items = label_sql['select'][1] if len(label_sql['select']) > 1 else []
        for i, (agg_id, val_unit) in enumerate(select_items):
            label_mapping[i] = (agg_id, val_unit)

    return pred_mapping, label_mapping


def normalize_order_by_with_alias(orderby_info, select_mapping, schema=None):
    if not orderby_info or len(orderby_info) != 2:
        return orderby_info

    order_type, val_units = orderby_info
    normalized_val_units = []

    for val_unit in val_units:
        # === 추가: ORDER BY의 val_unit이 SELECT 항목과 직접 매칭되는지 확인 ===
        matched_from_select = False
        if select_mapping:
            for alias, select_val_unit in select_mapping.items():
                if schema:
                    if normalize_val_unit_semantic(val_unit, select_val_unit, schema):
                        normalized_val_units.append(select_val_unit)
                        matched_from_select = True
                        break
                else:
                    if val_unit == select_val_unit:
                        normalized_val_units.append(select_val_unit)
                        matched_from_select = True
                        break

        # 매칭되지 않으면 기존 로직
        if not matched_from_select:
            if schema:
                normalized_val_unit = normalize_val_unit(val_unit, schema)
            else:
                normalized_val_unit = val_unit
            normalized_val_units.append(normalized_val_unit)

    return (order_type, normalized_val_units)

# === ORDER BY절 평가 ===
def eval_order(pred, label, schema=None):
    """ORDER BY절 평가 (별칭 처리 + 함수 정규화)"""
    # ORDER BY 존재 여부 확인
    pred_total = label_total = cnt = 0

    if len(pred.get('orderBy', [])) > 1 and pred['orderBy'][1]:
        pred_total = 1
    if len(label.get('orderBy', [])) > 1 and label['orderBy'][1]:
        label_total = 1

    if pred_total == 0 and label_total == 0:
        return 0, 0, 0  # 둘 다 ORDER BY 없음

    # SELECT alias 매핑 추출
    pred_select_mapping, label_select_mapping = extract_select_alias_mapping(pred, label)

    # === 추가: alias 없을 때 순서 기반 매핑 ===
    if not pred_select_mapping and not label_select_mapping:
        # 둘 다 alias가 없으면 SELECT 순서대로 매핑
        pred_items = pred['select'][1] if len(pred['select']) > 1 else []
        label_items = label['select'][1] if len(label['select']) > 1 else []

        for i, (agg_id, val_unit) in enumerate(pred_items):
            pred_select_mapping[i] = (agg_id, val_unit)
        for i, (agg_id, val_unit) in enumerate(label_items):
            label_select_mapping[i] = (agg_id, val_unit)

    # ORDER BY 정규화 (기존 코드)
    pred_normalized = normalize_order_by_with_alias(pred.get('orderBy'), pred_select_mapping, schema)
    label_normalized = normalize_order_by_with_alias(label.get('orderBy'), label_select_mapping, schema)

    # 정규화된 ORDER BY 비교
    if pred_normalized == label_normalized:
        cnt = 1

    return label_total, pred_total, cnt


# === AND/OR 연산자 평가 ===
def eval_and_or(pred, label):
    # WHERE절에서 사용된 AND/OR 연산자들의 집합 비교
    def get_nestedSQL(sql):
        nested = []
        for cond_unit in sql['from']['conds'][::2] + sql['where'][::2] + sql['having'][::2]:
            if cond_unit[3] is not None and type(cond_unit[3]) is dict:
                nested.append(cond_unit[3])
            if cond_unit[4] is not None and type(cond_unit[4]) is dict:
                nested.append(cond_unit[4])
        if sql['intersect'] is not None:
            nested.append(sql['intersect'])
        if sql['except'] is not None:
            nested.append(sql['except'])
        if sql['union'] is not None:
            nested.append(sql['union'])
        return nested

    def get_operators(sql):
        res = sql['from']['conds'][1::2] + sql['where'][1::2] + sql['having'][1::2]
        for nested in get_nestedSQL(sql):
            res.extend(get_operators(nested))
        return res

    pred_ao = get_operators(pred)
    label_ao = get_operators(label)
    pred_ao = [o for o in pred_ao if o in ['and', 'or']]
    label_ao = [o for o in label_ao if o in ['and', 'or']]
    pred_total = len(pred_ao)
    label_total = len(label_ao)

    cnt = 0
    if pred_ao == label_ao:
        cnt = pred_total

    return label_total, pred_total, cnt


# === 중첩된 서브쿼리 평가 ===
def eval_nested(pred, label):
    # 서브쿼리 비교 평가
    label_total = pred_total = cnt = 0

    if pred is not None:
        pred_total = 1
    if label is not None:
        label_total = 1

    # 둘 다 서브쿼리가 있으면 재귀적으로 완전 일치 평가 수행
    if pred is not None and label is not None:
        cnt += Evaluator().eval_exact_match(pred, label)

    return label_total, pred_total, cnt


# === INTERSECT/UNION/EXCEPT 평가 ===
def eval_IUEN(pred, label):
    # 세 가지 집합 연산을 개별적으로 평가한 후 결과를 통합
    lt1, pt1, cnt1 = eval_nested(pred['intersect'], label['intersect'])
    lt2, pt2, cnt2 = eval_nested(pred['except'], label['except'])
    lt3, pt3, cnt3 = eval_nested(pred['union'], label['union'])

    label_total = lt1 + lt2 + lt3
    pred_total = pt1 + pt2 + pt3
    cnt = cnt1 + cnt2 + cnt3

    return label_total, pred_total, cnt


# === 키워드 추출 및 평가 ===
def get_keywords(sql):
    # SQL 구조체를 분석하여 사용된 모든 키워드들을 집합으로 반환
    res = set()

    # 기본 SQL 절 키워드 확인
    if len(sql['where']) > 0:
        res.add('where')
    if len(sql['groupBy']) > 0:
        res.add('group')
    if len(sql['having']) > 0:
        res.add('having')

    # ORDER BY 키워드 확인
    if len(sql['orderBy']) > 0:
        res.add(sql['orderBy'][0])
        res.add('order')

    # 집합 연산 키워드 확인
    if sql['except'] is not None:
        res.add('except')
    if sql['union'] is not None:
        res.add('union')
    if sql['intersect'] is not None:
        res.add('intersect')

    # 논리 연산자 확인
    ao = sql['from']['conds'][1::2] + sql['where'][1::2] + sql['having'][1::2]
    if len([token for token in ao if token == 'or']) > 0:
        res.add('or')

    # 조건 연산자들 확인
    cond_units = sql['from']['conds'][::2] + sql['where'][::2] + sql['having'][::2]

    # NOT 키워드 확인
    if len([cond_unit for cond_unit in cond_units if cond_unit[0]]) > 0:
        res.add('not')

    # IN 키워드 확인
    val_units = []
    for cond_unit in cond_units:
        if cond_unit[2] is not None:
            val_units.append(cond_unit[2])
        if cond_unit[3] is not None and type(cond_unit[3]) != dict:
            val_units.append(cond_unit[3])
        if cond_unit[4] is not None and type(cond_unit[4]) != dict:
            val_units.append(cond_unit[4])

    for val_unit in val_units:
        if type(val_unit) == tuple:
            if val_unit[0] == 0:
                res.add('in')

    # LIKE 키워드 확인
    for cond_unit in cond_units:
        if cond_unit[1] == 9:
            res.add('like')

    return res


def eval_keywords(pred, label):
    # 키워드 기반 평가
    pred_keywords = get_keywords(pred)
    label_keywords = get_keywords(label)
    pred_total = len(pred_keywords)
    label_total = len(label_keywords)
    cnt = len(pred_keywords & label_keywords)

    return label_total, pred_total, cnt


# === 턴 점수 추적 ===
turn_scores = {'exec': [], 'exact': []}


def eval_turn_scores():
    # 턴별 점수 출력
    if turn_scores['exec']:
        print(f"Turn Execution Accuracy: {sum(turn_scores['exec']) / len(turn_scores['exec']):.3f}")
    if turn_scores['exact']:
        print(f"Turn Exact Match Accuracy: {sum(turn_scores['exact']) / len(turn_scores['exact']):.3f}")


# === Evaluator 클래스 ===
class Evaluator:
    def __init__(self, schema=None):
        self.partial_scores = None
        self.schema = schema

    def eval_hardness(self, sql):
        # SQL 복잡도 평가
        count_comp1_ = 0
        count_comp2_ = 0
        count_others = 0

        if len(sql['where']) > 1:
            count_comp1_ += 1
        if len(sql['groupBy']) > 0:
            count_comp1_ += 1
        if len(sql['orderBy']) > 0:
            count_comp1_ += 1
        if condition_has_or(sql['from']['conds']):
            count_comp1_ += 1
        if condition_has_like(sql['where']):
            count_comp1_ += 1

        for keyword in ['except', 'union', 'intersect']:
            if sql[keyword]:
                count_comp2_ += 1

        if sql['intersect'] is not None:
            count_others += 1
        if sql['except'] is not None:
            count_others += 1
        if sql['union'] is not None:
            count_others += 1
        if condition_has_sql(sql['where']):
            count_others += 1
        if condition_has_sql(sql['having']):
            count_others += 1

        if count_comp1_ <= 1 and count_others == 0 and count_comp2_ == 0:
            return "easy"
        elif (count_others <= 2 and count_comp1_ <= 1 and count_comp2_ == 0) or (
                count_comp1_ <= 2 and count_others < 2 and count_comp2_ == 0):
            return "medium"
        elif (count_others <= 2 and count_comp1_ <= 2 and count_comp2_ <= 1) or (
                count_comp1_ <= 3 and count_others <= 2 and count_comp2_ == 0) or (
                count_comp1_ <= 1 and count_others == 0 and count_comp2_ <= 1):
            return "hard"
        else:
            return "extra"

    def eval_exact_match(self, pred, label):
        """정확 일치 평가 - None 값 안전 처리"""
        try:
            partial_scores = self.partial_match(pred, label)
            self.partial_scores = partial_scores

            if not partial_scores:
                return 0

            # 모든 절이 완전히 일치하는지 확인
            for key, scores in partial_scores.items():
                if not isinstance(scores, dict):
                    continue

                f1_score = scores.get('f1')

                # 🔥 핵심 수정: None 처리
                if f1_score is None:
                    # 사용되지 않은 절(not_used=True)은 무시
                    continue
                elif f1_score < 1.0:
                    return 0

            return 1

        except Exception as e:
            print(f"❌ [SPARC] eval_exact_match 오류: {e}")
            return 0

    # evaluation.py의 Evaluator.partial_match() 메서드 수정

    def partial_match(self, pred, label):
        """부분 일치 평가 - 반환값 개수 정확히 맞춤"""
        res = {}

        # SELECT절 평가 (4개 값 반환)
        label_total, pred_total, cnt, cnt_wo_agg = eval_select(pred, label)
        scores_result = get_scores(cnt, pred_total, label_total)
        if scores_result[0] is not None:
            acc, rec, f1 = scores_result
            res['select'] = {'acc': acc, 'rec': rec, 'f1': f1, 'label_total': label_total, 'pred_total': pred_total}
        else:
            res['select'] = {'acc': None, 'rec': None, 'f1': None, 'label_total': 0, 'pred_total': 0, 'not_used': True}

        # SELECT(no AGG)절 평가 (cnt_wo_agg 사용)
        scores_result = get_scores(cnt_wo_agg, pred_total, label_total)
        if scores_result[0] is not None:
            acc, rec, f1 = scores_result
            res['select(no AGG)'] = {'acc': acc, 'rec': rec, 'f1': f1, 'label_total': label_total,
                                     'pred_total': pred_total}
        else:
            res['select(no AGG)'] = {'acc': None, 'rec': None, 'f1': None, 'label_total': 0, 'pred_total': 0,
                                     'not_used': True}

        # WHERE절 평가 (4개 값 반환)
        label_total, pred_total, cnt, cnt_wo_op = eval_where(pred, label)
        scores_result = get_scores(cnt, pred_total, label_total)
        if scores_result[0] is not None:
            acc, rec, f1 = scores_result
            res['where'] = {'acc': acc, 'rec': rec, 'f1': f1, 'label_total': label_total, 'pred_total': pred_total}
        else:
            res['where'] = {'acc': None, 'rec': None, 'f1': None, 'label_total': 0, 'pred_total': 0, 'not_used': True}

        # WHERE(no OP)절 평가 (cnt_wo_op 사용)
        scores_result = get_scores(cnt_wo_op, pred_total, label_total)
        if scores_result[0] is not None:
            acc, rec, f1 = scores_result
            res['where(no OP)'] = {'acc': acc, 'rec': rec, 'f1': f1, 'label_total': label_total, 'pred_total': pred_total}
        else:
            res['where(no OP)'] = {'acc': None, 'rec': None, 'f1': None, 'label_total': 0, 'pred_total': 0,
                                   'not_used': True}

        # GROUP BY절 평가 (HAVING 제외) - 3개 값 반환
        label_total, pred_total, cnt = eval_group(pred, label)
        scores_result = get_scores(cnt, pred_total, label_total)
        if scores_result[0] is not None:
            acc, rec, f1 = scores_result
            res['group(no Having)'] = {'acc': acc, 'rec': rec, 'f1': f1, 'label_total': label_total, 'pred_total': pred_total}
        else:
            res['group(no Having)'] = {'acc': None, 'rec': None, 'f1': None, 'label_total': 0, 'pred_total': 0,
                                       'not_used': True}

        # GROUP BY절 평가 (HAVING 포함) - 3개 값 반환
        label_total, pred_total, cnt = eval_having(pred, label)
        scores_result = get_scores(cnt, pred_total, label_total)
        if scores_result[0] is not None:
            acc, rec, f1 = scores_result
            res['group'] = {'acc': acc, 'rec': rec, 'f1': f1, 'label_total': label_total, 'pred_total': pred_total}
        else:
            res['group'] = {'acc': None, 'rec': None, 'f1': None, 'label_total': 0, 'pred_total': 0, 'not_used': True}

        # ORDER BY절 평가 - 3개 값 반환
        label_total, pred_total, cnt = eval_order(pred, label)
        scores_result = get_scores(cnt, pred_total, label_total)
        if scores_result[0] is not None:
            acc, rec, f1 = scores_result
            res['order'] = {'acc': acc, 'rec': rec, 'f1': f1, 'label_total': label_total, 'pred_total': pred_total}
        else:
            res['order'] = {'acc': None, 'rec': None, 'f1': None, 'label_total': 0, 'pred_total': 0, 'not_used': True}

        # AND/OR 평가 - 3개 값 반환
        label_total, pred_total, cnt = eval_and_or(pred, label)
        scores_result = get_scores(cnt, pred_total, label_total)
        if scores_result[0] is not None:
            acc, rec, f1 = scores_result
            res['and/or'] = {'acc': acc, 'rec': rec, 'f1': f1, 'label_total': label_total, 'pred_total': pred_total}
        else:
            res['and/or'] = {'acc': None, 'rec': None, 'f1': None, 'label_total': 0, 'pred_total': 0, 'not_used': True}

        # INTERSECT/UNION/EXCEPT 평가 - 3개 값 반환
        label_total, pred_total, cnt = eval_IUEN(pred, label)
        scores_result = get_scores(cnt, pred_total, label_total)
        if scores_result[0] is not None:
            acc, rec, f1 = scores_result
            res['IUEN'] = {'acc': acc, 'rec': rec, 'f1': f1, 'label_total': label_total, 'pred_total': pred_total}
        else:
            res['IUEN'] = {'acc': None, 'rec': None, 'f1': None, 'label_total': 0, 'pred_total': 0, 'not_used': True}

        # 키워드 평가 - 3개 값 반환
        label_total, pred_total, cnt = eval_keywords(pred, label)
        scores_result = get_scores(cnt, pred_total, label_total)
        if scores_result[0] is not None:
            acc, rec, f1 = scores_result
            res['keywords'] = {'acc': acc, 'rec': rec, 'f1': f1, 'label_total': label_total, 'pred_total': pred_total}
        else:
            res['keywords'] = {'acc': None, 'rec': None, 'f1': None, 'label_total': 0, 'pred_total': 0,
                               'not_used': True}

        return res



def get_scores_safe(count, pred_total, label_total):
    """안전한 점수 계산 함수 - None 값 방지"""
    try:
        if pred_total == 0 and label_total == 0:
            return None, None, None  # 사용되지 않은 절
        elif pred_total == 0:
            return 0.0, 0.0, 0.0  # 예측이 없음
        elif label_total == 0:
            return 0.0, 0.0, 0.0  # 정답이 없음
        else:
            acc = float(count) / float(pred_total)
            rec = float(count) / float(label_total)

            if acc + rec == 0:
                f1 = 0.0
            else:
                f1 = (2.0 * acc * rec) / (acc + rec)

            return acc, rec, f1

    except Exception as e:
        print(f"❌ [SPARC] get_scores_safe 오류: {e}")
        return 0.0, 0.0, 0.0


def create_score_dict(scores_result, label_total, pred_total):
    """점수 딕셔너리 생성 - None 처리 포함"""
    if scores_result[0] is None:
        # 사용되지 않은 절
        return {
            'acc': None, 'rec': None, 'f1': None,
            'label_total': 0, 'pred_total': 0, 'not_used': True
        }
    else:
        acc, rec, f1 = scores_result
        return {
            'acc': acc, 'rec': rec, 'f1': f1,
            'label_total': label_total, 'pred_total': pred_total
        }

# === 점수 출력 함수 ===
def print_scores(scores, etype):
    # 점수 출력
    levels = ['easy', 'medium', 'hard', 'extra', 'all']
    partial_types = ['select', 'select(no AGG)', 'where', 'where(no OP)', 'group(no Having)',
                     'group', 'order', 'and/or', 'IUEN', 'keywords']

    print("=" * 50)
    print(f"📊 SQL 평가 결과 ({etype})")
    print("=" * 50)

    for level in levels:
        if scores[level]['count'] == 0:
            continue

        print(f"\n🎯 {level.upper()} (개수: {scores[level]['count']})")

        if etype in ["all", "exec"]:
            exec_acc = scores[level]['exec']
            print(f"  • 실행 정확도: {exec_acc:.3f}")

        if etype in ["all", "match"]:
            exact_acc = scores[level]['exact']
            print(f"  • 정확 일치: {exact_acc:.3f}")

            print(f"  📈 부분 점수:")
            for type_ in partial_types:
                partial = scores[level]['partial'][type_]
                if partial['label_total'] > 0 or partial['pred_total'] > 0:
                    print(f"    - {type_}: F1={partial['f1']:.3f}")

# === SQL 재구성 함수들 ===
def rebuild_sql_val(sql):
    # SQL 값 재구성
    if type(sql) == dict:
        for key, val in sql.items():
            if type(val) == list:
                for i, item in enumerate(val):
                    sql[key][i] = rebuild_sql_val(item)
            elif type(val) == dict:
                sql[key] = rebuild_sql_val(val)

    return sql


def rebuild_sql_col(valid_col_units, sql, kmap):
    # SQL 컬럼 재구성
    if type(sql) == dict:
        for key, val in sql.items():
            if type(val) == list:
                for i, item in enumerate(val):
                    sql[key][i] = rebuild_sql_col(valid_col_units, item, kmap)
            elif type(val) == dict:
                sql[key] = rebuild_sql_col(valid_col_units, val, kmap)

    return sql


def build_valid_col_units(table_units, schema):
    # 유효한 컬럼 단위들 구성
    col_ids = []
    for table_unit in table_units:
        if table_unit[0] == 'table_unit':
            table_name = table_unit[1]
            if table_name in schema.schema:
                for i, col_name in enumerate(schema.schema[table_name]):
                    col_ids.append((0, i, table_name, col_name))
    return col_ids


# === 실행 결과 일치 평가 ===
def eval_exec_match(db, p_str, g_str, pred, gold):
    # 셍성과 정답의 실행 결과가 일치하는지 확인
    conn = None
    cursor = None

    try:
        conn = get_oracle_connection()
        if conn is None:
            print("❌ eval_exec_match: 데이터베이스 연결 실패")
            return False

        cursor = conn.cursor()

        # SQL 전처리 함수
        from evaluation_module import normalize_oracle_sql_for_comparison

        # SQL 정규화 적용 (exact match와 동일한 기준)
        p_str_clean = normalize_oracle_sql_for_comparison(p_str)
        g_str_clean = normalize_oracle_sql_for_comparison(g_str)

        # 🔥 디버깅 출력 추가
        # print(f"🔍 [EVAL_DEBUG] 원본 셍성 SQL: {repr(p_str)}")
        print(f"🔍 [EVAL_DEBUG] 정리된 셍성 SQL: {repr(p_str_clean)}")
        # print(f"🔍 [EVAL_DEBUG] 원본 정답 SQL: {repr(g_str)}")
        print(f"🔍 [EVAL_DEBUG] 정리된 정답 SQL: {repr(g_str_clean)}")

        # 예측 SQL 실행
        try:
            # print(f"🔍 [EVAL_DEBUG] 셍성 SQL 실행 시도...")
            cursor.execute(p_str_clean)
            p_res = cursor.fetchall()
            p_res = [list(row) for row in p_res]
            print(f"🔍 [EVAL_DEBUG] 셍성 SQL 실행 성공: {len(p_res)}행")
        except Exception as e:
            print(f"❌ 셍성 SQL 실행 오류: {e}")
            print(f"🔍 [EVAL_DEBUG] 실패한 SQL: {repr(p_str_clean)}")
            return False

        # 정답 SQL 실행
        try:
            # print(f"🔍 [EVAL_DEBUG] 정답 SQL 실행 시도...")
            cursor.execute(g_str_clean)
            g_res = cursor.fetchall()
            g_res = [list(row) for row in g_res]
            print(f"🔍 [EVAL_DEBUG] 정답 SQL 실행 성공: {len(g_res)}행")
        except Exception as e:
            print(f"❌ 정답 SQL 실행 오류: {e}")
            print(f"🔍 [EVAL_DEBUG] 실패한 SQL: {repr(g_str_clean)}")
            return False

        # 결과 비교
        if len(p_res) != len(g_res):
            return False

        # 각 행을 정렬하여 비교
        p_res_sorted = sorted([tuple(row) for row in p_res])
        g_res_sorted = sorted([tuple(row) for row in g_res])

        return p_res_sorted == g_res_sorted

    except Exception as e:
        print(f"❌ eval_exec_match 오류: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# === 외래키 맵 구성 함수들 ===
def build_foreign_key_map(entry):
    # JSON 스키마에서 외래키 맵 구성
    cols = []

    for i, (tab_id, col_name) in enumerate(zip(entry["column_names"][1:], entry["column_names_original"][1:])):
        if tab_id >= 0:
            t = entry["table_names_original"][tab_id]
            c = col_name
            cols.append("__" + t.lower() + "." + c.lower() + "__")
        else:
            cols.append("__all__")

    def keyset_in_list(k1, k2, k_list):
        # 키 집합을 리스트에서 찾거나 새로 생성하는 내부 함수
        for k_set in k_list:
            if k1 in k_set or k2 in k_set:
                return k_set
        new_k_set = set()
        k_list.append(new_k_set)
        return new_k_set

    foreign_key_list = []
    foreign_keys = entry["foreign_keys"]
    for fkey in foreign_keys:
        key1, key2 = fkey
        key_set = keyset_in_list(key1, key2, foreign_key_list)
        key_set.add(key1)
        key_set.add(key2)

    foreign_key_map = {}
    for key_set in foreign_key_list:
        sorted_list = sorted(list(key_set))
        midx = sorted_list[0]
        for idx in sorted_list:
            foreign_key_map[cols[idx]] = cols[midx]

    return foreign_key_map


def build_foreign_key_map_from_json(table):
    # JSON 파일에서 외래키 맵 구성 함수
    with open(table) as f:
        data = json.load(f)
    tables = {}
    for entry in data:
        tables[entry['db_id']] = build_foreign_key_map(entry)
    return tables


def build_foreign_key_map_from_oracle(table_config):
    # Oracle에서 외래키 맵 구성 함수
    try:
        conn = get_oracle_connection()
        cursor = conn.cursor()

        # Oracle 외래키 정보 조회
        cursor.execute("""
            SELECT 
                a.table_name as child_table, 
                a.column_name as child_column,
                c_pk.table_name as parent_table, 
                c_pk.column_name as parent_column
            FROM user_cons_columns a
            JOIN user_constraints b ON a.constraint_name = b.constraint_name
            JOIN user_cons_columns c_pk ON b.r_constraint_name = c_pk.constraint_name
            WHERE b.constraint_type = 'R'
        """)

        foreign_keys = cursor.fetchall()

        # 외래키 맵 구성
        foreign_key_map = {}
        for child_table, child_col, parent_table, parent_col in foreign_keys:
            child_key = f"__{child_table.lower()}.{child_col.lower()}__"
            parent_key = f"__{parent_table.lower()}.{parent_col.lower()}__"
            foreign_key_map[child_key] = parent_key

        cursor.close()
        conn.close()

        return {"mimic_iv": foreign_key_map}

    except Exception as e:
        print(f"Foreign key map building error: {e}")
        return {"mimic_iv": {}}


def build_simple_foreign_key_map():
    # 간단한 외래키 맵 - 외래키 관계 없이 사용
    return {"mimic_iv": {}}


# === 메인 평가 함수 ===
def evaluate(gold, predict, db_dir, etype, kmaps):
    # 메인 SQL 평가 함수 - 정답과 셍성 SQL을 비교하여 다양한 지표로 평가
    # 정답 파일 읽기 및 파싱
    with open(gold) as f:
        glist = []
        gseq_one = []
        for l in f.readlines():
            if len(l.strip()) == 0:
                glist.append(gseq_one)
                gseq_one = []
            else:
                lstrip = l.strip().split('\t')
                gseq_one.append(lstrip)

    # 셍성 파일 읽기 및 파싱
    with open(predict) as f:
        plist = []
        pseq_one = []
        for l in f.readlines():
            if len(l.strip()) == 0:
                plist.append(pseq_one)
                pseq_one = []
            else:
                pseq_one.append(l.strip().split('\t'))

    evaluator = Evaluator()

    # 평가 결과를 저장할 데이터 구조 초기화
    turns = ['turn 1', 'turn 2', 'turn 3', 'turn 4', 'turn >4']
    levels = ['easy', 'medium', 'hard', 'extra', 'all', 'joint_all']
    partial_types = ['select', 'select(no AGG)', 'where', 'where(no OP)', 'group(no Having)',
                     'group', 'order', 'and/or', 'IUEN', 'keywords']
    entries = []
    scores = {}

    # 턴별 점수 초기화
    for turn in turns:
        scores[turn] = {'count': 0, 'exact': 0.}
        scores[turn]['exec'] = 0

    # 난이도별 점수 초기화
    for level in levels:
        scores[level] = {'count': 0, 'partial': {}, 'exact': 0.}
        scores[level]['exec'] = 0
        for type_ in partial_types:
            scores[level]['partial'][type_] = {'acc': 0., 'rec': 0., 'f1': 0., 'acc_count': 0, 'rec_count': 0}

    eval_err_num = 0

    # 메인 평가 루프
    for p, g in zip(plist, glist):
        scores['joint_all']['count'] += 1
        turn_scores = {"exec": [], "exact": []}

        for idx, pg in enumerate(zip(p, g)):
            p, g = pg
            p_str = p[0]
            p_str = p_str.replace("value", "1")
            g_str, db = g
            db_name = db

            # Oracle 스키마 정보 처리
            try:
                schema = get_oracle_schema_info(db_name)
            except:
                continue

            g_sql = get_sql(schema, g_str)
            # Gold SQL 파싱 디버깅 로그
            if not g_str:
                print("❌ gold_sql 없음!")
            elif g_sql is None:
                print("❌ gold_sql 파싱 실패! → g_str =", g_str)
            else:
                print("✅ gold_sql 파싱 성공 → g_str =", g_str)

            hardness = evaluator.eval_hardness(g_sql)

            # 턴 인덱스 처리
            if idx > 3:
                idx = ">4"
            else:
                idx += 1
            turn_id = "turn " + str(idx)

            # 각 분류별 카운트 증가
            scores[turn_id]['count'] += 1
            scores[hardness]['count'] += 1
            scores['all']['count'] += 1

            # 셍성 SQL 파싱 시도
            try:
                p_sql = get_sql(schema, p_str)
            except:
                # 셍성 SQL이 유효하지 않으면 빈 SQL 구조를 사용
                p_sql = {
                    "except": None,
                    "from": {
                        "conds": [],
                        "table_units": []
                    },
                    "groupBy": [],
                    "having": [],
                    "intersect": None,
                    "orderBy": [],
                    "select": [
                        False,
                        []
                    ],
                    "union": None,
                    "where": []
                }
                eval_err_num += 1
                print("eval_err_num:{}".format(eval_err_num))

            # 값 평가를 위한 SQL 재구성 (외래키 관계 고려)
            kmap = kmaps[db_name]
            g_valid_col_units = build_valid_col_units(g_sql['from']['table_units'], schema)
            g_sql = rebuild_sql_val(g_sql)
            g_sql = rebuild_sql_col(g_valid_col_units, g_sql, kmap)
            p_valid_col_units = build_valid_col_units(p_sql['from']['table_units'], schema)
            p_sql = rebuild_sql_val(p_sql)
            p_sql = rebuild_sql_col(p_valid_col_units, p_sql, kmap)

            # 실행 평가 (실제 SQL 실행 결과 비교)
            if etype in ["all", "exec"]:
                exec_score = eval_exec_match(db_name, p_str, g_str, p_sql, g_sql)
                if exec_score:
                    scores[hardness]['exec'] += 1
                    scores[turn_id]['exec'] += 1
                    turn_scores['exec'].append(1)
                else:
                    turn_scores['exec'].append(0)

            # 매칭 평가 (SQL 구조 비교)
            if etype in ["all", "match"]:
                exact_score = evaluator.eval_exact_match(p_sql, g_sql)
                partial_scores = evaluator.partial_scores
                if exact_score == 0:
                    turn_scores['exact'].append(0)
                    print("{} pred: {}".format(hardness, p_str))
                    print("{} gold: {}".format(hardness, g_str))
                    print("")
                else:
                    turn_scores['exact'].append(1)

                # 각 분류별 점수 누적
                scores[turn_id]['exact'] += exact_score
                scores[hardness]['exact'] += exact_score
                scores['all']['exact'] += exact_score

                # 부분 점수들 누적
                for type_ in partial_types:
                    if partial_scores[type_]['pred_total'] > 0:
                        scores[hardness]['partial'][type_]['acc'] += partial_scores[type_]['acc']
                        scores[hardness]['partial'][type_]['acc_count'] += 1
                        scores[turn_id]['partial'][type_]['acc'] += partial_scores[type_]['acc']
                        scores[turn_id]['partial'][type_]['acc_count'] += 1
                        scores['all']['partial'][type_]['acc'] += partial_scores[type_]['acc']
                        scores['all']['partial'][type_]['acc_count'] += 1
                    if partial_scores[type_]['label_total'] > 0:
                        scores[hardness]['partial'][type_]['rec'] += partial_scores[type_]['rec']
                        scores[hardness]['partial'][type_]['rec_count'] += 1
                        scores[turn_id]['partial'][type_]['rec'] += partial_scores[type_]['rec']
                        scores[turn_id]['partial'][type_]['rec_count'] += 1
                        scores['all']['partial'][type_]['rec'] += partial_scores[type_]['rec']
                        scores['all']['partial'][type_]['rec_count'] += 1

            entries.append(exec_score)

        # 턴별 점수 계산
        exec_acc = sum(turn_scores["exec"]) / len(turn_scores["exec"]) if turn_scores["exec"] else 0
        exact_acc = sum(turn_scores["exact"]) / len(turn_scores["exact"]) if turn_scores["exact"] else 0
        scores['joint_all']['exec'] += exec_acc
        scores['joint_all']['exact'] += exact_acc

    # 평균 계산
    for level in levels:
        if scores[level]['count'] == 0:
            continue
        if etype in ["all", "exec"]:
            scores[level]['exec'] /= scores[level]['count']

        if etype in ["all", "match"]:
            scores[level]['exact'] /= scores[level]['count']
            for type_ in partial_types:
                # 정확도 평균 계산
                if scores[level]['partial'][type_]['acc_count'] == 0:
                    scores[level]['partial'][type_]['acc'] = 0
                else:
                    scores[level]['partial'][type_]['acc'] = scores[level]['partial'][type_]['acc'] / \
                                                             scores[level]['partial'][type_]['acc_count'] * 1.0
                # 재현율 평균 계산
                if scores[level]['partial'][type_]['rec_count'] == 0:
                    scores[level]['partial'][type_]['rec'] = 0
                else:
                    scores[level]['partial'][type_]['rec'] = scores[level]['partial'][type_]['rec'] / \
                                                             scores[level]['partial'][type_]['rec_count'] * 1.0
                # F1 점수 계산
                if scores[level]['partial'][type_]['acc'] == 0 and scores[level]['partial'][type_]['rec'] == 0:
                    scores[level]['partial'][type_]['f1'] = 1
                else:
                    scores[level]['partial'][type_]['f1'] = 2.0 * scores[level]['partial'][type_]['acc'] * \
                                                            scores[level]['partial'][type_]['rec'] / (
                                                                        scores[level]['partial'][type_]['rec'] +
                                                                        scores[level]['partial'][type_]['acc'])

    print_scores(scores, etype)



# === 메인 실행 부분 ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--gold', dest='gold', type=str)
    parser.add_argument('--pred', dest='pred', type=str)
    parser.add_argument('--db', dest='db', type=str)
    parser.add_argument('--table', dest='table', type=str)
    parser.add_argument('--etype', dest='etype', type=str)
    args = parser.parse_args()

    gold = args.gold
    pred = args.pred
    db_config = args.db
    table = args.table
    etype = args.etype

    assert etype in ["all", "exec", "match"], "Unknown evaluation method"

    try:
        kmaps = build_foreign_key_map_from_oracle(table)
    except:
        print("Warning: Could not build foreign key map, using empty map")
        kmaps = build_simple_foreign_key_map()

    evaluate(gold, pred, db_config, etype, kmaps)