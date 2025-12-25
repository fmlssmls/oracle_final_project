#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
evaluation_module.py를 사용한 간단한 평가 스크립트
Exact Match, Execution Match, Partial Match (단순 평균) 3가지만 출력
"""

import os
import sys
import argparse
import re
import time
from datetime import datetime

# 프로젝트 모듈들 import
sys.path.append('..')

# 전역 변수 선언
ORACLE_AVAILABLE = False
oracledb = None
start_time = time.time()

try:
    from process_sql import get_sql, Schema
    from evaluation import Evaluator, get_oracle_schema_info

    # Oracle 연결을 위한 모듈
    try:
        import oracledb

        # Oracle Instant Client 라이브러리 경로 지정
        try:
            oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient_23_8")
            print("✅ Oracle Client 라이브러리 초기화 성공")
        except Exception as init_e:
            print(f"⚠️ Oracle Client 초기화 시도: {init_e}")
            # 초기화 실패해도 계속 진행
        ORACLE_AVAILABLE = True
    except ImportError:
        try:
            import cx_Oracle as oracledb

            # cx_Oracle용 초기화
            try:
                oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient_23_8")
                print("✅ cx_Oracle Client 라이브러리 초기화 성공")
            except Exception as init_e:
                print(f"⚠️ cx_Oracle Client 초기화 시도: {init_e}")
            ORACLE_AVAILABLE = True
        except ImportError:
            ORACLE_AVAILABLE = False
            print("⚠️ Oracle 라이브러리가 설치되지 않음. pip install oracledb 실행 필요")

except ImportError as e:
    print(f"❌ 모듈 import 실패: {e}")
    print("process_sql.py, evaluation.py 파일이 같은 폴더에 있는지 확인하세요.")
    sys.exit(1)


def normalize_oracle_sql_for_comparison(sql_str):
    """
    Oracle SQL을 SParC 평가용으로 정규화
    evaluation_module.py에서 가져온 함수
    """
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

    return sql


def read_sql_file(file_path):
    """SQL 파일 읽기"""
    if not os.path.exists(file_path):
        print(f"❌ 파일이 존재하지 않습니다: {file_path}")
        return []

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    queries = []
    for line in lines:
        line = line.strip()
        if line:  # 빈 줄이 아니면
            # 탭으로 분리된 경우 첫 번째가 SQL
            parts = line.split('\t')
            sql = parts[0]
            db_name = parts[1] if len(parts) > 1 else 'mimic_iv'
            queries.append((sql, db_name))

    return queries


def calculate_simple_partial_match(partial_scores):
    """
    단순 평균 방식으로 Partial Match 계산
    사용된 구성 요소들의 F1 점수만 평균
    """
    if not partial_scores:
        return 0.0

    valid_f1_scores = []

    for component_scores in partial_scores.values():
        if isinstance(component_scores, dict):
            f1 = component_scores.get('f1')
            # None이 아니고 실제로 사용된 구성 요소만 포함
            if f1 is not None and not component_scores.get('not_used', False):
                valid_f1_scores.append(f1)

    if valid_f1_scores:
        return sum(valid_f1_scores) / len(valid_f1_scores)
    else:
        return 0.0


def test_oracle_connection():
    """Oracle DB 연결 테스트"""
    global ORACLE_AVAILABLE, oracledb

    if not ORACLE_AVAILABLE or oracledb is None:
        print("⚠️ Oracle 라이브러리가 설치되지 않음")
        return False

    try:
        # Oracle 연결 정보
        user = os.getenv("ORACLE_USER", "SYSTEM")
        password = os.getenv("ORACLE_PW", "oracle_4U")
        host = os.getenv("ORACLE_HOST", "localhost")
        port = int(os.getenv("ORACLE_PORT", "1521"))
        service = os.getenv("ORACLE_SERVICE", "xe")

        # 연결 시도
        dsn = f"{host}:{port}/{service}"
        with oracledb.connect(user=user, password=password, dsn=dsn) as conn:
            print("✅ Oracle DB 연결 테스트 성공")
        return True

    except Exception as e:
        print(f"❌ Oracle 연결 실패: {str(e)[:100]}...")
        return False


def eval_exec_match_simple(db_name, pred_sql, gold_sql, pred_parsed, gold_parsed):
    """
    간단한 실행 결과 비교 (normalize_oracle_sql_for_comparison 의존성 제거)
    """
    global ORACLE_AVAILABLE, oracledb

    if not ORACLE_AVAILABLE or oracledb is None:
        return False

    try:
        print(f"    🔗 Oracle 연결 중...")
        # Oracle 연결 정보
        user = os.getenv("ORACLE_USER", "GPTify")
        password = os.getenv("ORACLE_PW", "oracle_4U")
        host = os.getenv("ORACLE_HOST", "138.2.63.245")
        port = int(os.getenv("ORACLE_PORT", "1521"))
        service = os.getenv("ORACLE_SERVICE", "srvinv.sub03250142080.kdtvcn.oraclevcn.com")

        dsn = f"{host}:{port}/{service}"

        with oracledb.connect(user=user, password=password, dsn=dsn) as conn:
            cursor = conn.cursor()

            print(f"    ⚡ Gold SQL 실행 중...")

            # SQL 정규화
            pred_sql_clean = normalize_oracle_sql_for_comparison(pred_sql)
            gold_sql_clean = normalize_oracle_sql_for_comparison(gold_sql)

            # 정답 SQL 실행
            try:
                cursor.execute(gold_sql_clean)
                gold_result = cursor.fetchall()
                gold_result = [list(row) for row in gold_result]
                print(f"    ✅ Gold SQL 완료 (행수: {len(gold_result)})")
            except Exception:
                return False  # 정답 SQL이 실행되지 않으면 False

            print(f"    ⚡ Pred SQL 실행 중...")

            # 예측 SQL 실행
            try:
                cursor.execute(pred_sql_clean)
                pred_result = cursor.fetchall()
                pred_result = [list(row) for row in pred_result]
            except Exception:
                return False  # 예측 SQL이 실행되지 않으면 False

            # 결과 비교
            if len(gold_result) != len(pred_result):
                return False

            # 각 행을 정렬하여 비교
            pred_sorted = sorted([tuple(row) for row in pred_result])
            gold_sorted = sorted([tuple(row) for row in gold_result])

            return pred_sorted == gold_sorted

    except Exception as e:
        return False


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='evaluation_module.py 기반 간단한 SQL 평가')
    parser.add_argument('--gold', default='gold.txt', help='정답 SQL 파일 (기본: gold.txt)')
    parser.add_argument('--pred', default='predict.txt', help='예측 SQL 파일 (기본: predict.txt)')

    args = parser.parse_args()

    print("🚀 evaluation_module.py 기반 SQL 평가")
    print(f"📄 정답 파일: {args.gold}")
    print(f"📄 예측 파일: {args.pred}")
    print("=" * 50)

    # 1. 파일 읽기
    gold_queries = read_sql_file(args.gold)
    pred_queries = read_sql_file(args.pred)

    if not gold_queries or not pred_queries:
        print("❌ 파일 로딩 실패")
        return

    total_queries = min(len(gold_queries), len(pred_queries))
    print(f"📊 총 쿼리 수: {total_queries}")

    # 2. 스키마 로드
    try:
        if ORACLE_AVAILABLE:
            schema = get_oracle_schema_info('mimic_iv')
        else:
            print("⚠️ Oracle 연결 불가능. 기본 스키마 사용")
            return

        if not schema:
            print("❌ 스키마 로드 실패")
            return
        print("✅ 스키마 로드 성공")
    except Exception as e:
        print(f"❌ 스키마 로드 실패: {e}")
        return

    # 3. Oracle 연결 테스트
    if ORACLE_AVAILABLE and test_oracle_connection():
        print("✅ Oracle DB 연결 성공")
        can_execute = True
    else:
        print("❌ Oracle DB 연결 실패 - Execution Match는 계산할 수 없습니다")
        can_execute = False

    # 4. Evaluator 초기화 (evaluation.py의 실용적 Evaluator)
    evaluator = Evaluator()
    print("✅ Evaluator 초기화 완료")

    print("=" * 50)
    print("🔍 평가 시작...")

    # 🔥 디버깅 코드 추가
    start_time = time.time()
    print(f"⏰ 평가 시작 시간: {datetime.now().strftime('%H:%M:%S')}")

    # 평가 결과 저장
    exact_matches = 0
    execution_matches = 0
    partial_scores_list = []
    parsing_errors = 0
    execution_errors = 0

    for i, ((gold_sql, gold_db), (pred_sql, pred_db)) in enumerate(zip(gold_queries, pred_queries)):

        # 🔥 진행 상황 출력 (매 10개마다)
        if (i + 1) % 10 == 0 or i == 0:
            elapsed = time.time() - start_time
            progress = (i + 1) / total_queries * 100
            eta = (elapsed / (i + 1)) * (total_queries - i - 1) if i > 0 else 0
            print(f"🔄 진행률: {i + 1}/{total_queries} ({progress:.1f}%) | 경과: {elapsed:.1f}s | 예상 남은 시간: {eta:.1f}s")

        # Exact Match 및 Partial Match 평가
        try:
            # 정답 SQL 파싱
            gold_parsed = get_sql(schema, gold_sql)

            # 예측 SQL 파싱
            pred_parsed = get_sql(schema, pred_sql)

            # Exact Match 계산 (evaluation.py의 관대한 평가)
            exact_match = evaluator.eval_exact_match(pred_parsed, gold_parsed)
            if exact_match:
                exact_matches += 1

            # Partial Match 계산
            partial_scores = evaluator.partial_scores
            if partial_scores:
                partial_match_score = calculate_simple_partial_match(partial_scores)
                partial_scores_list.append(partial_match_score)
            else:
                partial_scores_list.append(0.0)

        except Exception as e:
            parsing_errors += 1
            partial_scores_list.append(0.0)
            if i < 3:  # 처음 3개 오류만 출력
                print(f"  ⚠️ 쿼리 {i + 1} 파싱 실패: {str(e)[:100]}...")

        # Execution Match 평가 (간단한 자체 구현 사용)
        if can_execute:
            try:
                # 🔥 SQL 실행 시작 알림
                if (i + 1) % 5 == 0:  # 매 5개마다 출력
                    print(f"  🔍 쿼리 {i + 1}: Oracle 실행 중...")
                # 자체 구현한 실행 비교 함수 사용
                exec_result = eval_exec_match_simple(gold_db, pred_sql, gold_sql,
                                                     pred_parsed if 'pred_parsed' in locals() else None,
                                                     gold_parsed if 'gold_parsed' in locals() else None)
                if exec_result:
                    execution_matches += 1
            except Exception as e:
                execution_errors += 1
                if i < 3:  # 처음 3개 오류만 출력
                    print(f"  ⚠️ 쿼리 {i + 1} 실행 실패: {str(e)[:100]}...")

    # 5. 최종 결과 계산
    exact_match_score = exact_matches / total_queries
    execution_match_score = execution_matches / total_queries if can_execute else 0.0
    partial_match_score = sum(partial_scores_list) / len(partial_scores_list) if partial_scores_list else 0.0

    # 6. 깔끔한 결과 출력 (3줄만!)
    print("\n" + "=" * 50)
    print("🎯 최종 평가 결과")
    print("=" * 50)
    print(f"✅ Exact Match: {exact_match_score:.3f}")
    if can_execute:
        print(f"⚡ Execution Match: {execution_match_score:.3f}")
    else:
        print(f"⚡ Execution Match: N/A (DB 연결 실패)")
    print(f"📈 Partial Match: {partial_match_score:.3f}")
    print("=" * 50)

    # 추가 정보 (선택적)
    if parsing_errors > 0 or execution_errors > 0:
        print(f"📋 추가 정보: 파싱 오류 {parsing_errors}개, 실행 오류 {execution_errors}개")
        print("=" * 50)

    print("✅ 평가 완료!")


if __name__ == "__main__":
    main()