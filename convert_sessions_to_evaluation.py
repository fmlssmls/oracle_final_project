#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
multiturn_sessions.json 파일을 evaluation.py용 gold.txt/predict.txt 형식으로 변환
- 세션별로 공백줄로 구분
- 각 턴은 줄바꿈만
- gold.txt: target_sql\tmimic_iv
- predict.txt: generated_sql\tmimic_iv
"""

import json
import os
from datetime import datetime


def extract_sqls_from_sessions(json_file="multiturn_sessions.json", gold_file="gold.txt", predict_file="predict.txt"):
    """multiturn_sessions.json에서 SQL 쌍을 추출해서 gold.txt와 predict.txt 생성"""

    if not os.path.exists(json_file):
        print(f"❌ 파일이 존재하지 않습니다: {json_file}")
        return False

    try:
        # JSON 파일 읽기
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        sessions = data.get('multiturn_sessions', [])
        if not sessions:
            print("❌ multiturn_sessions가 비어있습니다")
            return False

        print(f"📊 총 {len(sessions)}개 세션 발견")

        # 완료된 세션만 필터링
        completed_sessions = [s for s in sessions if s.get('status') == '완료']
        print(f"✅ 완료된 세션: {len(completed_sessions)}개")

        if not completed_sessions:
            print("❌ 완료된 세션이 없습니다")
            return False

        # gold.txt와 predict.txt 파일 생성
        with open(gold_file, 'w', encoding='utf-8') as gold_f, \
                open(predict_file, 'w', encoding='utf-8') as pred_f:

            for session_idx, session in enumerate(completed_sessions):
                session_id = session.get('session_id', f'Session_{session_idx + 1}')
                turns = session.get('turns', [])

                if not turns:
                    print(f"⚠️ {session_id}: 턴이 없음, 건너뜀")
                    continue

                print(f"📝 {session_id}: {len(turns)}개 턴 처리")

                # 각 세션의 턴들을 줄바꿈으로 연결
                for turn in turns:
                    target_sql = turn.get('target_sql', '').strip()
                    generated_sql = turn.get('generated_sql', '').strip()

                    if target_sql and generated_sql:
                        # 멀티라인 SQL을 한 줄로 변환
                        target_sql_clean = ' '.join(target_sql.split())
                        generated_sql_clean = ' '.join(generated_sql.split())

                        # gold.txt와 predict.txt에 각각 저장
                        gold_f.write(f"{target_sql_clean}\tmimic_iv\n")
                        pred_f.write(f"{generated_sql_clean}\tmimic_iv\n")
                    else:
                        print(f"⚠️ {session_id} 턴 {turn.get('turn_number', '?')}: SQL 누락")

                # 세션 간 구분용 공백줄 (마지막 세션 제외)
                if session_idx < len(completed_sessions) - 1:
                    gold_f.write("\n")
                    pred_f.write("\n")

        # 결과 통계
        total_turns = sum(len(session.get('turns', [])) for session in completed_sessions)
        print(f"✅ 변환 완료!")
        print(f"📄 {gold_file}: 정답 SQL")
        print(f"📄 {predict_file}: 생성 SQL")
        print(f"📊 총 {total_turns}개 쿼리, {len(completed_sessions)}개 세션")

        return True

    except Exception as e:
        print(f"❌ 변환 실패: {e}")
        import traceback
        traceback.print_exc()
        return False


def show_session_summary(json_file="multiturn_sessions.json"):
    """세션별 요약 정보 출력"""

    if not os.path.exists(json_file):
        print(f"❌ 파일이 존재하지 않습니다: {json_file}")
        return

    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        sessions = data.get('multiturn_sessions', [])
        completed_sessions = [s for s in sessions if s.get('status') == '완료']

        print("=" * 80)
        print("📊 멀티턴 세션 요약")
        print("=" * 80)

        total_turns = 0
        total_exact_matches = 0
        total_execution_matches = 0

        for i, session in enumerate(completed_sessions):
            session_id = session.get('session_id', f'Session_{i + 1}')
            turns = session.get('turns', [])

            # 세션 통계
            exact_matches = sum(1 for turn in turns if turn.get('exact_match') == True)
            exec_matches = sum(1 for turn in turns if turn.get('execution_match') == True)
            total_tokens = session.get('total_tokens', 0)

            created_at = session.get('created_at', '')
            completed_at = session.get('completed_at', '')

            print(f"🎯 {session_id}:")
            print(f"   • 턴 수: {len(turns)}개")
            print(f"   • Exact Match: {exact_matches}/{len(turns)} ({exact_matches / len(turns) * 100:.1f}%)")
            print(f"   • Execution Match: {exec_matches}/{len(turns)} ({exec_matches / len(turns) * 100:.1f}%)")
            print(f"   • 토큰 사용량: {total_tokens}")
            if created_at:
                created_time = datetime.fromisoformat(created_at).strftime('%m-%d %H:%M')
                print(f"   • 생성 시간: {created_time}")
            print()

            total_turns += len(turns)
            total_exact_matches += exact_matches
            total_execution_matches += exec_matches

        # 전체 통계
        print("📋 전체 요약:")
        print(f"   • 완료 세션: {len(completed_sessions)}개")
        print(f"   • 전체 턴: {total_turns}개")
        print(
            f"   • 전체 Exact Match: {total_exact_matches}/{total_turns} ({total_exact_matches / total_turns * 100:.1f}%)")
        print(
            f"   • 전체 Execution Match: {total_execution_matches}/{total_turns} ({total_execution_matches / total_turns * 100:.1f}%)")
        print("=" * 80)

    except Exception as e:
        print(f"❌ 요약 생성 실패: {e}")


def validate_files(gold_file="gold.txt", predict_file="predict.txt"):
    """생성된 파일들의 유효성 검사"""

    print("\n🔍 파일 검증 중...")

    try:
        # 파일 존재 확인
        if not os.path.exists(gold_file):
            print(f"❌ {gold_file} 파일이 없습니다")
            return False

        if not os.path.exists(predict_file):
            print(f"❌ {predict_file} 파일이 없습니다")
            return False

        # 라인 수 확인
        with open(gold_file, 'r', encoding='utf-8') as f:
            gold_lines = f.readlines()

        with open(predict_file, 'r', encoding='utf-8') as f:
            pred_lines = f.readlines()

        print(f"📄 {gold_file}: {len(gold_lines)}줄")
        print(f"📄 {predict_file}: {len(pred_lines)}줄")

        if len(gold_lines) != len(pred_lines):
            print("⚠️ 파일의 라인 수가 다릅니다!")
            return False

        # 샘플 검증
        non_empty_gold = [line for line in gold_lines if line.strip()]
        non_empty_pred = [line for line in pred_lines if line.strip()]

        print(f"📊 실제 쿼리: gold {len(non_empty_gold)}개, predict {len(non_empty_pred)}개")

        # 첫 번째 쿼리 예시 출력
        if non_empty_gold and non_empty_pred:
            print(f"\n📝 첫 번째 쿼리 예시:")
            print(f"Gold: {non_empty_gold[0].strip()[:80]}...")
            print(f"Pred: {non_empty_pred[0].strip()[:80]}...")

        print("✅ 파일 검증 완료!")
        return True

    except Exception as e:
        print(f"❌ 검증 실패: {e}")
        return False


def main():
    """메인 실행 함수"""
    import argparse

    parser = argparse.ArgumentParser(description='multiturn_sessions.json을 gold.txt/predict.txt로 변환')
    parser.add_argument('--input', '-i', default='multiturn_sessions.json', help='입력 JSON 파일')
    parser.add_argument('--gold', '-g', default='gold.txt', help='정답 SQL 출력 파일')
    parser.add_argument('--predict', '-p', default='predict.txt', help='생성 SQL 출력 파일')
    parser.add_argument('--summary', '-s', action='store_true', help='세션 요약만 출력')
    parser.add_argument('--validate', '-v', action='store_true', help='생성된 파일 검증')

    args = parser.parse_args()

    if args.summary:
        show_session_summary(args.input)
        return

    if args.validate:
        validate_files(args.gold, args.predict)
        return

    # 세션 요약 먼저 출력
    show_session_summary(args.input)
    print()

    # 변환 실행
    success = extract_sqls_from_sessions(args.input, args.gold, args.predict)

    if success:
        print()
        validate_files(args.gold, args.predict)
        print()
        print("🎉 변환이 완료되었습니다!")
        print(f"📁 출력 파일: {args.gold}, {args.predict}")
        print()
        print("💡 다음 단계:")
        print(f"   python batch_evaluate_gpt.py --gold {args.gold} --pred {args.predict}")


if __name__ == "__main__":
    main()