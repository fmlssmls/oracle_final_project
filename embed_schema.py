"""
embed_schema_hybrid.py - 하이브리드 청킹 방식
기존 스키마 + 상세 스키마 통합 처리

주요 특징:
1. 기존 스키마: 테이블별 독립 청크 + FAQ 청크
2. 상세 스키마: 헤더 기반 의미 단위 청크
3. 관계정보 청크 (복잡한 JOIN 처리)
4. 도메인별 메타데이터 (의료 도메인 특성 반영)
"""

from langchain_huggingface import HuggingFaceEmbeddings
from langchain_chroma import Chroma
from langchain_core.documents import Document
import re
import os
import json
from datetime import datetime


class HybridSchemaChunker:
    """하이브리드 청킹을 위한 메인 클래스"""

    def __init__(self):
        # 모든 스키마 파일 정의 (기존 + 상세)
        self.schema_files = [
            # 기존 스키마 파일들
            'schema_patients.txt',
            'schema_diagproc.txt',
            'schema_drugs.txt',
            'schema_events.txt',
            'schema_trial.txt',
            # 새로운 상세 스키마 파일들
            'schema_patients_detailed.txt',
            'schema_events_detailed.txt',
            'schema_diagproc_detailed.txt',
            'schema_drugs_detailed.txt',
            'schema_trial_detailed.txt'
        ]

        # 도메인별 분류 (의료 워크플로우 순서)
        self.domain_map = {
            'patients': {'domain': '환자기본정보', 'priority': 1, 'keywords': ['환자', '나이', '성별', '입원', '퇴원']},
            'diagproc': {'domain': '진단시술', 'priority': 2, 'keywords': ['진단', '질병', 'ICD', '시술', '수술']},
            'drugs': {'domain': '약물치료', 'priority': 3, 'keywords': ['약물', '처방', '투약', '용량', '항생제']},
            'events': {'domain': '임상이벤트', 'priority': 4, 'keywords': ['검사', '수치', '측정', '모니터링']},
            'trial': {'domain': '임상시험', 'priority': 5, 'keywords': ['시험', '연구', '임상', '치료효과']}
        }

        self.all_chunks = []  # 모든 청크를 저장할 리스트

    def extract_table_info(self, content, source_file):
        """기존 스키마: 테이블 정보를 추출하여 개별 청크로 생성"""
        chunks = []

        # 실제 파일 구조에 맞게 수정: 대문자 테이블명만 단독으로 있는 패턴
        table_sections = re.split(r'\n(?=[A-Z_]+\n)', content)

        for section in table_sections:
            section = section.strip()
            if len(section) < 50:  # 너무 짧은 섹션 제외
                continue

            # 테이블명 추출: 첫 번째 줄에서 대문자 단어 찾기
            lines = section.split('\n')
            table_name = None

            for line in lines:
                line = line.strip()
                # 대문자로만 이루어지고, |가 없고, #으로 시작하지 않는 단독 단어
                if (line.isupper() and
                    not '|' in line and
                    not line.startswith('#') and
                    not line.startswith('[') and
                    len(line.split()) == 1 and
                    (line.isalpha() or '_' in line)):
                    table_name = line.lower()
                    break

            if not table_name:
                continue

            # 도메인 정보 가져오기
            file_key = source_file.replace('schema_', '').replace('.txt', '').replace('_detailed', '')
            domain_info = self.domain_map.get(file_key, {})

            # 컬럼 정보 정리 (| 구분자 사용)
            columns = []
            for line in lines:
                if '|' in line and not line.strip().startswith('#'):
                    col_parts = line.split('|')
                    if len(col_parts) >= 2:
                        col_name = col_parts[0].strip()
                        col_desc = col_parts[1].strip()
                        # 빈 값이 아니고 테이블명이 아닌 경우만 추가
                        if col_name and col_desc and col_name != table_name.upper():
                            columns.append(f"{col_name}: {col_desc}")

            # 컬럼이 없으면 스킵
            if not columns:
                continue

            # 테이블 청크 생성
            table_content = f"테이블: {table_name.upper()}\n"
            table_content += f"도메인: {domain_info.get('domain', '기타')}\n"
            table_content += f"컬럼 정보:\n" + "\n".join(columns[:15])  # 최대 15개 컬럼

            chunks.append(Document(
                page_content=table_content,
                metadata={
                    "type": "table_schema",  # 청크 타입
                    "source": source_file,
                    "table_name": table_name,
                    "domain": domain_info.get('domain', '기타'),
                    "priority": domain_info.get('priority', 99),
                    "keywords": ", ".join(domain_info.get('keywords', [])),
                    "column_count": len(columns)
                }
            ))

        return chunks

    def extract_faq_info(self, content, source_file):
        """기존 스키마: FAQ 정보를 추출하여 개별 청크로 생성"""
        chunks = []

        # Q&A 패턴 매칭
        qa_pairs = re.findall(r"Q[:：]\s*(.*?)\nA[:：]\s*(.*?)(?=\n(?:Q[:：]|#|\d+\.|\Z))", content, re.DOTALL)

        file_key = source_file.replace('schema_', '').replace('.txt', '').replace('_detailed', '')
        domain_info = self.domain_map.get(file_key, {})

        for i, (question, answer) in enumerate(qa_pairs):
            q_clean = question.strip()
            a_clean = answer.strip()

            # 의미있는 Q&A만 선별
            if len(q_clean) < 5 or len(a_clean) < 10:
                continue

            # FAQ 청크 생성
            faq_content = f"Q: {q_clean}\nA: {a_clean}"

            # SQL이 포함된 답변인지 확인
            has_sql = any(keyword in a_clean.upper() for keyword in ['SELECT', 'FROM', 'WHERE', 'JOIN'])

            chunks.append(Document(
                page_content=faq_content,
                metadata={
                    "type": "table_faq",  # 청크 타입
                    "source": source_file,
                    "domain": domain_info.get('domain', '기타'),
                    "priority": domain_info.get('priority', 99),
                    "keywords": ", ".join(domain_info.get('keywords', [])),
                    "has_sql": has_sql,
                    "faq_id": i + 1
                }
            ))

        return chunks

    def extract_detailed_chunks(self, content, source_file):
        """상세 스키마: 헤더 기반 의미 단위로 청크 생성"""
        chunks = []

        # 파일 키와 도메인 정보 추출
        file_key = source_file.replace('schema_', '').replace('_detailed.txt', '')
        domain_info = self.domain_map.get(file_key, {})

        # 1. 메인 섹션별로 분할 (## 헤더 기준)
        main_sections = re.split(r'\n## ', content)

        for main_section in main_sections:
            main_section = main_section.strip()
            if len(main_section) < 100:
                continue

            # 메인 섹션 제목 추출
            section_title = main_section.split('\n')[0].strip()

            # 2. 하위 섹션으로 분할 (### 헤더 기준)
            sub_sections = re.split(r'\n### ', main_section)

            if len(sub_sections) <= 1:
                # 하위 섹션이 없으면 메인 섹션을 그대로 청크로 생성
                chunks.append(Document(
                    page_content=main_section,
                    metadata={
                        "type": "detailed_section",
                        "source": source_file,
                        "section_title": section_title,
                        "domain": domain_info.get('domain', '기타'),
                        "priority": domain_info.get('priority', 99),
                        "keywords": ", ".join(domain_info.get('keywords', []))
                    }
                ))
            else:
                # 하위 섹션이 있으면 각각을 청크로 생성
                for sub_section in sub_sections:
                    sub_section = sub_section.strip()
                    if len(sub_section) < 50:
                        continue

                    # 하위 섹션 제목 추출 (테이블명 등)
                    sub_title = sub_section.split('\n')[0].strip()

                    # 테이블명 추출 시도
                    table_match = re.search(r'^([A-Z_]+)', sub_title)
                    table_name = table_match.group(1).lower() if table_match else "unknown"

                    # 3. 큰 섹션은 더 세분화 (**bold** 헤더 기준)
                    if len(sub_section) > 2000:
                        detail_parts = re.split(r'\n\*\*(.+?)\*\*', sub_section)

                        for i, part in enumerate(detail_parts):
                            part = part.strip()
                            if len(part) < 100:
                                continue

                            # 짝수 인덱스는 제목, 홀수 인덱스는 내용
                            part_type = "detail_header" if i % 2 == 1 else "detail_content"

                            chunks.append(Document(
                                page_content=part,
                                metadata={
                                    "type": "detailed_subsection",
                                    "source": source_file,
                                    "section_title": section_title,
                                    "sub_title": sub_title,
                                    "table_name": table_name,
                                    "part_type": part_type,
                                    "domain": domain_info.get('domain', '기타'),
                                    "priority": domain_info.get('priority', 99),
                                    "keywords": ", ".join(domain_info.get('keywords', []))
                                }
                            ))
                    else:
                        # 작은 섹션은 그대로 청크로 생성
                        chunks.append(Document(
                            page_content=sub_section,
                            metadata={
                                "type": "detailed_table",
                                "source": source_file,
                                "section_title": section_title,
                                "sub_title": sub_title,
                                "table_name": table_name,
                                "domain": domain_info.get('domain', '기타'),
                                "priority": domain_info.get('priority', 99),
                                "keywords": ", ".join(domain_info.get('keywords', []))
                            }
                        ))

        return chunks

    def create_relationship_chunks(self):
        """테이블 간 관계 정보를 청크로 생성"""
        chunks = []

        # MIMIC-IV 기반 주요 관계 정의
        relationships = {
            "기본연결": {
                "content": """주요 테이블 연결 관계:
- PATIENTS.SUBJECT_ID ← 모든 테이블의 기본 키
- ADMISSIONS.HADM_ID ← 입원 관련 테이블 연결
- ICUSTAYS.STAY_ID ← ICU 관련 테이블 연결
- 환자 → 입원 → 진단/약물/이벤트 순서로 연결""",
                "keywords": ["연결", "관계", "조인", "키"]
            },
            "진단관계": {
                "content": """진단 관련 테이블 연결:
- DIAGNOSES_ICD ↔ D_ICD_DIAGNOSES (ICD_CODE로 연결)
- PROCEDURES_ICD ↔ D_ICD_PROCEDURES (ICD_CODE로 연결)
- 진단코드와 진단명을 매칭할 때 사용""",
                "keywords": ["진단", "ICD", "질병코드"]
            },
            "약물관계": {
                "content": """약물 관련 테이블 연결:
- PRESCRIPTIONS ↔ D_ITEMS (ITEMID로 연결)
- INPUTEVENTS ↔ D_ITEMS (ITEMID로 연결)
- 약물코드와 약물명을 매칭할 때 사용""",
                "keywords": ["약물", "처방", "투약", "ITEMID"]
            },
            "검사관계": {
                "content": """검사 관련 테이블 연결:
- LABEVENTS ↔ D_LABITEMS (ITEMID로 연결)
- CHARTEVENTS ↔ D_ITEMS (ITEMID로 연결)
- 검사코드와 검사명을 매칭할 때 사용""",
                "keywords": ["검사", "측정", "ITEMID", "결과"]
            }
        }

        for rel_name, rel_info in relationships.items():
            chunks.append(Document(
                page_content=rel_info["content"],
                metadata={
                    "type": "relationship",  # 청크 타입
                    "source": "system_generated",
                    "relationship_name": rel_name,
                    "keywords": ", ".join(rel_info["keywords"]),
                    "priority": 1  # 관계 정보는 높은 우선순위
                }
            ))

        return chunks

    def create_domain_guide_chunks(self):
        """도메인별 분석 가이드 청크 생성"""
        chunks = []

        domain_guides = {
            "환자분석가이드": {
                "content": """환자 정보 분석 시 주요 포인트:
- 나이대별 분석: ANCHOR_AGE 컬럼 활용
- 성별별 분석: GENDER 컬럼 활용  
- 입원기간 분석: ADMITTIME, DISCHTIME 활용
- 사망 여부: DOD (Date of Death) 확인
- ICU 체류: ICUSTAYS 테이블과 조인""",
                "keywords": ["환자", "나이", "성별", "입원", "사망"]
            },
            "임상분석가이드": {
                "content": """임상 데이터 분석 시 주의사항:
- 시간순 분석: CHARTTIME 기준 정렬
- 정상범위 확인: VALUENUM과 REF_RANGE 비교
- 결측값 처리: NULL 값이 많은 항목 주의
- 중복 측정: 같은 시간대 중복 측정값 확인
- 단위 통일: VALUEUOM 확인 필수""",
                "keywords": ["임상", "측정", "시간", "정상범위"]
            },
            "JOIN가이드": {
                "content": """효과적인 JOIN 사용법:
- 환자 기본정보: PATIENTS 테이블을 중심으로
- 입원별 분석: ADMISSIONS 테이블과 조인
- ICU 분석: ICUSTAYS 테이블 필수
- 진단 정보: D_ICD_DIAGNOSES와 조인으로 진단명 확인
- 약물 정보: D_ITEMS와 조인으로 약물명 확인""",
                "keywords": ["JOIN", "조인", "연결", "테이블"]
            }
        }

        for guide_name, guide_info in domain_guides.items():
            chunks.append(Document(
                page_content=guide_info["content"],
                metadata={
                    "type": "domain_guide",  # 청크 타입
                    "source": "system_generated",
                    "guide_name": guide_name,
                    "keywords": ", ".join(guide_info["keywords"]),
                    "priority": 2  # 가이드는 중간 우선순위
                }
            ))

        return chunks

    def process_file(self, filename):
        """파일 타입에 따라 다른 처리 방식 적용"""
        if not os.path.exists(filename):
            print(f"   ❌ 파일 없음: {filename}")
            return []

        print(f"\n📂 처리 중: {filename}")

        try:
            with open(filename, encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            print(f"   ❌ 읽기 실패: {e}")
            return []

        chunks = []

        if 'detailed' in filename:
            # 상세 스키마: 헤더 기반 분할
            detail_chunks = self.extract_detailed_chunks(content, filename)
            chunks.extend(detail_chunks)
            print(f"   📖 상세 청크: {len(detail_chunks)}개")
        else:
            # 기존 스키마: 기존 방식
            table_chunks = self.extract_table_info(content, filename)
            faq_chunks = self.extract_faq_info(content, filename)
            chunks.extend(table_chunks)
            chunks.extend(faq_chunks)
            print(f"   📋 테이블 청크: {len(table_chunks)}개")
            print(f"   ❓ FAQ 청크: {len(faq_chunks)}개")

        return chunks

    def process_all_files(self):
        """모든 파일을 처리하여 하이브리드 청크 생성"""
        print("🔄 하이브리드 청킹 시작...")

        total_chunks = 0

        # 1. 각 스키마 파일 처리
        for filename in self.schema_files:
            file_chunks = self.process_file(filename)
            self.all_chunks.extend(file_chunks)
            total_chunks += len(file_chunks)

        # 2. 관계 정보 청크 생성
        rel_chunks = self.create_relationship_chunks()
        self.all_chunks.extend(rel_chunks)
        print(f"\n🔗 관계 청크: {len(rel_chunks)}개")

        # 3. 도메인 가이드 청크 생성
        guide_chunks = self.create_domain_guide_chunks()
        self.all_chunks.extend(guide_chunks)
        print(f"📖 가이드 청크: {len(guide_chunks)}개")

        total_chunks += len(rel_chunks) + len(guide_chunks)

        print(f"\n✅ 총 {total_chunks}개 청크 생성 완료")
        return self.all_chunks

    def create_vectordb(self, chunks):
        """벡터 DB 생성"""
        print("\n🤖 임베딩 모델 로딩...")

        # 임베딩 모델 설정 (멀티턴 평가에 최적화)
        embedding = HuggingFaceEmbeddings(
            model_name="intfloat/multilingual-e5-large",
            model_kwargs={'device': 'cpu'},
            encode_kwargs={'normalize_embeddings': True}
        )

        print("💾 벡터 DB 생성 중...")

        # 기존 DB 삭제 후 새로 생성
        import shutil
        if os.path.exists("chroma_db"):
            shutil.rmtree("chroma_db")

        # Chroma DB 생성 (하이브리드 검색 지원)
        vectordb = Chroma.from_documents(
            chunks,
            embedding,
            persist_directory="./chroma_db",
            collection_metadata={"hnsw:space": "cosine"}
        )

        print("✅ 벡터 DB 생성 완료!")
        return vectordb

    def generate_stats(self, chunks):
        """통계 정보 생성"""
        stats = {
            "created_at": datetime.now().isoformat(),
            "total_chunks": len(chunks),
            "chunk_types": {},
            "domains": {},
            "files_processed": list(self.schema_files)
        }

        # 타입별/도메인별 통계
        for chunk in chunks:
            chunk_type = chunk.metadata.get('type', 'unknown')
            domain = chunk.metadata.get('domain', 'unknown')

            stats["chunk_types"][chunk_type] = stats["chunk_types"].get(chunk_type, 0) + 1
            stats["domains"][domain] = stats["domains"].get(domain, 0) + 1

        # 통계 파일 저장
        with open("hybrid_chunking_stats.json", "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)

        return stats

    def test_hybrid_search(self, vectordb):
        """하이브리드 검색 테스트"""
        print("\n🔍 하이브리드 검색 테스트:")

        test_queries = [
            "환자 나이 정보",  # 환자 도메인
            "혈압 측정 데이터",  # 이벤트 도메인
            "항생제 처방",  # 약물 도메인
            "테이블 연결 방법",  # 관계 정보
            "ADMISSIONS 테이블 구조"  # 상세 정보
        ]

        for query in test_queries:
            print(f"\n질문: '{query}'")
            results = vectordb.similarity_search(query, k=3)

            for i, doc in enumerate(results):
                chunk_type = doc.metadata.get('type', 'unknown')
                domain = doc.metadata.get('domain', 'unknown')
                source = doc.metadata.get('source', 'unknown')

                preview = doc.page_content[:80].replace('\n', ' ')
                print(f"  {i+1}. [{chunk_type}|{domain}] {preview}...")


def create_hybrid_embeddings():
    """메인 실행 함수"""
    print("🚀 하이브리드 청킹 임베딩 생성 시작")

    # 청킹 객체 생성
    chunker = HybridSchemaChunker()

    # 모든 청크 생성
    all_chunks = chunker.process_all_files()

    if not all_chunks:
        print("❌ 생성된 청크가 없습니다.")
        return None

    # 벡터 DB 생성
    vectordb = chunker.create_vectordb(all_chunks)

    # 통계 생성
    stats = chunker.generate_stats(all_chunks)
    print(f"\n📊 통계 정보:")
    print(f"   └ 총 청크: {stats['total_chunks']}개")
    for chunk_type, count in stats['chunk_types'].items():
        print(f"   └ {chunk_type}: {count}개")

    # 검색 테스트
    chunker.test_hybrid_search(vectordb)

    print("\n🎉 하이브리드 청킹 완료!")
    return vectordb


if __name__ == "__main__":
    # 하이브리드 임베딩 생성 실행
    vectordb = create_hybrid_embeddings()