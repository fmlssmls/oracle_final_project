# column_manager.py
# 컬럼 관리 전담 모듈 - 기본 컬럼셋 강제 적용

import json
import os
from typing import Dict, List, Optional


class ColumnManager:
    def __init__(self, config_file="column_config.json", user_settings_file="user_column_settings.json"):
        self.config_file = config_file
        self.user_settings_file = user_settings_file
        self.base_columns = self._load_base_columns()
        self.user_settings = self._load_user_settings()

    def _load_base_columns(self) -> Dict:
        """기본 컬럼셋 정의 - 5개 카테고리별 필수 컬럼들"""
        return {
            "환자/입원": {
                "essential": [],
                "description": "환자 기본정보 + 입원정보 + ICU정보",
                "optional_groups": {
                    "환자 기본정보": ["dod", "anchor_year", "anchor_year_group"],
                    "입원정보": ["hadm_id", "admittime", "dischtime", "admission_type", "discharge_location", "insurance"],
                    "ICU정보": ["stay_id", "intime", "outtime", "los", "first_careunit", "last_careunit"],
                    "기타": ["language", "marital_status", "race", "hospital_expire_flag"]
                }
            },
            "검사/바이탈": {
                "essential": [],
                "description": "바이탈사인 + 검사결과 + 미생물검사",
                "optional_groups": {
                    "환자 정보": ["gender", "anchor_age"],
                    "검사 기본": ["label", "valuenum", "valueuom", "storetime"],
                    "미생물": ["test_name", "org_name", "ab_name", "interpretation"],
                    "기타": ["warning", "flag", "comments", "specimen_id"]
                }
            },
            "진단/시술": {
                "essential": [],
                "description": "진단정보 + 시술정보 + DRG정보",
                "optional_groups": {
                    "환자 정보": ["gender", "anchor_age"],
                    "진단명": ["short_title", "long_title"],
                    "DRG": ["drg_code", "drg_type", "description", "drg_severity", "drg_mortality"],
                    "기타": ["icd_version", "chartdate"]
                }
            },
            "약물/투약": {
                "essential": [],
                "description": "처방정보 + 투약기록 + 수액/투여",
                "optional_groups": {
                    "환자 정보": ["gender", "anchor_age"],
                    "처방 기본": ["endtime", "drug_type", "dose_val_rx", "dose_unit_rx"],
                    "투약 기록": ["medication", "charttime", "event_txt"],
                    "수액/투여": ["amount", "amountuom", "rate", "rateuom", "orderid"]
                }
            },
            "임상시험": {
                "essential": [],
                "description": "임상시험 포함/제외 기준 + AE/ADR",
                "optional_groups": {
                    "환자 기본": ["gender", "anchor_age", "admittime"],
                    "진단 관련": ["icd_code", "short_title"],
                    "약물 관련": ["drug", "starttime", "drug_type"],
                    "검사 관련": ["itemid", "value", "charttime"],
                    "임상시험 특화": ["inclusion_criteria", "exclusion_criteria", "ae_term", "severity"]
                }
            }
        }

    # column_manager.py의 _load_user_settings에 디버깅 추가
    def _load_user_settings(self) -> Dict:
        """사용자 설정 파일에서 로드"""
        try:
            with open(self.user_settings_file, 'r', encoding='utf-8') as f:
                settings = json.load(f)
                print(f"🔍 [COLUMN_DEBUG] 사용자 설정 로드 성공: {settings}")  # 추가
                return settings
        except FileNotFoundError:
            print(f"🔍 [COLUMN_DEBUG] 설정 파일 없음: {self.user_settings_file}")  # 추가
            return {}
        except:
            print(f"🔍 [COLUMN_DEBUG] 설정 파일 로드 실패")  # 추가
            pass
        return {}

    def save_user_settings(self, settings: Dict):
        """사용자 설정 저장"""
        try:
            with open(self.user_settings_file, 'w', encoding='utf-8') as f:
                json.dump(settings, f, ensure_ascii=False, indent=2)
            self.user_settings = settings
            print(f"🔍 [COLUMN_DEBUG] 메모리 업데이트 완료: {self.user_settings}")  # 디버깅
            return True
        except Exception as e:
            print(f"설정 저장 실패: {e}")
            return False

    def get_columns_for_intent(self, intent: str) -> Dict:
        """특정 intent에 대한 컬럼 정보 반환"""
        print(f"🔍 [COLUMN_DEBUG] Intent 요청: '{intent}'")  # 추가
        print(f"🔍 [COLUMN_DEBUG] 사용 가능한 intents: {list(self.base_columns.keys())}")  # 추가

        base_config = self.base_columns.get(intent, self.base_columns.get("환자/입원", {}))
        user_config = self.user_settings.get(intent, {})
        print(f"🔍 [COLUMN_DEBUG] User config for '{intent}': {user_config}")  # 추가

        result = {
            "essential": base_config.get("essential", []),
            "description": base_config.get("description", ""),
            "optional_groups": base_config.get("optional_groups", {}),
            "user_selected": user_config.get("selected_optional", [])
        }
        return result

    def get_all_intents(self) -> List[str]:
        """모든 intent 목록 반환"""
        return list(self.base_columns.keys())

    def generate_column_instruction(self, intent: str) -> str:
        """LLM에 전달할 컬럼 강제 지시문 생성"""
        self.user_settings = self._load_user_settings()

        column_info = self.get_columns_for_intent(intent)

        essential_cols = column_info["essential"]
        selected_optional = column_info["user_selected"]

        all_required_cols = essential_cols + selected_optional

        if not all_required_cols:
            return ""

        instruction = f"""

⚠️ 컬럼 포함 규칙:
다음 컬럼들을 반드시 SELECT절에 포함할 것:
{', '.join(all_required_cols)}

SELECT * 대신 위 컬럼들을 명시적으로 나열하세요.
이는 사용자가 설정한 기본 컬럼셋이므로 절대 누락하지 마세요.
        """

        return instruction


# 전역 컬럼 매니저 인스턴스
column_manager = ColumnManager()