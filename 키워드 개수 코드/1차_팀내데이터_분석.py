import os
import chardet
import pandas as pd
import re
from typing import List, Dict, Any

# 1. 파일 경로 목록 정의
filepaths = [
    "위아준생기부OCR_창의적활동본.csv",
    "성홍경생기부OCR_창의적활동본.csv",
    "최현준생기부OCR_창의적활동본.csv",
    "김동하생기부OCR_창의적활동본.csv"
]

# 2. 분석에 사용할 키워드 정의
keywords = [
    "진로", "전공", "직업", "탐색", "융합", "심화", "부스", "한마당", 
    "워크숍", "체험교실", "특강", "강좌", "세미나", "프로그램", 
    "공모전", "소논문", "발표회", "보고서", "연구", "설계", "프로젝트"
]

# 정규식 패턴 생성
pattern = "(" + "|".join(keywords) + ")"

# 3. 파일 처리 함수 정의
def process_file(filepath: str, pattern: str) -> Dict[str, Any]:
    file_name = os.path.basename(filepath)
    result = {"File": file_name, "Status": "Processing", "Total Keywords": 0, "Total Word Count": 0, "Matched Examples": []}

    # 3.1 경로 체크
    if not os.path.exists(filepath):
        result["Status"] = "❌ 파일 없음"
        return result

    # 3.2 인코딩 자동 감지
    enc = None
    try:
        with open(filepath, "rb") as f:
            raw = f.read(200000)
            detected = chardet.detect(raw)
            enc = detected["encoding"]
    except Exception:
        result["Status"] = "❌ 인코딩 감지 실패"
        return result
    
    print(f"\n--- {file_name} 분석 시작 ---")
    print(f"감지된 인코딩: {enc}")

    # 3.3 CSV 로딩 시도
    df = None
    encodings = [enc, "utf-8", "utf-8-sig", "cp949", "euc-kr"]
    for e in encodings:
        try:
            df = pd.read_csv(filepath, encoding=e)
            print(f"CSV 로딩 성공! 사용 인코딩 = {e}")
            break
        except Exception:
            continue
    
    if df is None:
        result["Status"] = "❌ CSV 로딩 실패"
        return result

    # 3.4 텍스트 컬럼 통합
    text_columns = df.select_dtypes(include=["object"]).columns
    if len(text_columns) == 0:
        result["Status"] = "❌ 분석할 텍스트 컬럼 없음"
        return result
        
    df["__merged_text__"] = df[text_columns].fillna("").astype(str).agg(" ".join, axis=1)

    # --- 총 단어 수 계산 ---
    full_text = " ".join(df["__merged_text__"].astype(str))
    # 공백 기준으로 단어를 분리하여 개수를 셉니다.
    total_word_count = len(full_text.split())
    # -----------------------

    # 3.5 키워드 매칭된 활동 총 개수 추출
    all_matched_keywords = []
    
    for text in df["__merged_text__"]:
        matches = re.findall(pattern, text)
        all_matched_keywords.extend(matches)

    total_keywords = len(all_matched_keywords)
    
    all_unique_keyword_types = list(set(all_matched_keywords))
    
    result["Status"] = "✅ 성공"
    result["Total Keywords"] = total_keywords
    result["Total Word Count"] = total_word_count # 새로운 측정 항목 추가
    result["Matched Examples"] = all_unique_keyword_types[:5]
    
    return result

# 4. 루프 실행 및 결과 출력
print("===== 생기부 활동 키워드 분석 시작 =====")
summary_results = []
grand_total_keywords = 0
grand_total_words = 0 # 전체 단어 총합을 위한 변수 추가

for fp in filepaths:
    summary = process_file(fp, pattern)
    summary_results.append(summary)
    
    # 성공적으로 분석된 경우에만 전체 합산에 추가
    if summary["Status"] == "✅ 성공":
        grand_total_keywords += summary["Total Keywords"]
        grand_total_words += summary["Total Word Count"]


# 5. 최종 요약 출력
print("\n" + "="*90)
print("===== 파일별 최종 요약 결과 =====")
print("="*90)
# 출력 포맷 수정: '총 단어 수' 컬럼 추가
print("{:<30} {:<10} {:<15} {:<15} {:<}".format("파일명", "상태", "총 키워드 수", "총 단어 수", "매칭 키워드 예시 (최대 5개)"))
print("-" * 90)

for res in summary_results:
    samples = ', '.join(res["Matched Examples"])
    if res["Status"] == "✅ 성공":
        print("{:<30} {:<10} {:<15} {:<15} {:<}".format(
            res["File"], res["Status"], res["Total Keywords"], res["Total Word Count"], samples
        ))
    else:
        # 실패 시 N/A 처리 (총 단어 수도 N/A)
        print("{:<30} {:<10} {:<15} {:<15} {:<}".format(
            res["File"], res["Status"], "N/A", "N/A", res["Status"]
        ))
print("-" * 90)

# 최종 전체 총합 출력
print(f"🔥 모든 파일의 키워드 총합 (전체 발생 횟수): {grand_total_keywords} 개")
print(f"🔥 모든 파일의 데이터 총 단어 수 (전체 합산): {grand_total_words} 단어")