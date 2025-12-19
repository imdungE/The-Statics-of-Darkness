import os
import chardet
import pandas as pd
import re
import glob
from typing import List, Dict, Any

TARGET_FOLDER = "./생기부_censored_txt/일반고/공립일반고_창체" # 작업 폴더 경로
SOURCE_EXTENSION = "*.txt" # 읽어올 원본 파일 확장자
OUTPUT_CSV_NAME = "공립일반고_테스트.csv" # 저장될 통합 CSV 파일명

# 분석 키워드
keywords = [
    "진로", "전공", "직업", "탐색", "융합", "심화", "부스", "한마당", 
    "워크숍", "체험교실", "특강", "강좌", "세미나", "프로그램", 
    "공모전", "소논문", "발표회", "보고서", "연구", "설계", "프로젝트"
]
pattern = "(" + "|".join(keywords) + ")"

def process_txt_file(filepath: str, pattern: str) -> Dict[str, Any]:
    file_name = os.path.basename(filepath)
    result = {"파일명": file_name, "키워드 등장 수": 0, "전체 단어 개수": 0, "Status": "Fail"}

    try:
        with open(filepath, "rb") as f:
            raw_data = f.read(100000)
            enc = chardet.detect(raw_data)["encoding"]
        
        content = ""
        for e in [enc, "utf-8", "cp949", "utf-8-sig"]:
            try:
                with open(filepath, "r", encoding=e) as f:
                    content = f.read()
                break
            except: continue
        
        if content:
            # 1. 전체 단어 개수 계산
            total_words = len(content.split())
            
            # 2. 키워드 등장 수 계산
            all_matches = re.findall(pattern, content)
            keyword_count = len(all_matches)
            
            result.update({
                "키워드 등장 수": keyword_count,
                "전체 단어 개수": total_words,
                "Status": "Success"
            })
            
    except Exception as e:
        print(f"오류 발생 ({file_name}): {e}")
        
    return result


print(f"===== TXT 파일 분석 및 {OUTPUT_CSV_NAME} 저장 시작 =====")
all_filepaths = glob.glob(os.path.join(TARGET_FOLDER, SOURCE_EXTENSION))

# 파일명에서 이 단어를 포함한 것만 선택
filepaths = [fp for fp in all_filepaths if "정시" in os.path.basename(fp)]

analysis_data = []

if not filepaths:
    print("❌ 파일명에 포함된 TXT 파일을 찾을 수 없습니다.")
else:
    print(f"🔍 총 {len(filepaths)}개의 파일을 찾았습니다.")
    
    for fp in filepaths:
        if OUTPUT_CSV_NAME in fp: continue 
        
        res = process_txt_file(fp, pattern)
        if res["Status"] == "Success":
            analysis_data.append({
                "파일명": res["파일명"],
                "키워드 등장 수": res["키워드 등장 수"],
                "전체 단어 개수": res["전체 단어 개수"]
            })


if analysis_data:
    final_df = pd.DataFrame(analysis_data)
    final_df.to_csv(OUTPUT_CSV_NAME, index=False, encoding="utf-8-sig")

    print("\n" + "="*50)
    print(f"✅ 분석 및 통합 저장 완료!")
    print(f"- 생성된 파일: {OUTPUT_CSV_NAME}")
    print(f"- 처리된 파일 수: {len(analysis_data)}개")
    print("="*50)
    print(final_df.head())
else:
    print("❌ 분석할 TXT 파일이 없거나 처리에 실패했습니다.")