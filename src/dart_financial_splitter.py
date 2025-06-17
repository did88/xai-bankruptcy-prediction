import pandas as pd
import asyncio
import xml.etree.ElementTree as ET
from pathlib import Path

### 📌 1. CORPCODE.xml 파싱 함수 (corp_cls None 문제 해결됨)
def parse_corp_code_xml(xml_path: str) -> pd.DataFrame:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    data = []
    for child in root.findall("list"):
        corp_code = child.findtext("corp_code")
        corp_name = child.findtext("corp_name")
        stock_code = child.findtext("stock_code")
        modify_date = child.findtext("modify_date")

        # 🔧 수정: corp_cls가 None이면 빈 문자열이 아니라 None으로 처리
        corp_cls_elem = child.find("corp_cls")
        corp_cls = corp_cls_elem.text.strip() if corp_cls_elem is not None and corp_cls_elem.text else None

        data.append({
            "corp_code": corp_code,
            "corp_name": corp_name,
            "stock_code": stock_code,
            "modify_date": modify_date,
            "corp_cls": corp_cls
        })

    df = pd.DataFrame(data)
    return df


### 📌 2. KOSPI/KOSDAQ + 비금융 기업 필터링 함수 (진단 로그 포함)
def filter_kospi_kosdaq_non_financial(df: pd.DataFrame) -> pd.DataFrame:
    print("\n📊 [진단] corp_cls 값 분포:")
    print(df["corp_cls"].value_counts(dropna=False))

    print("\n📊 [진단] stock_code가 존재하는 기업 수:")
    print(df["stock_code"].notna().sum())

    financial_keywords = ["은행", "보험", "금융", "투자", "리츠"]
    mask_financial = df["corp_name"].str.contains("|".join(financial_keywords), na=False)
    print("\n📊 [진단] 금융 관련 키워드 포함 기업 수:")
    print(mask_financial.sum())

    mask_kospi_kosdaq = df["stock_code"].notna() & df["corp_cls"].isin(["Y", "K"])
    print("\n📊 [진단] KOSPI/KOSDAQ 조건 충족 기업 수:")
    print(mask_kospi_kosdaq.sum())

    final_df = df[mask_kospi_kosdaq & (~mask_financial)]
    return final_df


### 📌 3. 메인 실행 함수
async def main():
    print("📥 기업 코드 XML 파싱 중...")
    xml_path = Path(__file__).parent / "CORPCODE.xml"
    corp_df = parse_corp_code_xml(str(xml_path))
    print(f"✅ 총 기업 수: {len(corp_df)}")

    filtered_df = filter_kospi_kosdaq_non_financial(corp_df)
    print(f"\n✅ KOSPI/KOSDAQ 비금융 기업 수: {len(filtered_df)}")

    # 저장
    filtered_df.to_csv("filtered_corp_list.csv", index=False, encoding="utf-8-sig")
    print("📄 필터링 결과 saved to 'filtered_corp_list.csv'")


### 📌 4. 실행
if __name__ == "__main__":
    asyncio.run(main())
