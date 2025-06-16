from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import pandas as pd
import time

# 33개 KRX 섹터 코드와 이름
sector_dict = {
    '101': '수산·농림업', '102': '광업', '103': '건설업', '104': '식품업', '105': '섬유·의복업',
    '106': '제지업', '107': '화학업', '108': '제약업', '109': '석유·석탄제품업', '110': '고무제품업',
    '111': '유리·도자기제품업', '112': '철강업', '113': '비철금속업', '114': '금속제품업', '115': '기계업',
    '116': '전기·전자업', '117': '운송장비업', '118': '정밀기기업', '119': '기타제품업', '120': '전기·가스업',
    '121': '육상운송업', '122': '해상운송업', '123': '항공운송업', '124': '창고·하역업', '125': '정보통신업',
    '126': '도매업', '127': '소매업', '128': '은행', '129': '증권 및 상품선물업', '130': '보험업',
    '131': '기타금융업', '132': '부동산업', '133': '서비스업'
}

# 크롬 드라이버 설정 (절대경로 사용)
service = Service('C:/chromedriver-win64/chromedriver.exe')
options = webdriver.ChromeOptions()
options.add_argument('--headless')  # 창 없이 실행
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(service=service, options=options)

# KRX 업종별 시총 페이지 접속
driver.get('https://kind.krx.co.kr/disclosure/industryIndInfo.do?method=searchIndustryIndMain')
time.sleep(2)

result = []

for code, name in sector_dict.items():
    try:
        # 드롭다운 선택
        select_box = driver.find_element(By.ID, 'industryTypeCode')
        select_box.click()
        time.sleep(0.5)
        driver.find_element(By.XPATH, f'//option[@value="{code}"]').click()
        time.sleep(1)

        # 검색 버튼 클릭
        driver.find_element(By.ID, 'btnSearch').click()
        time.sleep(2)

        # 테이블 가져오기
        table = driver.find_element(By.CLASS_NAME, 'type_5')
        rows = table.find_elements(By.TAG_NAME, 'tr')[1:11]  # 상위 10개만

        for i, row in enumerate(rows):
            cols = row.find_elements(By.TAG_NAME, 'td')
            result.append({
                '섹터코드': code,
                '섹터명': name,
                '순위': i + 1,
                '종목명': cols[0].text,
                '현재가': cols[1].text,
                '전일대비': cols[2].text,
                '등락률': cols[3].text,
                '거래량': cols[4].text,
                '시가총액': cols[5].text
            })

        print(f"[{name}] 완료 ✅")
    except Exception as e:
        print(f"[{name}] 섹터 오류 ❌:", e)

driver.quit()

# 엑셀로 저장
df = pd.DataFrame(result)
df.to_excel("KRX_섹터별_탑10_기업.xlsx", index=False)
print("✅ 모든 섹터 완료! 'KRX_섹터별_탑10_기업.xlsx' 저장됨")
