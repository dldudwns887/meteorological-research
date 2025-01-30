import os
import glob
import pandas as pd
from datetime import datetime

# 사용자 정의 경로 설정
ROOT_DIRECTORY = "/home/papalio/test_research/python_edu/test_2024/test_2024/DATA"
OUTPUT_DIRECTORY = "/home/papalio/test_research/python_edu/test_2024/test_2024/RESULTS"

def scan_missing_dates(var: str, freq: str, root_directory: str) -> pd.DataFrame:
    """
    지정된 변수에 대해 결측 날짜를 스캔하는 함수.

    Args:
        var (str): 검사할 변수 (예: 'ta', 'rn_day').
        freq (str): 시간 빈도 ('hour' 또는 'day').
        root_directory (str): 데이터 경로.

    Returns:
        pd.DataFrame: 결측 날짜 정보를 포함한 DataFrame.
    """
    # 파일 탐색 패턴 설정
    file_pattern = f"**/sfc_grid_{var}_*.nc"
    search_path = os.path.join(root_directory, file_pattern)
    file_list = glob.glob(search_path, recursive=True)

    # 파일 이름에서 날짜 추출
    existing_dates = []
    for filepath in file_list:
        filename = os.path.basename(filepath)
        date_str = filename.split("_")[-1].split(".")[0]
        if len(date_str) == 12:
            existing_dates.append(date_str)

    # 날짜 리스트를 DataFrame으로 변환
    existing_dates = pd.to_datetime(existing_dates, format="%Y%m%d%H%M")

    # 주파수 변환: 'day' -> 'D', 'hour' -> 'H'
    freq = 'D' if freq.lower() == 'day' else 'H'

    # 전체 시간 범위 생성
    all_dates = pd.date_range(existing_dates.min(), existing_dates.max(), freq=freq)

    # 결측 날짜 계산
    missing_dates = all_dates.difference(existing_dates)
    df_missing = pd.DataFrame(missing_dates, columns=["missing_date"])
    df_missing["year"] = df_missing["missing_date"].dt.year
    df_missing["month"] = df_missing["missing_date"].dt.month
    df_missing["day"] = df_missing["missing_date"].dt.day

    return df_missing

def save_missing_dates(var: str, df_missing: pd.DataFrame) -> None:
    """
    결측 날짜 정보를 저장하는 함수.

    Args:
        var (str): 변수명.
        df_missing (pd.DataFrame): 결측 날짜 정보 DataFrame.
    """
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

    # 파일 경로 설정
    base_filename = f"missing_dates_{var}_{datetime.now().strftime('%Y%m%d%H%M')}"
    full_path = os.path.join(OUTPUT_DIRECTORY, f"{base_filename}.csv")

    # 결측 날짜 저장
    df_missing.to_csv(full_path, index=False)
    print(f"결측 날짜 목록이 저장되었습니다: {full_path}")

    # 월별 통계 저장
    monthly_stats = df_missing.groupby(["year", "month"]).size().unstack(fill_value=0)
    monthly_stats_path = os.path.join(OUTPUT_DIRECTORY, f"{base_filename}_monthly_stats.csv")
    monthly_stats.to_csv(monthly_stats_path)

    print(f"월별 결측 통계가 저장되었습니다: {monthly_stats_path}")

def main():
    """
    메인 실행 함수
    """
    print("\n=== 표준격자 데이터 결측 날짜 검사 프로그램 ===")
    print("사용 가능한 변수: ta (기온), rn_day (강수량), hm (습도), ws_10m (풍속)")

    # 사용자 입력
    var = input("검사할 변수를 입력하세요 (기본값: ta): ").strip() or "ta"
    freq = input("시간 빈도를 입력하세요 ('hour' 또는 'day', 기본값: 'hour'): ").strip() or "hour"

    if freq not in ["hour", "day"]:
        print("잘못된 시간 빈도입니다. 'hour' 또는 'day' 중 하나를 선택하세요.")
        return

    # 결측 날짜 스캔
    print(f"\n'{var}' 변수의 결측 날짜를 스캔합니다...")
    df_missing = scan_missing_dates(var, freq, ROOT_DIRECTORY)

    # 결과 저장
    if not df_missing.empty:
        save_missing_dates(var, df_missing)
        print(f"\n결측 날짜 수: {len(df_missing)}")
    else:
        print("\n결측 날짜가 없습니다. 모든 데이터가 정상적으로 존재합니다.")

if __name__ == "__main__":
    main()
