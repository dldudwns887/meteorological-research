import requests
import datetime
import os
import pandas as pd
from tqdm import tqdm
import time
from multiprocessing import Pool, cpu_count
import sys
import os

# ✅ data_api.py가 있는 경로 추가
sys.path.append("/home/papalio/test_research/python_edu/test_2024/test_2024/DATA")

# ✅ data_api.py에서 key 변수 가져오기
from data_api import key2
print(f"✅ 가져온 API Key: {key2}")

def download_file(args):
    """파일 다운로드 함수"""
    url, save_path, min_file_size = args
    try:
        if os.path.exists(save_path) and os.path.getsize(save_path) >= min_file_size:
            print(f"파일 존재, 스킵: {save_path}")
            return None
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, 'wb') as f:
            f.write(response.content)
        
        file_size = os.path.getsize(save_path)
        if file_size < min_file_size:
            raise Exception("File size too small")
            
        print(f"다운로드 완료: {os.path.basename(save_path)} ({file_size} bytes)")
        return None
    except Exception as e:
        print(f"다운로드 실패: {os.path.basename(save_path)} - {str(e)}")
        return (url, save_path)  # 실패한 다운로드 목록 저장용

def get_time_range(start_date, end_date, freq):
    """주어진 빈도에 따라 시간 범위 생성"""
    dates = []
    
    if freq == 'hour':
        # 시작일 01시부터
        current = start_date.replace(hour=1)
        
        # 종료일 다음날 00시까지
        end_next_day = (end_date + datetime.timedelta(days=1)).replace(hour=0)
        
        while current <= end_next_day:
            dates.append(current)
            current += datetime.timedelta(hours=1)
    else:  # freq == 'day'
        current = start_date
        while current <= end_date:
            dates.append(current.replace(hour=0))
            current += datetime.timedelta(days=1)
            
    return dates

def scan_files(start_date, end_date, var, freq, base_dir, min_file_size):
    """다운로드 필요한 파일 목록 스캔"""
    download_queue = []
    
    # 시간 범위 생성
    time_range = get_time_range(start_date, end_date, freq)
    
    for current_date in tqdm(time_range, desc="파일 스캔 중"):
        # 파일명 생성
        if freq == 'hour':
            date_str = current_date.strftime("%Y%m%d")
            hour_str = f"{current_date.hour:02d}"
            filename = f'sfc_grid_{var}_{date_str}{hour_str}00.nc'
        else:  # freq == 'day'
            date_str = current_date.strftime("%Y%m%d")
            filename = f'sfc_grid_{var}_{date_str}0000.nc'
        
        year = current_date.year
        month = current_date.month
        day = current_date.day
        save_file_path = f'{base_dir}/org/sgd/{year}/{month:02d}/{day:02d}/{filename}'
        
        # 파일이 없거나 크기가 최소 크기보다 작으면 다운로드 대상에 추가
        if not os.path.exists(save_file_path) or os.path.getsize(save_file_path) < min_file_size:
            download_queue.append((date_str, save_file_path))
    
    return download_queue

def main():
    # 기본 설정
    base_dir = "/home/papalio/test_research/python_edu/test_2024/test_2024/DATA"
    key = key2
    var = "rn_day"  # 변수명 (예: "rn_day", "rn_hr" 등)
    freq = "day"    # 'hour' 또는 'day'
    
    # 시작일과 종료일 설정
    start_date = datetime.datetime(2020, 1, 1)
    end_date = datetime.datetime(2021, 12, 31)
    
    min_file_size = 47 * 1024  # 47KB
    
    # 다운로드 필요한 파일 목록 스캔
    print(f"{freq} 단위 다운로드 대상 파일 스캔 시작...")
    download_queue = scan_files(start_date, end_date, var, freq, base_dir, min_file_size)
    print(f"다운로드 대상 파일 수: {len(download_queue)}")

    # 다운로드 실행 (멀티프로세싱 적용)
    failed_downloads = []
    download_tasks = []
    
    for date_str, save_path in download_queue:
        if freq == 'hour':
            date_format = f"{date_str}H00"
        else:  # freq == 'day'
            date_format = f"{date_str}0000"
        
        url = f'https://apihub.kma.go.kr/api/typ01/url/sfc_grid_nc_down.php?obs={var}&tm={date_format}&authKey={key}'
        download_tasks.append((url, save_path, min_file_size))
    
    num_workers = min(4, cpu_count())  # 최대 4개 코어 사용
    print(f"멀티프로세싱을 사용하여 {num_workers}개의 코어로 다운로드 진행...")

    try:
        with Pool(processes=num_workers) as pool:
            results = list(tqdm(pool.imap(download_file, download_tasks), total=len(download_tasks), desc="파일 다운로드 중"))
        
        # 실패한 다운로드 수집
        failed_downloads = [res for res in results if res is not None]

    except KeyboardInterrupt:
        print("\n프로그램이 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"예상치 못한 오류 발생: {e}")
    finally:
        if failed_downloads:
            df_failed = pd.DataFrame(failed_downloads, columns=["url", "save_path"])
            failed_log_path = f'{base_dir}/failed_downloads.csv'
            os.makedirs(os.path.dirname(failed_log_path), exist_ok=True)
            df_failed.to_csv(failed_log_path, index=False)
            print(f"{len(failed_downloads)}개 파일 다운로드 실패. 목록이 'failed_downloads.csv'에 저장되었습니다.")

if __name__ == "__main__":
    main()
