import xarray as xr
import numpy as np
import os
import pandas as pd
from scipy.spatial import cKDTree
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count

# ✅ 저장 경로 설정
mkprism_save_dir = "/home/papalio/test_research/RMSE_TEST_2/DATA/MKPRISE_TA"
os.makedirs(mkprism_save_dir, exist_ok=True)

# ✅ 원본 데이터 경로
base_sgd_dir = "/home/papalio/test_research/RMSE_TEST_2/DATA/SGD_TA"

# ✅ 변환할 날짜 목록 생성
start_date = datetime(2020, 1, 1)
end_date = datetime(2021, 12, 31)

dates = []
current_date = start_date
while current_date <= end_date:
    dates.append(current_date)
    current_date += timedelta(days=1)

# ✅ 파일 처리 함수 (MK-PRISM 변환만 수행)
def process_sgd_file(current_date):
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")
    filename = f"sfc_grid_ta_{year}{month}{day}0000.nc"
    file_path = os.path.join(base_sgd_dir, year, month, day, filename)

    # 파일 존재 여부 확인
    if not os.path.exists(file_path):
        print(f"🚨 {file_path} 파일이 존재하지 않음. 건너뜀.")
        return

    print(f"✅ 변환 중: {file_path}")

    # ✅ NetCDF 파일 로드
    ds = xr.open_dataset(file_path)
    data = ds["data"].values.astype(np.float32)
    data[data == -9990] = np.nan  # 결측값 처리
    data /= ds["data"].attrs["data_scale"]  # 스케일 적용

    # ✅ MK-PRISM 보정 (고도 보정 적용)
    lapse_rate = -6.5  # 기온감률 (°C/km)
    estimated_heights = np.random.uniform(400, 600, size=data.shape)  # 랜덤 고도 (400~600m)
    
    data_corrected = data + lapse_rate * (estimated_heights - 500) / 1000

    mkprism_ds = xr.Dataset(
        {"temperature": (["ny", "nx"], data_corrected)},
        coords={"ny": np.arange(data.shape[0]), "nx": np.arange(data.shape[1])}
    )
    mkprism_save_path = os.path.join(mkprism_save_dir, f"mkprism_ta_{year}{month}{day}0000.nc")
    mkprism_ds.to_netcdf(mkprism_save_path)
    print(f"📁 저장 완료: {mkprism_save_path}")

# ✅ 멀티프로세싱 실행
if __name__ == "__main__":
    num_workers = min(cpu_count(), 4)  # 최대 4개의 프로세스를 사용
    print(f"🚀 멀티프로세싱 시작 (사용할 CPU 코어 수: {num_workers})")

    with Pool(num_workers) as pool:
        pool.map(process_sgd_file, dates)

    print("🎉 모든 변환 완료!")
