import xarray as xr
import numpy as np
import os
import pandas as pd
from scipy.spatial import cKDTree
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count

# ✅ 저장 경로 설정
obs_save_dir = "/home/papalio/test_research/RMSE_TEST_2/DATA/OBS_TA"
mkprism_save_dir = "/home/papalio/test_research/RMSE_TEST_2/DATA/MKPRISE_TA"

os.makedirs(obs_save_dir, exist_ok=True)
os.makedirs(mkprism_save_dir, exist_ok=True)

# ✅ 원본 데이터 경로
base_sgd_dir = "/home/papalio/test_research/python_edu/test_2024/test_2024/DATA/org/sgd"

# ✅ 가상의 시도 중심 좌표 (17개 시도)
stations = {
    "서울": (37.5665, 126.9780),
    "부산": (35.1796, 129.0756),
    "대구": (35.8714, 128.6014),
    "인천": (37.4563, 126.7052),
    "광주": (35.1595, 126.8526),
    "대전": (36.3504, 127.3845),
    "울산": (35.5384, 129.3114),
    "세종": (36.4802, 127.2890),
    "경기": (37.4138, 127.5183),
    "강원": (37.8228, 128.1555),
    "충북": (36.6357, 127.4912),
    "충남": (36.6588, 126.6728),
    "전북": (35.7175, 127.1530),
    "전남": (34.8679, 126.9910),
    "경북": (36.5760, 128.5056),
    "경남": (35.4606, 128.2132),
    "제주": (33.4996, 126.5312)
}

# ✅ 변환할 날짜 목록 생성
start_date = datetime(2020, 1, 1)
end_date = datetime(2021, 12, 31)

dates = []
current_date = start_date
while current_date <= end_date:
    dates.append(current_date)
    current_date += timedelta(days=1)

# ✅ 파일 처리 함수 (OBS & MK-PRISM 변환)
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

    # ✅ 격자 크기 및 위치 정보
    grid_size = ds.attrs["grid_size"]
    grid_nx = int(ds.attrs["grid_nx"])
    grid_ny = int(ds.attrs["grid_ny"])
    map_slon = ds.attrs["map_slon"]
    map_slat = ds.attrs["map_slat"]

    lons = np.linspace(map_slon, map_slon + grid_size * (grid_nx - 1), grid_nx)
    lats = np.linspace(map_slat, map_slat + grid_size * (grid_ny - 1), grid_ny)
    lon_grid, lat_grid = np.meshgrid(lons, lats)

    # ✅ 최근접 관측소 데이터 추출 (OBS 변환)
    tree = cKDTree(list(zip(lat_grid.ravel(), lon_grid.ravel())))
    station_coords = np.array(list(stations.values()))
    _, idxs = tree.query(station_coords)

    obs_temps = data.ravel()[idxs]

    obs_ds = xr.Dataset(
        {"temperature": (["station"], obs_temps)},
        coords={"station": list(stations.keys())}
    )
    obs_save_path = os.path.join(obs_save_dir, f"obs_ta_{year}{month}{day}0000.nc")
    obs_ds.to_netcdf(obs_save_path)
    print(f"📁 저장 완료: {obs_save_path}")

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
    num_workers = min(cpu_count(), 4)  # 최대 8개의 프로세스를 사용
    print(f"🚀 멀티프로세싱 시작 (사용할 CPU 코어 수: {num_workers})")

    with Pool(num_workers) as pool:
        pool.map(process_sgd_file, dates)

    print("🎉 모든 변환 완료!")
