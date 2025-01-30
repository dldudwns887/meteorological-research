import xarray as xr
import numpy as np
import os
import pandas as pd
from scipy.spatial import cKDTree

# 저장 경로 설정
obs_save_dir = "/home/papalio/test_research/RMSE_TEST_2/DATA/OBS_TA"
os.makedirs(obs_save_dir, exist_ok=True)

# 가상의 시도 중심 좌표 (17개 시도)
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

# 파일 경로 설정
file_path = "/home/papalio/test_research/python_edu/test_2024/test_2024/DATA/org/sgd/2020/01/01/sfc_grid_ta_202001010000.nc"

# NetCDF 파일 로드
ds = xr.open_dataset(file_path)
data = ds["data"].values.astype(np.float32)
data[data == -9990] = np.nan  # 결측값 처리
data /= ds["data"].attrs["data_scale"]  # 스케일 적용

# 격자 크기 및 위치 정보
grid_size = ds.attrs["grid_size"]  # 0.5° 격자
grid_nx = int(ds.attrs["grid_nx"])
grid_ny = int(ds.attrs["grid_ny"])
map_slon = ds.attrs["map_slon"]
map_slat = ds.attrs["map_slat"]

# 격자 중심점 좌표 생성
lons = np.linspace(map_slon, map_slon + grid_size * (grid_nx - 1), grid_nx)
lats = np.linspace(map_slat, map_slat + grid_size * (grid_ny - 1), grid_ny)
lon_grid, lat_grid = np.meshgrid(lons, lats)

# KDTree를 이용한 최근접 보간
tree = cKDTree(list(zip(lat_grid.ravel(), lon_grid.ravel())))
station_coords = np.array(list(stations.values()))
_, idxs = tree.query(station_coords)

# 최근접 격자 데이터 추출
obs_temps = data.ravel()[idxs]

# Xarray Dataset 생성 및 저장
obs_ds = xr.Dataset(
    {"temperature": (["station"], obs_temps)},
    coords={"station": list(stations.keys())}
)
obs_ds.to_netcdf(os.path.join(obs_save_dir, "obs_ta_202001010000.nc"))

print(f"✅ OBS 데이터 저장 완료: {obs_save_dir}/obs_ta_202001010000.nc")
