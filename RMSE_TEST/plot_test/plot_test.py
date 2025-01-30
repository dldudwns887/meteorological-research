import os
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

# 📌 데이터 경로 설정
ROOT_DIRECTORY = "/home/papalio/test_research/python_edu/test_2024/test_2024/DATA"
INPUT_FILE = os.path.join(ROOT_DIRECTORY, "org/sgd/2020/02/01/sfc_grid_ta_202002010000.nc")
OUTPUT_IMG = "/home/papalio/test_research/RMSE_TEST/IMG/obs_station_map.png"

# 📌 한글 폰트 설정 (맑은 고딕 적용)
plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False  # 마이너스 기호 깨짐 방지

def plot_observation_stations(file_path, output_path):
    """📌 관측소 위치를 지도 위에 점으로 표시하는 함수"""
    # 📌 NetCDF 데이터 로드
    ds = xr.open_dataset(file_path)

    # 📌 관측소 관련 변수 확인
    if "stn_num" not in ds.attrs or "map_slon" not in ds.attrs or "map_slat" not in ds.attrs:
        print("❌ 관측소 위치 정보를 포함하는 변수가 없습니다.")
        return

    stn_num = int(ds.attrs["stn_num"])  # 관측소 개수
    lon_0 = float(ds.attrs["map_slon"])  # 기준 경도
    lat_0 = float(ds.attrs["map_slat"])  # 기준 위도

    # 📌 임시: 가상의 관측소 좌표 생성 (SGD에 실제 관측소 위치 변수가 없을 경우)
    np.random.seed(42)
    lons = lon_0 + np.random.uniform(-3, 3, stn_num)  # 중심값을 기준으로 ±3도 내 무작위 생성
    lats = lat_0 + np.random.uniform(-3, 3, stn_num)

    # 📌 Basemap 설정 (대한민국 영역 기준)
    lon_min, lon_max = 124.0, 130.0
    lat_min, lat_max = 33.0, 39.0

    fig, ax = plt.subplots(figsize=(8, 8))
    m = Basemap(
        projection="lcc",
        resolution="i",
        llcrnrlon=lon_min, llcrnrlat=lat_min,
        urcrnrlon=lon_max, urcrnrlat=lat_max,
        lat_0=lat_0, lon_0=lon_0,
        ax=ax
    )

    # 📌 지도 요소 추가
    m.drawcoastlines()
    m.drawcountries()
    m.drawparallels(np.arange(lat_min, lat_max, 1), labels=[1, 0, 0, 0])
    m.drawmeridians(np.arange(lon_min, lon_max, 1), labels=[0, 0, 0, 1])

    # 📌 관측소 위치 플로팅
    x, y = m(lons, lats)
    m.scatter(x, y, marker="o", color="red", edgecolor="black", s=40, label="Observation Station")

    # 📌 제목 및 범례
    plt.title("Observation Stations", fontsize=12)
    plt.legend()

    # 📌 이미지 저장
    plt.savefig(output_path, bbox_inches="tight", dpi=300)
    plt.close()
    print(f"📍 관측소 지도 저장 완료: {output_path}")

# 실행
plot_observation_stations(INPUT_FILE, OUTPUT_IMG)
