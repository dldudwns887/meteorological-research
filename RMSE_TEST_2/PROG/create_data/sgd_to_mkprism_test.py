import xarray as xr
import numpy as np
import os

# 저장 경로 설정
mkprism_save_dir = "/home/papalio/test_research/RMSE_TEST_2/DATA/MKPRISE_TA"
os.makedirs(mkprism_save_dir, exist_ok=True)

# 파일 경로 설정
file_path = "/home/papalio/test_research/python_edu/test_2024/test_2024/DATA/org/sgd/2020/01/01/sfc_grid_ta_202001010000.nc"

# NetCDF 파일 로드
ds = xr.open_dataset(file_path)
data = ds["data"].values.astype(np.float32)
data[data == -9990] = np.nan  # 결측값 처리
data /= ds["data"].attrs["data_scale"]  # 스케일 적용

# MK-PRISM 보정 (고도 보정 적용)
lapse_rate = -6.5  # 기온감률 (°C/km)
estimated_heights = np.random.uniform(400, 600, size=data.shape)  # 임의의 고도 데이터 (400~600m)

# 보정 공식: T_corrected = T_sgd + lapse_rate * (H_grid - 500m) / 1000
data_corrected = data + lapse_rate * (estimated_heights - 500) / 1000

# Xarray Dataset 생성 및 저장
mkprism_ds = xr.Dataset(
    {"temperature": (["ny", "nx"], data_corrected)},
    coords={"ny": np.arange(data.shape[0]), "nx": np.arange(data.shape[1])}
)
mkprism_ds.to_netcdf(os.path.join(mkprism_save_dir, "mkprism_ta_202001010000.nc"))

print(f"✅ MK-PRISM 데이터 저장 완료: {mkprism_save_dir}/mkprism_ta_202001010000.nc")
