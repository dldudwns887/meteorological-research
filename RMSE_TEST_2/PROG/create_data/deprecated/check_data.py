import os
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from multiprocessing import Pool, cpu_count
import tqdm


# ✅ 데이터 경로 설정
base_dir = "/home/papalio/test_research/RMSE_TEST_2/DATA"
sgd_dir = os.path.join(base_dir, "SGD_TA")
obs_dir = os.path.join(base_dir, "OBS_TA")
mkprism_dir = os.path.join(base_dir, "MKPRISE_TA")

# ✅ 이미지 저장 경로 설정
img_base_dir = "/home/papalio/test_research/RMSE_TEST_2/IMG"
size_dir = os.path.join(img_base_dir, "test_size")
data_dir = os.path.join(img_base_dir, "test_data")

# ✅ 디렉토리 생성 (없으면 생성)
os.makedirs(size_dir, exist_ok=True)
os.makedirs(data_dir, exist_ok=True)

# ✅ 데이터셋 리스트
datasets = {
    "SGD": sgd_dir,
    "OBS": obs_dir,
    "MKPRISE": mkprism_dir
}

# ✅ 파일을 병렬로 처리하는 함수
def process_file(args):
    name, data_path, nc_file = args
    file_path = os.path.join(data_path, nc_file)
    
    try:
        ds = xr.open_dataset(file_path)

        # ✅ 변수 선택 (OBS & MK-PRISM은 `temperature` 사용)
        var_name = "temperature" if name in ["OBS", "MKPRISE"] else "data"
        if var_name not in ds:
            return None  # 데이터 변수 없으면 스킵

        # ✅ 데이터 로드 및 변환
        data_values = ds[var_name].values.astype(np.float32).flatten()

        # ✅ SGD 데이터 스케일 변환 (스케일 적용 필요)
        if name == "SGD":
            scale_factor = ds[var_name].attrs.get("data_scale", 1.0)
            data_values /= scale_factor

        # ✅ MK-PRISM 데이터 `float64 → float32` 변환
        if name == "MKPRISE":
            data_values = data_values.astype(np.float32)

        # ✅ 결측값 처리 (-9990을 결측치로 가정)
        missing_count = np.sum(data_values == -9990)

        # ✅ 이상치 (-100°C 이하, 100°C 이상 값) 개수 저장
        outlier_count = np.sum((data_values < -100) | (data_values > 100))

        # ✅ 파일 크기 저장 (KB 단위 변환)
        file_size = os.path.getsize(file_path) / 1024

        return file_size, missing_count, outlier_count, data_values.tolist()
    
    except Exception as e:
        print(f"❌ {nc_file} 처리 중 오류 발생: {e}")
        return None

# ✅ 데이터 검사 실행
for name, data_path in datasets.items():
    print(f"📌 {name} 데이터 검사 시작...")

    # 파일 목록 가져오기
    nc_files = sorted([f for f in os.listdir(data_path) if f.endswith(".nc")])

    if not nc_files:
        print(f"🚨 {name} 데이터 없음! ({data_path})")
        continue

    # ✅ 병렬 처리 설정
    num_workers = min(4, cpu_count())  # 최대 8개 프로세스 사용
    pool_args = [(name, data_path, nc_file) for nc_file in nc_files]

    with Pool(processes=num_workers) as pool:
        results = pool.map(process_file, pool_args)

    # ✅ 결과 필터링 (None 값 제거)
    results = [r for r in results if r is not None]

    # ✅ 데이터 크기, 결측치, 이상치 및 전체 데이터 추출
    file_sizes = [r[0] for r in results]
    missing_counts = [r[1] for r in results]
    outlier_counts = [r[2] for r in results]
    all_values = np.concatenate([r[3] for r in results])

    # ✅ 크기 분포 시각화
    plt.figure(figsize=(10, 5))
    plt.scatter(range(len(file_sizes)), file_sizes, alpha=0.7, color='blue')
    plt.xlabel("파일 인덱스")
    plt.ylabel("파일 크기 (KB)")
    plt.title(f"{name} 데이터 파일 크기 분포")
    plt.grid()
    size_plot_path = os.path.join(size_dir, f"{name}_size_distribution.png")
    plt.savefig(size_plot_path, dpi=300)
    plt.close()
    print(f"📊 {name} 데이터 크기 분포 저장 완료: {size_plot_path}")

    # ✅ 결측치 및 이상치 시각화
    plt.figure(figsize=(10, 5))
    plt.plot(missing_counts, label="결측치 개수", color='red', marker='o')
    plt.plot(outlier_counts, label="이상치 개수", color='purple', marker='s')
    plt.xlabel("파일 인덱스")
    plt.ylabel("개수")
    plt.title(f"{name} 데이터 결측치 및 이상치 분포")
    plt.legend()
    plt.grid()
    data_plot_path = os.path.join(data_dir, f"{name}_missing_outliers.png")
    plt.savefig(data_plot_path, dpi=300)
    plt.close()
    print(f"📊 {name} 데이터 결측치 및 이상치 분포 저장 완료: {data_plot_path}")

    # ✅ 전체 데이터 값의 분포 (히스토그램)
    plt.figure(figsize=(10, 5))
    plt.hist(all_values, bins=100, color='skyblue', edgecolor='black', alpha=0.7)
    plt.xlabel("온도 값 (°C)")
    plt.ylabel("빈도수")
    plt.title(f"{name} 데이터 온도 분포")
    plt.grid()
    hist_plot_path = os.path.join(data_dir, f"{name}_histogram.png")
    plt.savefig(hist_plot_path, dpi=300)
    plt.close()
    print(f"📊 {name} 데이터 온도 분포 저장 완료: {hist_plot_path}")

print("\n🎉 데이터 검사 완료! PNG 파일 확인하세요.")
