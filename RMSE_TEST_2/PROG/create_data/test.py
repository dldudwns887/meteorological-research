import os
import shutil

# ✅ 데이터가 저장된 최상위 디렉토리
base_dir = "/home/papalio/test_research/RMSE_TEST_2/DATA/SGD_TA"

# ✅ 이동할 대상 디렉토리 (폴더 없이 저장)
target_dir = base_dir

# ✅ 모든 하위 디렉토리 순회
for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.endswith(".nc") and file.startswith("sfc_grid_ta_"):  # *.nc 파일만 선택
            source_path = os.path.join(root, file)
            target_path = os.path.join(target_dir, file)

            # 파일이 이미 존재하지 않으면 이동
            if not os.path.exists(target_path):
                shutil.move(source_path, target_path)
                print(f"📁 이동 완료: {source_path} → {target_path}")
            else:
                print(f"✅ 이미 존재: {target_path} (이동 스킵)")

# ✅ 빈 디렉토리 제거
for root, dirs, files in os.walk(base_dir, topdown=False):
    for dir in dirs:
        dir_path = os.path.join(root, dir)
        if not os.listdir(dir_path):  # 빈 디렉토리만 삭제
            os.rmdir(dir_path)
            print(f"🗑️ 빈 디렉토리 삭제: {dir_path}")

print("🎉 모든 파일 이동 및 빈 디렉토리 삭제 완료!")
