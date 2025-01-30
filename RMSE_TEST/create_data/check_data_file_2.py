import os
import pandas as pd
# du -h /home/papalio/test_research/python_edu/test_2024/test_2024/DATA/* > /home/papalio/test_research/python_edu/test_2024/test_2024/DATA/file_sizes.txt
# 파일 경로를 설정하세요
file_path = "/home/papalio/test_research/python_edu/test_2024/test_2024/DATA/file_sizes.txt"  # 파일 경로를 입력

# 크기 변환 함수 (KB, MB를 바이트로 변환)
def convert_size_to_bytes(size_str):
    try:
        if size_str.endswith('K'):
            return float(size_str[:-1]) * 1024
        elif size_str.endswith('M'):
            return float(size_str[:-1]) * 1024 * 1024
        elif size_str.isdigit():
            return float(size_str)
    except ValueError:
        return 0  # 크기가 잘못된 경우 0으로 처리

# 파일 읽기 및 데이터 처리
data = []
with open(file_path, 'r') as file:
    for line in file:
        if '\t' in line:  # 탭으로 구분된 파일
            size, path = line.strip().split('\t', 1)
            size_in_bytes = convert_size_to_bytes(size)
            data.append((size_in_bytes, path))

# DataFrame 생성
df = pd.DataFrame(data, columns=['Size(Bytes)', 'Path'])

# 비정상 파일 기준 (예: 350KB 미만)
abnormal_threshold = 350 * 1024  # 350KB

# 총 파일 수
total_files = len(df)

# 비정상 파일 수 (크기가 기준 미만인 파일)
abnormal_files = df[df['Size(Bytes)'] < abnormal_threshold]

# 정상 파일 수
normal_files = total_files - len(abnormal_files)

# 비정상 파일 삭제
for _, row in abnormal_files.iterrows():
    file_path = row['Path']
    try:
        os.remove(file_path)
        print(f"삭제 완료: {file_path}")
    except Exception as e:
        print(f"삭제 실패: {file_path} - {e}")

# 결과 출력
print(f"총 파일 수: {total_files}")
print(f"비정상 파일 수: {len(abnormal_files)}")
print(f"정상 파일 수: {normal_files}")
