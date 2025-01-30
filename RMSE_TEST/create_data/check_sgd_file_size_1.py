import os
import glob
import pandas as pd

def format_size(size_bytes):
    """바이트 크기를 사람이 읽기 쉬운 형식으로 변환"""
    for unit in ['', 'K', 'M', 'G']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f}{unit}B"
        size_bytes /= 1024
    return f"{size_bytes:.1f}TB"

def check_file_sizes(base_dir, var, min_size):
    """SGD 파일 크기를 확인하고 비정상적인 파일을 기록"""
    # 데이터 탐색 경로
    search_pattern = os.path.join(base_dir, "**", f"sfc_grid_{var}*00.nc")
    file_list = glob.glob(search_pattern, recursive=True)

    if not file_list:
        print(f"'{base_dir}' 경로에서 '{var}' 변수를 포함하는 파일이 없습니다.")
        return

    print(f"탐색된 파일 수: {len(file_list):,}")

    # 파일 크기 검사
    data = []
    for filepath in file_list:
        filename = os.path.basename(filepath)
        file_size = os.path.getsize(filepath)
        data.append({
            'filename': filename,
            'size_bytes': file_size,
            'size_human': format_size(file_size),
            'path': filepath
        })

    # DataFrame 생성
    df = pd.DataFrame(data)

    # 기준 이하 파일 필터링
    abnormal_files = df[df['size_bytes'] < min_size]
    normal_files = df[df['size_bytes'] >= min_size]

    # 결과 출력
    print(f"전체 파일 수: {len(df):,}")
    print(f"정상 파일 수: {len(normal_files):,}")
    print(f"비정상 파일 수: {len(abnormal_files):,}")

    # 결과 저장 경로 설정
    output_dir = os.path.join(base_dir, "etc/file_size")
    os.makedirs(output_dir, exist_ok=True)

    all_files_path = os.path.join(output_dir, f"sgd_{var}_all_files.csv")
    abnormal_files_path = os.path.join(output_dir, f"sgd_{var}_abnormal_files.csv")

    # CSV 저장
    df.to_csv(all_files_path, index=False)
    abnormal_files.to_csv(abnormal_files_path, index=False)

    print(f"\n결과가 저장되었습니다:")
    print(f"- 전체 파일 목록: {all_files_path}")
    print(f"- 비정상 파일 목록: {abnormal_files_path}")

def main():
    base_dir = "/home/papalio/test_research/python_edu/test_2024/test_2024/DATA"

    # 사용자 입력
    var = input("분석할 변수를 입력하세요 (예: rn_day, hm, ta, ws_10m): ").strip()
    while True:
        try:
            min_size = int(input("비정상 파일로 간주할 최소 파일 크기(bytes)를 입력하세요 (예: 48000): "))
            break
        except ValueError:
            print("올바른 숫자를 입력해주세요.")

    # 파일 크기 검사 실행
    check_file_sizes(base_dir, var, min_size)

if __name__ == "__main__":
    main()
