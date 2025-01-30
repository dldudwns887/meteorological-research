import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import netCDF4 as nc
import multiprocessing

def process_file(filepath):
    filename = os.path.basename(filepath)
    date_str = filename.split("_")[-1].split(".")[0]

    if len(date_str) != 12:
        return None

    try:
        date = datetime.strptime(date_str, "%Y%m%d%H%M")
        
        with nc.Dataset(filepath) as dataset:
            var_data = dataset.variables['data'][:]  # Load data without flattening

        # Flatten the array after loading
        var_data = var_data.ravel()
        
        valid_mask = var_data != -9990
        valid_count = np.sum(valid_mask)

        if valid_count == 0:
            file_info = {
                'date': date.strftime("%Y%m%d%H%M"),
                'size_bytes': os.path.getsize(filepath),
                'filename': filename,
                'min': None,
                'max': None,
                'no_valid_data': True,
                'zero_ratio': 0,
                'negative_ratio': 0,
                'reason': 'All values invalid (-9990)'
            }
            return file_info

        valid_data = var_data[valid_mask]
        min_value = np.min(valid_data)
        max_value = np.max(valid_data)

        zero_count = np.sum(valid_data == 0)
        negative_count = np.sum(valid_data < 0)

        zero_ratio = zero_count / valid_count
        negative_ratio = negative_count / valid_count
        
        file_info = {
            'date': date.strftime("%Y%m%d%H%M"),
            'size_bytes': os.path.getsize(filepath),
            'filename': filename,
            'min': min_value,
            'max': max_value,
            'no_valid_data': False,
            'zero_ratio': zero_ratio,
            'negative_ratio': negative_ratio
        }
        return file_info

    except Exception as e:
        print(f"Error processing file {filepath}: {str(e)}")
        return None

def main():
    var = input("검사할 변수를 입력하세요 (rn_day, hm, ta, ws_10m) [기본값: ta]: ").strip() or "ta"
    version = input("출력할 버전을 입력하세요 (예: v1, v2, v3) [기본값: v1]: ").strip() or "v1"

    root_directory = "/home/papalio/test_research/python_edu/test_2024/test_2024/DATA"
    search_pattern = os.path.join(root_directory, "**", f"sfc_grid_{var}*.nc")
    file_list = glob.glob(search_pattern, recursive=True)

    if not file_list:
        print(f"'{root_directory}'에서 '{var}' 변수 파일을 찾을 수 없습니다.")
        return

    max_workers = max(1, multiprocessing.cpu_count() // 4)
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_file, filepath) for filepath in file_list]
        file_info = [f.result() for f in as_completed(futures) if f.result() is not None]

    df = pd.DataFrame(file_info)
    df.sort_values(by='date', inplace=True)

    high_zero_ratio_df = df[df['zero_ratio'] >= 0.3]

    output_dir = "/home/papalio/test_research/python_edu/test_2024/test_2024/RESULTS"
    os.makedirs(output_dir, exist_ok=True)

    out_path1 = os.path.join(output_dir, f'zero_ratio_{var}_file_list.csv')
    out_path2 = os.path.join(output_dir, f'zero_ratio_over_30pct_{var}_file_list.csv')
    df.to_csv(out_path1, index=False)
    high_zero_ratio_df.to_csv(out_path2, index=False)

    print(f"파일 분석 결과가 저장되었습니다. 총 {len(df)} 개의 파일 중 {len(high_zero_ratio_df)} 개의 파일이 0값 비율 30% 이상입니다.")
    print(f"유효한 데이터가 없는 파일: {df['no_valid_data'].sum()}개")

    date_high_zero = pd.to_datetime(high_zero_ratio_df['date'], format='%Y%m%d%H%M')
    date_high_zero_df_monthly = date_high_zero.dt.to_period('M').value_counts().sort_index()
    date_high_zero_df_monthly.index = date_high_zero_df_monthly.index.astype(str)
    monthly_out_path = os.path.join(output_dir, f'zero_ratio_over_30pct_{var}_month.csv')
    date_high_zero_df_monthly.to_csv(monthly_out_path, header=True)

    print(f"월별 카운트가 '{monthly_out_path}'에 저장되었습니다.")
    print(f"사용된 CPU 코어 수: {max_workers}")

if __name__ == "__main__":
    main()
