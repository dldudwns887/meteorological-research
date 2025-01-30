import os
import glob
import pandas as pd
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# 📌 Data paths
ROOT_DIRECTORY = "/home/papalio/test_research/python_edu/test_2024/test_2024/DATA"
OUTPUT_CSV = "/home/papalio/test_research/RMSE_TEST/missing_files_2020_2021.csv"
OUTPUT_IMG_DIR = "/home/papalio/test_research/RMSE_TEST/IMG"
OUTPUT_SIZE_STATS = "/home/papalio/test_research/RMSE_TEST/file_size_statistics.csv"
OUTPUT_VALUE_STATS = "/home/papalio/test_research/RMSE_TEST/value_distribution_statistics.csv"

# 📌 Variables to analyze
variables = ["ta", "rn_day"]

# 📌 Time range
start_date = datetime(2020, 1, 1)
end_date = datetime(2021, 12, 31)

# 📌 File structure
file_structure = os.path.join(ROOT_DIRECTORY, "org/sgd/{year}/{month:02d}/{day:02d}/sfc_grid_{var}_{date}0000.nc")


def format_size(size_bytes):
    """Convert byte size to human-readable format."""
    for unit in ['', 'K', 'M', 'G']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f}{unit}B"
        size_bytes /= 1024
    return f"{size_bytes:.1f}TB"


def find_missing_files():
    """Scan for missing files between 2020-2021 and save to CSV."""
    missing_files = {var: 0 for var in variables}
    total_files = {var: 0 for var in variables}
    valid_files = {var: 0 for var in variables}
    missing_files_list = []

    current_date = start_date
    while current_date <= end_date:
        for var in variables:
            file_path = file_structure.format(
                year=current_date.year,
                month=current_date.month,
                day=current_date.day,
                var=var,
                date=current_date.strftime("%Y%m%d")
            )
            total_files[var] += 1
            if not os.path.exists(file_path):
                missing_files[var] += 1
                missing_files_list.append([var, current_date.strftime("%Y-%m-%d"), file_path])
            else:
                valid_files[var] += 1

        current_date += timedelta(days=1)

    df_missing = pd.DataFrame(missing_files_list, columns=["Variable", "Date", "Expected Path"])
    df_missing.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"\n📄 Missing file list saved: {OUTPUT_CSV}")

    # 📌 Print missing/valid/total file count per variable
    for var in variables:
        print(f"\n📌 [{var}] Data Summary")
        print(f" - ✅ Valid files: {valid_files[var]:,}")
        print(f" - ❌ Missing files: {missing_files[var]:,}")
        print(f" - 📊 Total files: {total_files[var]:,}")


def analyze_file_sizes():
    """Analyze file size distribution."""
    size_data = []

    for var in variables:
        file_sizes = []
        current_date = start_date
        while current_date <= end_date:
            file_path = file_structure.format(
                year=current_date.year,
                month=current_date.month,
                day=current_date.day,
                var=var,
                date=current_date.strftime("%Y%m%d")
            )
            if os.path.exists(file_path):
                file_sizes.append(os.path.getsize(file_path))
            current_date += timedelta(days=1)

        if not file_sizes:
            print(f"❌ No {var} data found. Skipping file size analysis.")
            continue

        # Compute statistics
        min_size, max_size = np.min(file_sizes), np.max(file_sizes)
        mean_size, median_size = np.mean(file_sizes), np.median(file_sizes)
        std_size = np.std(file_sizes)

        print(f"\n📌 [{var}] File Size Statistics")
        print(f" - Min Size: {format_size(min_size)}")
        print(f" - Max Size: {format_size(max_size)}")
        print(f" - Mean Size: {format_size(mean_size)}")
        print(f" - Median Size: {format_size(median_size)}")
        print(f" - Std Dev: {format_size(std_size)}")

        # Store data
        size_data.append([var, min_size, max_size, mean_size, median_size, std_size])

        # Generate histogram
        plt.figure(figsize=(8, 6))
        plt.hist(file_sizes, bins=50, edgecolor='black')
        plt.title(f"{var} File Size Distribution (2020-2021)", fontsize=14)
        plt.xlabel("File Size (bytes)")
        plt.ylabel("Frequency")
        plt.grid(True)
        img_path = os.path.join(OUTPUT_IMG_DIR, f"{var}_file_size_distribution.png")
        plt.savefig(img_path)
        plt.close()
        print(f"📊 {var} File size distribution saved: {img_path}")

    # Save file size statistics
    df_size = pd.DataFrame(size_data, columns=["Variable", "Min Size", "Max Size", "Mean Size", "Median Size", "Std Dev"])
    df_size.to_csv(OUTPUT_SIZE_STATS, index=False, encoding="utf-8-sig")
    print(f"📄 File size statistics saved: {OUTPUT_SIZE_STATS}")


def analyze_value_distribution():
    """Analyze -9990, 0, and valid value distributions."""
    os.makedirs(OUTPUT_IMG_DIR, exist_ok=True)
    value_data = []

    for var in variables:
        count_dict = {"Missing (-9990)": 0, "Zero (0)": 0, "Valid Data": 0}
        total_files = 0

        current_date = start_date
        while current_date <= end_date:
            file_path = file_structure.format(
                year=current_date.year,
                month=current_date.month,
                day=current_date.day,
                var=var,
                date=current_date.strftime("%Y%m%d")
            )

            if os.path.exists(file_path):
                try:
                    ds = xr.open_dataset(file_path, decode_times=False)
                    data_values = ds["data"].values.flatten()

                    # Count occurrences
                    count_dict["Missing (-9990)"] += np.sum(data_values == -9990)
                    count_dict["Zero (0)"] += np.sum(data_values == 0)
                    count_dict["Valid Data"] += np.sum((data_values != -9990) & (data_values != 0))

                    total_files += 1
                except Exception as e:
                    print(f"⚠ Error loading file: {file_path} - {e}")

            current_date += timedelta(days=1)

        # Generate pie chart
        plt.figure(figsize=(8, 6))
        plt.pie(count_dict.values(), labels=count_dict.keys(), autopct="%1.1f%%", startangle=140, colors=["red", "blue", "green"])
        plt.title(f"{var} Value Distribution (2020-2021)")
        img_path = os.path.join(OUTPUT_IMG_DIR, f"{var}_value_distribution.png")
        plt.savefig(img_path)
        plt.close()
        print(f"📊 {var} Value distribution saved: {img_path}")

        value_data.append([var, *count_dict.values()])

    df_value = pd.DataFrame(value_data, columns=["Variable", "-9990", "0", "Valid"])
    df_value.to_csv(OUTPUT_VALUE_STATS, index=False, encoding="utf-8-sig")
    print(f"📄 Value distribution statistics saved: {OUTPUT_VALUE_STATS}")


if __name__ == "__main__":
    find_missing_files()
    analyze_file_sizes()
    analyze_value_distribution()
    print("\n✅ Analysis Completed!")
