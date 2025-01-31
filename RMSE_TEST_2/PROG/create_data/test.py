import requests
import datetime
import os
import pandas as pd
from tqdm import tqdm
import time
from multiprocessing import Pool, cpu_count
import sys
import os

# ✅ data_api.py가 있는 경로 추가
sys.path.append("/home/papalio/test_research/python_edu/test_2024/test_2024/DATA")

# ✅ data_api.py에서 key 변수 가져오기
from data_api import key
print(f"✅ 가져온 API Key: {key}")
