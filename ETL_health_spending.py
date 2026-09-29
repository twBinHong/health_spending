from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

import pandas as pd
import requests

import json
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

DIS="/opt/airflow/dags"
START_YEAR = 2000      # 最早年份
END_YEAR = 2025        # 最新年份

# DAG arguments
default_args = {
    'owner': 'BinHong',
    'start_date': datetime(2026, 9, 1),
    'email': ['youemail@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='ETL_health_spending_data',
    schedule=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

def transpose_one_entity(part: pd.DataFrame) -> pd.DataFrame:
    """將「單一 Entity」的資料轉置成 1 列：Entity, Code, 1985 ~ 2025。"""
    # 1. 轉置：index=(Entity, Code)，columns=Year；同一年重複時取第一筆
    wide = part.pivot_table(
        index=["Entity", "Code"],
        columns="Year",
        values="Health expenditure per capita - Total", # 要轉置的數值欄位
        aggfunc="first",
    )
 
    # 2. 年份欄位固定為 START_YEAR ~ END_YEAR，缺少的年份補空欄（各 Entity 欄位才會一致）
    years = list(range(START_YEAR, END_YEAR + 1))
    wide = wide.reindex(columns=years)
 
    # 3. 檢查 years：該年度沒有數據就填 0
    wide[years] = wide[years].fillna(0)
 
    # 4. 整理格式：Entity、Code 變回一般欄位，年份欄名轉為字串
    wide = wide.reset_index()
    wide.columns = [str(c) for c in wide.columns]
    return wide
 
def split_each_entity(df: pd.DataFrame) -> dict:
    """依 Entity 拆成單一範圍，回傳 {Entity: 該 Entity 的所有列}。"""
    return {name: part for name, part in df.groupby("Entity", sort=True)}
 
 
def transpose_and_merge(parts: dict) -> pd.DataFrame:
    """逐一轉置每個 Entity，最後縱向合併。"""
    wide_list, failed = [], []
 
    for name, part in parts.items():
        try:
            wide_list.append(transpose_one_entity(part))
        except Exception as e:
            # 單一 Entity 失敗不立刻中斷，先記錄，最後統一回報
            failed.append((name, f"{type(e).__name__}: {e}"))
 
    if failed:
        for name, msg in failed:
            print(f"[轉置失敗] {name}：{msg}")
        raise RuntimeError(f"共 {len(failed)} 個 Entity 轉置失敗，已中止合併以避免資料遺漏。")
 
    # 各段欄位完全相同，可直接縱向合併，並依 Entity、Code 排序
    merged = pd.concat(wide_list, ignore_index=True)
    merged = merged.sort_values(["Entity", "Code"]).reset_index(drop=True)
 
    # Code 空字串還原為空值
    merged["Code"] = merged["Code"].replace("", pd.NA)
    return merged

def download_dataset():
    url='https://ourworldindata.org/grapher/health-expenditure-and-financing-per-capita.csv?v=1&csvType=full&useColumnShortNames=false'
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        target=f'{DIS}/health-expenditure-and-financing-per-capita.csv'
        with open(target, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    print(f"File downloaded successfully: {target}")

def transfer_dataset():
    df = pd.read_csv(f'{DIS}/health-expenditure-and-financing-per-capita.csv')
    parts = split_each_entity(df)
    merged = transpose_and_merge(parts)
    
    # 合併後檢查：列數需等於 Entity 數，且 Entity/Code 不重複
    if len(merged) != df["Entity"].nunique():
        raise ValueError("合併後列數與 Entity 數量不一致。")
    if merged.duplicated(subset=["Entity", "Code"]).any():
        raise ValueError("合併後有重複的 Entity/Code。")
    
    merged.to_csv(f"{DIS}/Transformed_file.csv", index=False, encoding="utf-8-sig")  # utf-8-sig 讓 Excel 不亂碼

def load_data():
    try:
        with open('mydatabase.txt', 'r', encoding='utf-8') as f:
            mydb = json.load(f)
    except Exception as e:
        print(f"[錯誤] {type(e).__name__}: {e}")
        return
    
    url = URL.create(
        "mysql+mysqlconnector",
        username=mydb['user'],
        password=mydb['password'],
        host=mydb['host'],
        database=mydb['database'])
    engine = create_engine(url)

    load_f = pd.read_csv("Transformed_file.csv")
    try:
        load_f.to_sql('health_spending', engine, schema=None, if_exists='replace', index=True, index_label=False)
    except Exception as e:
        print(f"[錯誤] {type(e).__name__}: {e}")
    finally:
        engine.dispose() 

download_data = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transfer_dataset',
    python_callable=transfer_dataset,
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

download_data >> transform_data >> load_data
