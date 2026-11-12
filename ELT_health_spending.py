from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable, task
from airflow.exceptions import AirflowException
from pymongo.mongo_client import MongoClient
from pymongo import UpdateOne

from datetime import datetime, timedelta
import pandas as pd
import requests

@task(
    retries=1,                              # 失敗時最多自動重試 1 次
    retry_delay=timedelta(minutes=2),          # 每次重試間隔 2 分鐘)
)
def download_dataset():
    url='https://ourworldindata.org/grapher/health-expenditure-and-financing-per-capita.csv?v=1&csvType=full&useColumnShortNames=false'
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        source_file = Variable.get("source_file_path")
        
        with open(source_file, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    print(f"File downloaded successfully: {source_file}")
    return source_file
    
@task(
    retries=2,                              # 失敗時最多自動重試 2 次
    retry_delay=timedelta(minutes=2),          # 每次重試間隔 2 分鐘)
)
def load_data_to_mongo_func(file_path):
    print(f"[Load Data] 準備讀取 CSV 檔案路徑: {file_path}")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        raise AirflowException(f"無法讀取 CSV 檔案 ({file_path})。錯誤原因: {e}")
        
    if df.empty:
        print("[Load Data] 警告：CSV 檔案內容為空，無資料可載入。")
        return

    
    # 透過 get_conn() 可以直接取得 pymongo 的 MongoClient 物件
    mongo_hook = MongoHook(mongo_conn_id="mongo_default")
    client = mongo_hook.get_conn()
    
    # 指定要存放的 Database 與 Collection 名稱
    db = client["testdb"]
    collection = db["health_expenditure_staging"]
    
    bulk_operations = []
    
    # 將檔案以 Document-per-Row 方式存入 MongoDB
    # 假設每一筆資料的唯一組合是 Entity + Year（用來判斷是否為同一筆資料）
    records = df.to_dict(orient="records")
    
    for row in records:
        # 定義唯一識別條件（Filter）：確保相同的 Entity 與 Year 重複執行時不會新增重複文件
        filter_query = {
            "Entity": row.get("Entity"),
            "Year": row.get("Year"),
        }
        
        # 使用 update_one 配合 upsert=True：若存在則更新，不存在則插入
        bulk_operations.append(
            UpdateOne(
                filter=filter_query,
                update={"$set": row},
                upsert=True
            )
        )
                
    if bulk_operations:
        bulk_result = collection.bulk_write(bulk_operations)
        
        # 計算總共被異動或新增的筆數
        total_affected = bulk_result.upserted_count + bulk_result.modified_count
        print(f"[Load Data 成功] 透過 bulk_write 批次完成！總共處理 {len(records)} 筆資料，實際新增/更新了 {total_affected} 筆至 MongoDB。")
    else:
        print("[Load Data] 沒有可寫入的資料。")
    
    return "Load completed successfully"
    
@task(
    retries=1,                              # 失敗時最多自動重試 1 次
    retry_delay=timedelta(minutes=2),          # 每次重試間隔 2 分鐘)
)
def transform_to_excel_friendly_format_func(load_status):
    # 1. 使用 MongoHook 取得 MongoDB 連線
    client = MongoHook(mongo_conn_id="mongo_default").get_conn()
    db = client["testdb"]
    
    source_collection = db["health_expenditure_staging"]
    target_collection = db["health_expenditure_excel_ready"]

    # 2. 從 Staging 區讀取所有原始資料
    raw_data = list(source_collection.find({}, {"_id": 0}))
    
    if not raw_data:
        print("[Transform Warning] Staging 區無資料可供轉換。")
        return

    # 3. 在 Python 中依國家進行分組與結構重塑
    country_dict = {}
    
    for row in raw_data:
        entity = row.get("Entity")
        code = row.get("Code")
        year_str = str(row.get("Year"))  # 將年份轉為字串，作為 JSON Key
        expenditure = row.get("Health expenditure per capita - Total")

        if not entity:
            continue

        # 如果該國家還沒被建立過，初始化其基本結構
        if entity not in country_dict:
            country_dict[entity] = {
                "entity": entity,
                "code": code,
                "yearly_records": []
            }

        # 依照需求將年份與數值包成獨立字典：{ "2020": 2100.5 }
        country_dict[entity]["yearly_records"].append({year_str: expenditure})

    # 4. 將整理好的字典轉換為清單
    transformed_documents = list(country_dict.values())

    # 5. 寫入目標 Collection（採用全量覆寫或先清空再插入，確保每次轉換都是最新乾淨的結果）
    target_collection.delete_many({})
    
    if transformed_documents:
        target_collection.insert_many(transformed_documents)
        print(f"[Transform Success] 成功轉換 {len(transformed_documents)} 個國家的歷年數據，已存入 'health_expenditure_excel_ready'！")


with DAG(
    dag_id="health_spending_ELT",
    start_date=datetime(2026,11,1),
    schedule=None,
    catchup=False,
    default_args={'owner': 'BinHong','retries': 1,'retry_delay': timedelta(minutes=5)},

) as dag:

    filepath = download_dataset()
    load_status = load_data_to_mongo_func(filepath)
    transform_to_excel_friendly_format_func(load_status)
