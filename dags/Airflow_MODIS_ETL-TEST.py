import requests
import pandas as pd
from datetime import datetime, timedelta
import os
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import SchemaField
from airflow import DAG
from airflow.operators.python import PythonOperator

current_year = datetime.now().year
date = datetime.now() - timedelta(days=1)
date_str = date.strftime("%Y%m%d")
bucket_name = 'envilink_raw'  # Update this to your GCS bucket name
source_file_name = f'/opt/airflow/dags/GISTDA_Hotspot/MODIS_{date_str}.csv'  # Update this to the path of your local file
destination_blob_name = f'gistda/modis_gistda_hotspot/MODIS_{date_str}.csv'  # Update this to the desired path in GCS

PROJECT_ID = 'envilink'
DATASET_NAME = 'gistda'
TABLE_NAME = 'MODIS_hotspot'
GCS_BUCKET = 'envilink_raw'
GCS_FILE_PATH = f'gs://{GCS_BUCKET}/gistda/modis_gistda_hotspot/MODIS_{date_str}.csv'  # Update the filename as needed
client = bigquery.Client(project=PROJECT_ID)
    
def download_excel(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        print(f"Failed to download from {url}")
        return None

# Download Excel files from fire GISTDA
#https://fire.gistda.or.th/fire/y2024/80_Report/Excel/N_Mod_Day/N_Mod_20240422.xlsx
#https://fire.gistda.or.th/fire/y2024/80_Report/Excel/N_Vi1_Day/N_Vi1_20240422.xlsx
    
def extract_transform():
    current_year = datetime.now().year
    date = datetime.now() - timedelta(days=1)
    date_str = date.strftime("%Y%m%d")

    base_url = "https://fire.gistda.or.th/fire/y{}/80_Report/Excel/"
    modis_url = base_url.format(current_year) + f"N_Mod_Day/N_Mod_{date_str}.xlsx"
    viirs_url = base_url.format(current_year) + f"N_Vi1_Day/N_Vi1_{date_str}.xlsx"
    modis_url = base_url.format(current_year) + f"N_Mod_Day/N_Mod_20240603.xlsx"
    viirs_url = base_url.format(current_year) + f"N_Vi1_Day/N_Vi1_20240603.xlsx"


    modis_data = download_excel(modis_url) #on 06/06/2024 N_Mod_20240606.xlsx not found last file found is N_Mod_20240604.xlsx
    viirs_data = download_excel(viirs_url)

    if modis_data is None or viirs_data is None:
        print("Exiting due to download failure")
        exit()

    modis_df = pd.read_excel(modis_data, sheet_name="ALL")
    modis_df = modis_df.dropna(how='all')
    viirs_df = pd.read_excel(viirs_data, sheet_name="ALL")
    viirs_df = viirs_df.dropna(how='all')

    # Filter out rows containing specific notes
    modis_df = modis_df[~modis_df['HotSpotID'].str.contains('หมายเหตุ', na=False)]
    modis_df = modis_df[~modis_df['HotSpotID'].str.contains('หมายเหตุ', na=False)]
    modis_df = modis_df[~modis_df['HotSpotID'].str.contains('ที่มาของข้อมูล', na=False)]
    modis_df = modis_df[~modis_df['HotSpotID'].str.contains('ข้อมูลจุดความร้อน', na=False)]
    modis_df = modis_df[~modis_df['HotSpotID'].str.contains('รายงานข้อมูลนี้จัดทำขึ้น', na=False)]
    modis_df = modis_df[~modis_df['HotSpotID'].str.contains('รายงานนี้เป็นรายงานสรุปเบื้องต้น', na=False)]
    modis_df = modis_df[~modis_df['HotSpotID'].str.contains('รายงานนี้เป็นรายงานสรุปเบื้องต้น ยังไม่สามารถอ้างอิงเป็นรายงานสุดท้ายได้', na=False)]
    modis_df = modis_df[~modis_df['HotSpotID'].str.contains('รายงานข้อมูลนี้จัดทำขึ้นเพื่อ การวางแผน และการเข้าระงับเหตุในพื้นที่', na=False)]

    viirs_df = viirs_df[~viirs_df['HotSpotID'].str.contains('หมายเหตุ', na=False)]
    viirs_df = viirs_df[~viirs_df['HotSpotID'].str.contains('หมายเหตุ', na=False)]
    viirs_df = viirs_df[~viirs_df['HotSpotID'].str.contains('ที่มาของข้อมูล', na=False)]
    viirs_df = viirs_df[~viirs_df['HotSpotID'].str.contains('ข้อมูลจุดความร้อน', na=False)]
    viirs_df = viirs_df[~viirs_df['HotSpotID'].str.contains('รายงานข้อมูลนี้จัดทำขึ้น', na=False)]
    viirs_df = viirs_df[~viirs_df['HotSpotID'].str.contains('รายงานนี้เป็นรายงานสรุปเบื้องต้น', na=False)]
    viirs_df = viirs_df[~viirs_df['HotSpotID'].str.contains('รายงานนี้เป็นรายงานสรุปเบื้องต้น ยังไม่สามารถอ้างอิงเป็นรายงานสุดท้ายได้', na=False)]
    viirs_df = viirs_df[~viirs_df['HotSpotID'].str.contains('รายงานข้อมูลนี้จัดทำขึ้นเพื่อ การวางแผน และการเข้าระงับเหตุในพื้นที่', na=False)]

    # Translate column headers to English
    # "(" , ")", "." not allowed in column names for BigQuery change it to _"
    translated_columns = {
        "วันที่": "Date",
        "จังหวัด": "Province",
        "อำเภอ": "District",
        "ตำบล": "Subdistrict",
        "ประเทศ": "Country",
        "รหัสรับผิดชอบ": "Responsibility_Code",
        "พื้นที่รับผิดชอบ": "Responsible_Area",
        "รหัสการใช้ที่ดิน": "Land_Use_Code",
        "การใช้ที่ดิน": "Land_Use",
        "จุดใกล้หมู่บ้าน": "Near_Village_Point",
        "ห่างหมู่บ้าน(กม)": "Distance_from_Village_km",
        "องศาจากหมู่บ้าน": "Degree_from_Village",
        "ทิศจากหมู่บ้าน": "Direction_from_Village",
        "ตำบล.1": "Subdistrict_1",
        "อำเภอ.1": "District_1",
        "จังหวัด.1": "Province_1",
        "วัน": "Day",
        "เวลา": "Time"
    }

    # Rename columns in MODIS DataFrame
    modis_df = modis_df.rename(columns=translated_columns)
    modis_df['Sensor'] = 'MODIS'

    # Rename columns in VIIRS DataFrame
    viirs_df = viirs_df.rename(columns=translated_columns)
    viirs_df['Sensor'] = 'VIIRS'

    # Define the folder to save the Excel files
    folder_name = "/opt/airflow/dags/GISTDA_Hotspot"
    os.makedirs(folder_name, exist_ok=True)  # Create the folder if it doesn't exist

    # Save the MODIS DataFrame as an Excel file
    modis_filepath = os.path.join(folder_name, f"MODIS_{date_str}.xlsx")
    modis_df.to_excel(modis_filepath, index=False)
    print(f"MODIS Excel file saved successfully at: {modis_filepath}")

    # Save the VIIRS DataFrame as an Excel file
    viirs_filepath = os.path.join(folder_name, f"VIIRS_{date_str}.xlsx")
    viirs_df.to_excel(viirs_filepath, index=False)
    print(f"VIIRS Excel file saved successfully at: {viirs_filepath}")

    read_file = pd.read_excel(f"/opt/airflow/dags/GISTDA_Hotspot/MODIS_{date_str}.xlsx")
    read_file.to_csv(f"/opt/airflow/dags/GISTDA_Hotspot/MODIS_{date_str}.csv", index=False, header=True)

def upload_file_to_gcs(bucket_name, source_file_name, destination_blob_name):
    # Create a storage client
    storage_client = storage.Client()
    
    # Get the bucket
    bucket = storage_client.bucket(bucket_name)
    
    # Create a new blob and upload the file's content
    blob = bucket.blob(destination_blob_name)
    
    # Upload the file
    blob.upload_from_filename(source_file_name)
    
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def load_csv_to_bigquery(gcs_file_path, dataset_name, table_name):
    # Construct the fully-qualified BigQuery table ID
    table_id = f"{PROJECT_ID}.{dataset_name}.{table_name}"

    # Set up the job configuration to append the data
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Assuming the first row is a header
        autodetect=False,  # Automatically infer the schema
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Append to the existing table
    )

    # Create a job to load the CSV data from GCS into the BigQuery table
    load_job = client.load_table_from_uri(
        gcs_file_path, 
        table_id, 
        job_config=job_config
    )

    # Wait for the job to complete
    load_job.result()

    # Check for errors
    if load_job.errors:
        print("Load job encountered the following errors:", load_job.errors)
    else:
        print(f"Data from {gcs_file_path} has been successfully appended to {table_id}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 20),
    'retries': 0,
}

dag = DAG(
    'MODIS_extract_transform_load',
    default_args=default_args,
    description='Dag to daily download daily data',
    schedule_interval='@daily',
    catchup=False,
)

extract_transform = PythonOperator(
    task_id='extract_transform',
    python_callable=extract_transform,
    dag=dag,
)

upload_file_to_gcs = PythonOperator(
    task_id='upload_file_to_gcs',
    python_callable=upload_file_to_gcs,
    op_args=[bucket_name, source_file_name, destination_blob_name],
    dag=dag,
)

load_csv_to_bigquery = PythonOperator(
    task_id='load_csv_to_bigquery',
    python_callable=load_csv_to_bigquery,
    op_args=[GCS_FILE_PATH, DATASET_NAME, TABLE_NAME],
    dag=dag,
)

extract_transform >> upload_file_to_gcs >> load_csv_to_bigquery