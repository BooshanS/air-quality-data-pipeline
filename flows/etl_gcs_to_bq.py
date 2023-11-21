from prefect import flow,task
from pathlib import Path
import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket
import requests
from datetime import datetime,timedelta
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials



@task()
def write_bq(date) -> None:
    gcs_path=f"gs://air-quality-india-parquet/India/{date}/air_quality_india_{date}.parquet"
    gcp_credentials_block = GcpCredentials.load("air-quality-cred")
    df=pd.read_parquet(gcs_path)
    print(df.iloc[1])
    target_table="air_quality_daily.air_quality_india_daily"
    df.to_gbq(target_table,
    project_id="data-pipeline-404114", 
    credentials=gcp_credentials_block.get_credentials_from_service_account(), 
    if_exists="append")

@flow()
def etl_gcs_to_bq():
    date=(datetime.now()-timedelta(days=1)).date()
    write_bq(date)


if __name__=="__main__":
    etl_gcs_to_bq()