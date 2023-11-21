from prefect import flow,task
from pathlib import Path
import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket
import requests
from datetime import datetime,timedelta
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials



@task(retries=3,log_prints=True)
def fetch(baseurl: str) -> pd.DataFrame:
    response=requests.get(baseurl)
    print(response.status_code)
    if response.status_code==200:
        data=response.json()
    else:
        print("fail")
    df=pd.json_normalize(data['results'])
    print(df.head(5))
    print(df['date.local'])
    print(df.dtypes)
    return df
@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    changetype={'location':str, 'parameter':str, 'unit':str,
                'country':str, 'city':str, 'entity':str, 
                'sensorType':str
                }
    df=df.astype(changetype)
    df['date.utc']=df['date.utc'].str[:19]
    df['date.utc']=pd.to_datetime(df['date.utc'])
    df['date.local']=df['date.local'].str[:19]
    df['date.local']=pd.to_datetime(df['date.local'])
    df=df.rename(columns={'date.utc':'UTCDate', 'date.local': 'LocalDate', 'coordinates.latitude': 'latitude', 'coordinates.longitude': 'longitude'})
    print(df.head())
    df=df.drop(columns=['isAnalysis'])
    print(df.dtypes)
    return df

@task()
def write_local(df: pd.DataFrame,dataset_file: str, day=(datetime.now()-timedelta(days=1)).date()) -> Path:
    path=Path(f"/home/Admin/datapipe/data/{day}/{dataset_file}.parquet")
    gcp_path=Path(f"India/{day}/{dataset_file}.parquet")
    path.parent.mkdir(exist_ok=True,parents=True)
    df.to_parquet(path,compression="gzip")
    return path,gcp_path

@task()
def write_gcs(path: Path,gcp_path: Path) -> None:
    # gcp_credentials_block = GcpCredentials.load("air-quality-cred")
    # print(gcp_credentials_block)
    gcp_block = GcsBucket.load("air-quality-bucket")
    print(gcp_block)
    gcp_block.upload_from_path(
        from_path=f"{path}", to_path=f"{gcp_path}"
    )
    return



@flow()
def etl_web_to_gcs() -> None:
    fromdate=(datetime.now()-timedelta(days=2)).date()
    todate=(datetime.now()-timedelta(days=1)).date()
    dataset_file=f"air_quality_india_{todate}"
    baseurl=f"https://api.openaq.org/v2/measurements?date_from={fromdate}T23%3A59%3A59&date_to={todate}T23%3A59%3A59&limit=10000&sort=desc&country=IN&radius=25000&order_by=datetime"
    df=fetch(baseurl)
    df_clean=clean(df)
    path,gcp_path=write_local(df_clean,dataset_file)
    write_gcs(path,gcp_path)

if __name__=='__main__':
    etl_web_to_gcs()









   