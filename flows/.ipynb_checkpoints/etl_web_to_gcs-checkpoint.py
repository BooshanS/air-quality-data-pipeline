from prefect import flow,task
from pathlib import Path
import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket
import requests
from datetime import datetime

@task(retries=3)
def fetch(baseurl: str) -> pd.DataFrame:
    response=requests.get(baseurl)
    print(response.status_code)
    if response.status_code==200:
        data=response.json()
        print(data)
    else:
        print("fail")
    df=pd.json_normalize(data)
    print(df.head(5))
    return df


@flow()
def etl_web_to_gcs() -> None:
    accesskey="6a6aef1bb857d51439fdb91364ba1cd7"
    baseurl=f"http://api.aviationstack.com/v1/flights?access_key={accesskey}"
    df=fetch(baseurl)


if __name__=='__main__':
    etl_web_to_gcs()









   