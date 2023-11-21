from etl_web_to_gcs import etl_web_to_gcs
from etl_gcs_to_bq import etl_gcs_to_bq
from prefect import flow

@flow()
def parent_etl():
    etl_web_to_gcs()
    etl_gcs_to_bq()

if __name__=="__main__":
    parent_etl()
