# ETL API Data Examples for Data Engineers

## APIs

* `yFinance` price and volume data

## Cloud Environment

* Google Cloud Platform

## Services Exemplified

* Google Cloud Composer (Cloud Managed Apache Airflow)
* Databricks on Google Cloud Platform
* Google Cloud Storage
* Google BigQuery

### Examples of Cloud Composer's Apache Airflow DAGs:

* `data_engineer_api_pipeline/src/commodity_prices/dags/dag_commodity_write_append.py`

![write truncate process](./src/commodity_prices/dags/docs/image-23.png)

* `data_engineer_api_pipeline/src/commodity_prices/dags/dag_commodity_write_truncate.py`/

![write append process](./src/commodity_prices/dags/docs/image-24.png)

### Example of Databricks Scheduled ETL Pipeline

* `data_engineer_api_pipeline/src/commodity_prices/databricks/write_truncate.py`

![overwrite process](./src/commodity_prices/databricks/docs/image-14.png)

### Example of Databricks Anomaly Detection Alert Email

* `data_engineer_api_pipeline/src/market_demand/databricks/market_demand_alert.ipynb`

![demand anomaly alert flow](./src/market_demand/docs/image-13.png)