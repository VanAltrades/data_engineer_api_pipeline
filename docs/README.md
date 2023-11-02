![enable api](image.png)

![new composer env](image-1.png)

![check cloud storage location](image-2.png)

![airflow dag bucket that was created](image-3.png)

![pypi yfinance package requirement](image-4.png)

## WRITE TRUNCATE

"analyst wants daily stock information but doesn't have any data for you scenario"

![write truncate dag running](image-6.png)

## WRITE APPEND

"analyst handed you a csv of historic data scenario" - backfill data by running `$ python -m data_engineer_api_pipeline.src.commodity_prices.setup_bq`

![add pandas to packages](image-7.png)

![pre dag run table](image-8.png)

