# Data Pipeline Setup
## Introduction
This is a complete data pipeline setup, done using docker and docker-compose. It runs below components.
* Airflow
* Confluent Kafka Broker
* Confluent Kafka Connect
* Kafka UI
* Local Stack S3
* Jupyter Lab
* Config Backend Service

Below image show how these components interact with each other at hight level.
![Service Interactions](/Images/Diagrapm.png)

## Service Setup
* Make sure you have docker setup done and have docker compose installed and updated.
* Make sure that docker VM have atleast 6GB of memory. Higher is better.
* Clone this repo and `cd` into it.
* Create a `.env` file with the contents of `sample.env` file.
* Run `make start` and this should start the all the services. This also setups the s3 bucket we will be using.
> Some more helpful commands can be found in the `Makefile`.

## Service Acces Points
| Service           | Local URL                     |
|-----------------  |------------------------------ |
| Airflow UI        | http://localhost:8080/        |
| Kafka UI          | http://localhost:9080/        |
| Jupyter Lab       | http://localhost:8888/        |
| Backend Swagger   | http://localhost:8000/docs/   |
> Direct shell in service containers can also be accessed. Check `make <service>-shell` commands in `Makefile`.

## Starting First Pipeline
* You can create a new table or use one of the existing ones present in airflow.
* To create a table run `make pg-shell` and run `CREATE TABLE` command. We will use `task_instance` table already present in airflow.
* First, to create a connector run below curl command in terminal. Replace database specs and table list if you are using any other table. This will start a connector. You can check this in Kafka UI.
```shell
curl -X POST -H "Content-Type: application/json" --data '{
    "name": "postgres-cdc-connector",
    "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "airflow",
    "database.password": "airflow",
    "database.dbname": "airflow",
    "database.server.name": "postgres_airflow",
    "topic.prefix": "postgres",
    "plugin.name": "pgoutput",
    "schema.include.list": "public",
    "table.include.list": "public.task_instance",
    "heartbeat.interval.ms": "5000",
    "slot.name": "debezium_slot",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "decimal.handling.mode": "double",
    "time.precision.mode": "connect",
    "tombstones.on.delete": "false"
  }
}' http://localhost:8083/connectors
```
* Visit Airflow UI and resume `continuous_python_dag`. This just a dummy DAG which runs continously, so that `task_instance` table gets continous updates. Check Kafka UI once to see the messages.
> Make sure to pause the `continuous_python_dag` after some time.
* Now you can onboard `task_instance` table by using `POST /configs` endpoint in Swagger UI. Or by using below curl directly in terminal.
```shell
curl -X 'POST' \
  'http://localhost:8000/configs/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "is_rds": true,
  "topic_name": "postgres.public.task_instance",
  "transformations": [
    {
      "col_expr": "upper(hostname)",
      "col_name": "hostname"
    }
  ],
  "mode": "upsert",
  "primary_keys": ["dag_id","task_id","run_id","map_index"],
  "infer_schema": true,
  "partition_col": "start_date",
  "dq_rules": [],
  "frequency": 1,
  "owner_name": "test-owner",
  "pipeline_version": 1,
  "catalog_name": "my_catalog",
  "table_name": "my_table"
}'
```
* Go to the Airflow UI and open `frequency_one` DAG. You will see the Airflow tasks for the table. Resume the DAG and trigger it. It will start the data pipeline.
> Check the Swagger of Backend Servive to know what all configs can be passed and what other APIs can be accessed.

![Airflow DAG Graph](/Images/AirflowUI.png)

## Querying Table
* Open the Jupyter Lab UI. There you can see an see `QueryLakeTable.ipynb` file.
* It has some example spark commands to query the delta tables.
* Make adjusments accordingly.
![Query Results](/Images/QueryResults.png)

## DQ Checking
* Open `DQCheck.ipynb` file in Jupyter Lab UI.
* It has some DQ checks that can be performed on the table with example.

## Considerations
* `frequency_one` DAG is not having any schedule. This is because we are running in local for testing purpose.
* Before you start onboarding new tables, make sure you increase docker VM memory.

