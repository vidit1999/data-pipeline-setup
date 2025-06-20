import os
import json
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from dateutil.parser import parse
import pyspark.sql.types as T
import pyspark.sql.functions as F
import requests


AWS_ENDPOINT_URL = os.environ.get('AWS_ENDPOINT_URL', 'http://localstack-s3:4566')
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', 'test')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', 'test')

spark = (
    SparkSession.builder \
    .appName("spark-app") \
    .config("spark.hadoop.fs.s3a.endpoint", AWS_ENDPOINT_URL)\
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)\
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)



def flatten_json(d, infer_schema):
    """
    Flattens a JSON dictionary all the way down with '__' as a separator,
    except for lists, which are converted to JSON strings. All non-list
    values are cast to strings.

    Args:
        d (dict): The input JSON dictionary.

    Returns:
        dict: The flattened dictionary.
    """
    flat_dict = {}
    # Use a stack for iterative flattening to avoid deep recursion for very large objects
    # Each item in the stack is a tuple: (current_object, current_prefix)
    stack = [(d, "")]

    while stack:
        current_obj, current_prefix = stack.pop()

        for key, value in current_obj.items():
            # Construct the new key, adding separator if a prefix exists
            new_key = f"{current_prefix}__{key}" if current_prefix else key

            if isinstance(value, dict):
                # If the value is a dictionary, add it to the stack for further flattening
                stack.append((value, new_key))
            elif isinstance(value, list):
                # If the value is a list, convert it to a JSON string
                flat_dict[new_key] = json.dumps(value)
            else:
                # For all other types, cast to string
                flat_dict[new_key] = str(value) if not infer_schema else value
    return flat_dict


@F.udf
def parse_value(value, infer_schema, is_rds):
    d = json.loads(value)

    if is_rds:
        values = {}
        if d.get("op") == "d":
            # take before key
            values.update(d.get("before") or {})
            values.update({"__op": "d"})
        else:
            values.update(d.get("after") or {})
            values.update({"__op": d.get("op")})

        return json.dumps(flatten_json(values, infer_schema))
    else:
        # static setting `__op` column
        return json.dumps(flatten_json({**d, "__op": "c"}, infer_schema))



def get_select_expressions(df):
    # Build the select expression list
    select_expressions = []

    # Add all fields from the 'value' struct directly
    select_expressions.append("value.*")

    # Add other top-level columns with '__' prefix
    for field in df.schema.fields:
        if field.name != "value":
            select_expressions.append(F.col(field.name).alias(f"__{field.name}"))

    # Apply the transformations
    return df.select(select_expressions)


def extract_ymd(df, ts_col):
    df_with_timestamp = df.withColumn(
        ts_col,
        F.col(ts_col).cast(T.TimestampType())
    )

    df_with_date_parts = df_with_timestamp\
        .withColumn("year", F.year(F.col(ts_col))) \
        .withColumn("month", F.month(F.col(ts_col))) \
        .withColumn("day", F.dayofmonth(F.col(ts_col)))

    return df_with_date_parts


################################################

pipeline_config_id = sys.argv[1]

config = requests.get(f"http://backend:8000/configs/{pipeline_config_id}/").json()

topic = config["topic_name"]
infer_schema = config["infer_schema"]
is_rds = config["is_rds"]

write_location = config["append_write_path"]
checkpoint_location = config["append_checkpoint_path"]

pipeline_mode = config["mode"]
transformations = config["transformations"]


def process_df(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    value_schema = (
        spark
        .read
        .option("inferSchema", True)
        .option("primitivesAsString", not infer_schema)
        .json(
            batch_df.rdd.map(
                lambda r: r.value
            )
        )
        .schema
    )

    batch_df = batch_df.withColumn("value", F.from_json("value", value_schema))

    df1 = get_select_expressions(batch_df)
    df2 = extract_ymd(df1, "__timestamp")

    if pipeline_mode == "append":
        # we have to do transformations here only.
        for transformation in transformations:
            col_name = transformation["col_name"]
            col_expr = transformation["col_expr"]
            if col_name in source_df.columns:
                source_df = source_df.withColumn(col_name, F.expr(col_expr))

    # Write the DataFrame to Delta Lake in append mode, partitioned by year, month, day
    df2.write \
    .option("mergeSchema", "true") \
    .format("delta") \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .save(write_location)

    print(f"Data is appened in {write_location}")



(
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:29092")
    .option("startingOffsets", "earliest")
    .option("subscribe", topic)
    .load()
    .withColumn("key", F.col("key").cast("string"))
    .withColumn("value", parse_value(F.col("value").cast("string"), F.lit(infer_schema), F.lit(is_rds)))
    .writeStream
    .foreachBatch(process_df)
    .option("checkpointLocation", checkpoint_location)
    .trigger(once=True)
    .start()
    .awaitTermination()
)
