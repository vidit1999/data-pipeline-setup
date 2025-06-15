import os
import json
import sys
import requests

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from dateutil.parser import parse
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql.window import Window


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




def flatten_json(d, cast_string):
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
                flat_dict[new_key] = str(value) if cast_string else value
    return flat_dict


@F.udf
def parse_value(value, cast_string):
    d = json.loads(value)
    values = {}
    if d.get("op") == "d":
        # take before key
        values.update(d.get("before") or {})
        values.update({"__op": "d"})
    else:
        values.update(d.get("after") or {})
        values.update({"__op": d.get("op")})

    return json.dumps(flatten_json(values, cast_string))


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

# Maps transformation name to spark function
transformation_mapper = {
    "lower": F.lower,
    "upper": F.upper,
}


################################################

pipeline_config_id = sys.argv[1]

config = requests.get(f"http://backend:8000/configs/{pipeline_config_id}/").json()

topic = config["topic_name"]
infer_schema = config["infer_schema"]
partition_col = config["partition_col"]
primary_keys = config["primary_keys"]

read_location = config["append_write_path"]
write_location = config["upsert_write_path"]
checkpoint_location = config["upsert_checkpoint_path"]

transformations = config["transformations"]

merge_condition = " and ".join([
    f"target.{pk} = source.{pk}"
    for pk in primary_keys
])


def process_df(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    window_spec = Window.partitionBy(primary_keys).orderBy(
        F.col("__timestamp").desc(),
        F.col("__offset").desc()
    )

    source_df = batch_df\
        .withColumn("__row_num", F.row_number().over(window_spec)) \
        .filter(F.col("__row_num") == 1) \
        .drop("__row_num")

    if partition_col:
        source_df = source_df.withColumn(partition_col, F.col(partition_col).cast(T.TimestampType()))
        source_df = extract_ymd(source_df, partition_col)

    for transformation in transformations:
        name = transformation["name"]
        op_column = transformation["op_column"]
        if op_column in source_df.columns and name in transformation_mapper:
            source_df = source_df.withColumn(op_column, transformation_mapper[name](op_column))

    # Perform the merge operation
    if not DeltaTable.isDeltaTable(spark, write_location):
        # If the table does not exist, create it by overwriting with the ranked data
        print(f"Delta table not found at '{write_location}'. Creating table by overwriting.")

        wdf = source_df.write \
        .format("delta") \
        .mode("overwrite")

        if partition_col:
            wdf = wdf.partitionBy("year", "month", "day")

        wdf.save(write_location)
    else:
        print(f"Delta table found at '{write_location}'. Merging into table.")
        delta_table = DeltaTable.forPath(spark, write_location)

        delta_table.alias("target").merge(
            source_df.alias("source"),  # Alias the source DataFrame for clarity in conditions
            merge_condition            # The join condition for the merge
        ) \
        .whenMatchedDelete("source.__op = 'd'") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()



(
    spark
    .readStream
    .format("delta")
    .load(read_location)
    .writeStream
    .foreachBatch(process_df)
    .option("checkpointLocation", checkpoint_location)
    .trigger(once=True)
    .start()
    .awaitTermination()
)
