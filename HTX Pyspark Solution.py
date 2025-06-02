# Databricks notebook source
# MAGIC %md
# MAGIC start of test dataset generation

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql import Row
from pyspark.sql.functions import col, count

schema = StructType([
    StructField("geographical_location_oid", LongType(), True),
    StructField("video_camera_oid", LongType(), True),
    StructField("detection_oid", LongType(), True),
    StructField("item_name", StringType(), True),
    StructField("timestamp_detected", LongType(), True)
])

data = [
    Row(1, 101, 1001, "item_1", 1622457600),
    Row(1, 101, 1002, "item_1", 1622457610),
    Row(1, 101, 1003, "item_1", 1622457611),
    Row(1, 101, 1004, "item_2", 1622457600),
    Row(1, 101, 1005, "item_3", 1622457600),
    Row(1, 101, 1006, "item_4", 1622457600),
    Row(1, 101, 1007, "item_5", 1622457600),
    Row(2, 102, 1008, "item_2", 1622457601),
    Row(3, 103, 1009, "item_3", 1622457602),
    Row(4, 104, 1010, "item_4", 1622457603),
    Row(5, 105, 1011, "item_5", 1622457604),
    Row(6, 106, 1012, "item_6", 1622457605),
    Row(7, 107, 1013, "item_7", 1622457606),
    Row(8, 108, 1014, "item_8", 1622457607),
    Row(9, 109, 1015, "item_9", 1622457608),
    Row(10, 110, 1016, "item_10", 1622457609),
    Row(10, 110, 1017, "item_1", 1622457609)
] 

trans = spark.createDataFrame(data, schema)
display(trans)

# trans.write.mode("overwrite").parquet("/dbfs/trans.parquet")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql import Row

schema_new = StructType([
    StructField("geographical_location_oid", LongType(), True),
    StructField("geographical_location", StringType(), True)
])

data_new = [
    Row(1, "location_1"),
    Row(2, "location_2"),
    Row(3, "location_3"),
    Row(4, "location_4"),
    Row(5, "location_5"),
    Row(6, "location_6"),
    Row(7, "location_7"),
    Row(8, "location_8"),
    Row(9, "location_9"),
    Row(10, "location_10")
]

ref_table = spark.createDataFrame(data_new, schema_new)
display(ref_table)
# ref_table.write.mode("overwrite").parquet("./ref_table.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC # Question 1

# COMMAND ----------

from pyspark.sql.functions import col, count

# Join the dataframes with alias
joined_df = trans.alias("trans").join(
    ref_table.alias("ref_table"),
    col("trans.geographical_location_oid") == col("ref_table.geographical_location_oid"),
    "left"
)
# display(joined_df.limit(5))

# Group by and aggregate with alias
item_rank_df = joined_df.groupBy(
    col("trans.geographical_location_oid"),
    col("item_name")
).agg(
    count("item_name").alias("item_rank")
).orderBy(
    col("trans.geographical_location_oid"),
    col("item_rank").desc()
)

# Select and cast columns
item_rank_df = item_rank_df.select(
    col("trans.geographical_location_oid"),
    col("item_rank").cast("string"),
    col("item_name")
)
display(item_rank_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Question 2

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct

# Join the dataframes with alias
joined_df = trans.alias("trans").join(
    ref_table.alias("ref_table"),
    col("trans.geographical_location_oid") == col("ref_table.geographical_location_oid"),
    "left"
)
# display(joined_df.limit(5))

# Group by and aggregate with distinct count of detection_oid
item_rank_df = joined_df.groupBy(
    col("trans.geographical_location_oid"),
    col("item_name")
).agg(
    countDistinct("detection_oid").alias("item_rank")
).orderBy(
    col("trans.geographical_location_oid"),
    col("item_rank").desc()
)

# Select and cast columns
item_rank_df = item_rank_df.select(
    col("trans.geographical_location_oid"),
    col("item_rank").cast("string"),
    col("item_name")
)
display(item_rank_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Question 3
# MAGIC In the main class, you should provide the flexibility to change the following:
# MAGIC
# MAGIC - Input path for Parquet File 1
# MAGIC - Input path for Parquet File 2
# MAGIC - Output path for Parquet File 3
# MAGIC - Top X configuration 
# MAGIC   -> For example, during runtime, we can specify 10, in order to return top 10 items to be considered.

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct
from pyspark.sql import DataFrame

class htx_custom:
    def __init__(self, input_path1: str, input_path2: str, output_path: str, top_x: int):
        self.input_path1 = input_path1
        self.input_path2 = input_path2
        self.output_path = output_path
        self.top_x = top_x


    def load_data(self) -> (DataFrame, DataFrame):
        trans = spark.read.parquet(self.input_path1)
        ref_table = spark.read.parquet(self.input_path2)
        return trans, ref_table

    def process_data(self, trans: DataFrame, ref_table: DataFrame) -> DataFrame:
        joined_df = trans.alias("trans").join(
            ref_table.alias("ref_table"),
            col("trans.geographical_location_oid") == col("ref_table.geographical_location_oid"),
            "left"
        )

        item_rank_df = joined_df.groupBy(
            col("trans.geographical_location_oid"),
            col("item_name")
        ).agg(
            countDistinct("detection_oid").alias("item_rank")
        ).orderBy(
            col("trans.geographical_location_oid"),
            col("item_rank").desc()
        )

        item_rank_df = item_rank_df.select(
            col("trans.geographical_location_oid"),
            col("item_rank").cast("string"),
            col("item_name")
        )
        
        return item_rank_df

    def save_data(self, df: DataFrame):
        df.limit(self.top_x).write.parquet(self.output_path)

    def run(self):
        trans, ref_table = self.load_data()
        processed_df = self.process_data(trans, ref_table)
        self.save_data(processed_df)

# initalize
# processor = htx_custom("./trans.parquet", "./ref_table.parquet", "./output.parquet", top_x = 10)
# processor.run()

# COMMAND ----------

# MAGIC %md
# MAGIC #solution class
# MAGIC

# COMMAND ----------


# Final Solution: 
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window

class HTXC:
    def __init__(self, trans_path, ref_table_path, output_path, top_x=10):
        self.trans_path = trans_path
        self.ref_table_path = ref_table_path
        self.output_path = output_path
        self.top_x = top_x

    def read_parquet_file(self, path):
        return spark.read.parquet(path)

    def transformation(self):
        trans = self.read_parquet_file(self.trans_path)
        ref_table = self.read_parquet_file(self.ref_table_path)

        joined_df = trans.alias("trans").join(
            ref_table.alias("ref_table"),
            col("trans.geographical_location_oid") == col("ref_table.geographical_location_oid"),
            "left"
        )

        item_counts = (
            joined_df.groupBy(col("trans.geographical_location_oid"), col("trans.item_name"))
            .agg(count("detection_oid").alias("detection_count"))
        )

        window_spec = Window.partitionBy(col("trans.geographical_location_oid")).orderBy(col("detection_count").desc())
        ranked_items = item_counts.withColumn("item_rank", row_number().over(window_spec))
        filtered_df = ranked_items.filter(col("item_rank") <= self.top_x)
        final_df = filtered_df.select(
            col("trans.geographical_location_oid").alias("geographical_location"),
            col("item_rank"),
            col("trans.item_name")
        )
        return final_df

    def output_parquet_file(self, df):
        df.coalesce(1).write.mode('overwrite').parquet(self.output_path)


processor = HTXC(trans_path="file:/Workspace/Users/alex_chen@cpf.gov.sg/HT Question/htx_q/data/trans.parquet", 
                ref_table_path="file:/Workspace/Users/alex_chen@cpf.gov.sg/HT Question/htx_q/data/ref_table.parquet", 
                output_path="./output.parquet",
                top_x=2)

final_df = processor.transformation()
display(final_df)
processor.output_parquet_file(final_df)
