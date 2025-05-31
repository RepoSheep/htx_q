import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, countDistinct}

class htxQ_class(spark: SparkSession) {
  def read_parquet_df(path: String): DataFrame = {
    spark.read.parquet(path)
  }

  def compute_item_rank(trans: DataFrame, ref_table: DataFrame): DataFrame = {
    val joined_df = trans.alias("trans")
      .join(
        ref_table.alias("ref_table"),
        col("trans.geographical_location_oid") === col("ref_table.geographical_location_oid"),
        "left"
      )

    val item_rank_df = joined_df
      .groupBy(
        col("trans.geographical_location_oid"),
        col("item_name")
      )
      .agg(
        countDistinct("detection_oid").alias("item_rank")
      )
      .orderBy(
        col("trans.geographical_location_oid"),
        col("item_rank").desc
      )
      .select(
        col("trans.geographical_location_oid"),
        col("item_rank").cast("string"),
        col("item_name")
      )

    item_rank_df
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("htx-de")
      .master("local[*]")
      .getOrCreate()
    val htxC = new htxQ_class(spark)
    val trans = htxC.read_parquet_df("/Users/alexchen/Documents/repo/htx/htx_q/data/trans.parquet")
    val ref_table = htxC.read_parquet_df("/Users/alexchen/Documents/repo/htx/htx_q/data/ref_table.parquet")

    val item_rank_df = htxC.compute_item_rank(trans, ref_table)
    item_rank_df.show(100, truncate = false)

    spark.stop()
  }
}