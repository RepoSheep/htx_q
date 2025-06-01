import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, countDistinct, row_number}
import org.apache.spark.sql.expressions.Window

class htxQ_class(spark: SparkSession) {
  def read_parquet_df(path: String): DataFrame = {
    spark.read.parquet(path)
  }

  def write_csv_df(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(path)
  }

  def write_parquet_df(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write
      .mode("overwrite")
      .parquet(path)
  }

  def compute_item_rank(trans: DataFrame, ref_table: DataFrame, topX: Int): DataFrame = {
    val joined_df = trans.alias("trans")
      .join(
        ref_table.alias("ref_table"),
        col("trans.geographical_location_oid") === col("ref_table.geographical_location_oid"),
        "left"
      )

    val agg_df = joined_df
      .groupBy(
        col("trans.geographical_location_oid"),
        col("item_name")
      )
      .agg(
        countDistinct("detection_oid").alias("item_rank")
      )

    val windowSpec = Window.partitionBy(col("trans.geographical_location_oid")).orderBy(col("item_rank").desc)

    val ranked_df = agg_df
      .withColumn("rank", row_number().over(windowSpec))
      .where(col("rank") <= topX)
      .orderBy(
        col("trans.geographical_location_oid"),
        col("item_rank").desc
      )
      .select(
        col("trans.geographical_location_oid"),
        col("item_rank").cast("string"),
        col("item_name")
      )

    ranked_df
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val inputPath1 = if (args.length > 0) args(0) else "/Users/alexchen/Documents/repo/htx/htx_q/data/trans.parquet"
    val inputPath2 = if (args.length > 1) args(1) else "/Users/alexchen/Documents/repo/htx/htx_q/data/ref_table.parquet"
    val outputPath = if (args.length > 2) args(2) else "item_rank_df"
    val topX = if (args.length > 3) args(3).toInt else 10
    val spark = SparkSession.builder
      .appName("htx-de")
      .master("local[*]")
      .getOrCreate()
    val htxC = new htxQ_class(spark)
    val trans = htxC.read_parquet_df(inputPath1)
    val ref_table = htxC.read_parquet_df(inputPath2)

    val item_rank_df = htxC.compute_item_rank(trans, ref_table, topX)
    item_rank_df.show(topX, truncate = false)

    htxC.write_parquet_df(item_rank_df, outputPath)
    htxC.write_csv_df(item_rank_df, s"${outputPath}_csv")
    spark.stop()
  }
}