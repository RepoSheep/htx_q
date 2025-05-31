import org.apache.spark.sql.{DataFrame, SparkSession}

class htxQ_class(spark: SparkSession) {
  def read_parquet_df(path: String): Unit = {
    val df: DataFrame = spark.read.parquet(path)
    df.show()
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("htx-de")
      .master("local[*]") // Add this line to run Spark locally
      .getOrCreate()
    val printer = new htxQ_class(spark)
    printer.read_parquet_df("/Users/alexchen/Documents/repo/htx/htx_q/data/trans.parquet")
    spark.stop()
  }
}