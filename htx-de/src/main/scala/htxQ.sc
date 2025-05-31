import org.apache.spark.sql.{DataFrame, SparkSession}

class htxQ(spark: SparkSession) {
  def read_parquet_df(path: String): Unit = {
    val df: DataFrame = spark.read.parquet(path)
    df.show()
  }
}

// Usage example:
// val spark = SparkSession.builder.appName("ParquetPrinterApp").getOrCreate()
// val printer = new ParquetPrinter(spark)
// printer.readAndDisplay("/path/to/file.parquet")    