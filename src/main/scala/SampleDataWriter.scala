import org.apache.spark.sql.SparkSession

object SampleDataWriter {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Usage: SampleDataWriter <outputPath>")
      System.exit(1)
    }

    val outputPath = args(0)

    val spark = SparkSession.builder
      .appName("Write Sample Delta Data")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      (1, "notification", "true", 1546333200L),
      (3, "refresh", "denied", 1546334200L),
      (2, "background", "notDetermined", 1546333611L),
      (3, "refresh", "4", 1546333443L),
      (1, "notification", "false", 1546335647L),
      (1, "background", "true", 1546333546L)
    )

    val df = data.toDF("id", "name", "value", "event_time")

    df.write
      .format("delta")
      .mode("overwrite")
      .partitionBy("event_time")
      .save(outputPath)

    spark.stop()
  }
}

