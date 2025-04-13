import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object Main {
  def rangeDf(spark: SparkSession, num: Long): Dataset[java.lang.Long] = {
    val df = spark.range(num)
    return df
  }
  def main(args: Array[String]): Unit                                  = {
    val spark = SparkSession.builder
      .appName("Scala ETL")
      .master("local[*]")
      .getOrCreate()

    val df = rangeDf(spark, 10)
    df.show()

    spark.stop()
  }
}
