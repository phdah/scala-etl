import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object StateUpdater {
  def main(args: Array[String]): Unit = {
    // Read args
    if (args.length != 2) {
      System.err.println("Usage: StateUpdater <sourcePath> <targetPath>")
      System.exit(1)
    }

    val sourcePath = args(0)
    val targetPath = args(1)
    val numPartitions = 500 // Tune as needed

    val spark = SparkSession.builder
      .appName("State Updater")
      .getOrCreate()

    import spark.implicits._

    // === Step 1: Read daily events ===
    val events = spark.read.format("delta").load(sourcePath)

    val latestUpdates = events
      .withColumn("id_hash", abs(hash(col("id"))) % numPartitions)
      .withColumn("row_num", row_number().over(Window.partitionBy("id").orderBy(col("event_time").desc)))
      .filter(col("row_num") === 1)
      .drop("row_num")

    // === Step 2: Repartition by id_hash for parallel processing ===
    val updatesRepartitioned = latestUpdates.repartition(numPartitions, col("id_hash"))

    // === Step 3: Parallel update logic ===
    updatesRepartitioned.foreachPartition { partitionIter =>
      val spark = SparkSession.builder.getOrCreate()
      import spark.implicits._

      val updatesDF = partitionIter.toSeq.toDF()

      if (updatesDF.isEmpty) return

      val idHashes = updatesDF.select("id_hash").distinct().as[Int].collect()
      val ids = updatesDF.select("id").distinct().as[String].collect()

      val stateDF = spark.read.format("delta")
        .load(targetPath)
        .filter(col("id_hash").isin(idHashes: _*))
        .filter(col("id").isin(ids: _*))

      val merged = updatesDF
        .unionByName(stateDF)
        .withColumn("row_num", row_number().over(Window.partitionBy("id").orderBy(col("event_time").desc)))
        .filter(col("row_num") === 1)
        .drop("row_num")

      // Overwrite only affected partitions
      merged.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", s"id_hash IN (${idHashes.mkString(",")})")
        .save(targetPath)
    }

    spark.stop()
  }
}
