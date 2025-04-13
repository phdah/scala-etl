name := "scala-etl"

version := "0.1.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.5"
val deltaVersion = "3.2.1"

libraryDependencies ++= Seq(
  "io.delta"         %% "delta-spark" % deltaVersion,
  "org.apache.spark" %% "spark-core"  % sparkVersion,
  "org.apache.spark" %% "spark-sql"   % sparkVersion
)
