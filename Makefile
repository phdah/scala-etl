JAR=target/scala-2.12/scala-etl_2.12-0.1.0.jar
DELTA_VERSION=3.2.1
DELTA=io.delta:delta-spark_2.12:$(DELTA_VERSION)
SOURCE=./data/source
TARGET=./data/state

.PHONY: build run clean

all: clean run

$(JAR):
	sbt package

$(SOURCE):
	@mkdir -p data
	spark-submit \
	  --packages $(DELTA) \
	  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	  --class SampleDataWriter $(JAR) $(SOURCE)

clean:
	rm -r target || echo "no target dir"
	rm data/* || echo "no files to remove"
