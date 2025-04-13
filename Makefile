JAR=target/scala-2.13/scala-etl_2.13-0.1.0.jar

.PHONY: build run clean

all: clean run

$(JAR):
	sbt package

run: $(JAR)
	spark-submit --class Main $(JAR)

clean:
	rm -r target || echo "no target dir"
