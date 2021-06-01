# Spark TPC-DS Performance tests

This is a performance testing framework for Spark SQL in Apache Spark 2.2+.

If you need more information, then you have to check [this repository.](https://github.com/databricks/spark-sql-perf)

## Build Process

Benchmark is built using Apache Maven. To build benchmark, run:
```
mvn clean package
```

## Running Benchmark

To add local jar dependency(spark-sql-perf) to a maven project, run:   
```
mvn install:install-file -Dfile=SPARK_SQL_PERF_JAR_PATH \
  -DgroupId=com.databricks -DartifactId=spark-sql-perf -Dversion=SPARK_SQL_PERF_VERSION \
  -Dpackaging=jar -DlocalRepositoryPath=lib
```

To run all queries in TPC-DS, run:  
```
./bin/spark-submit --class edu.sogang.benchmark.RunBench \
  --jars SPARK_SQL_PERF_JAR_PATH JAR_PATH --config-filenames config.properties
```

To run Query1 and Query2, run:  
```
./bin/spark-submit --class edu.sogang.benchmark.RunBench \
  --jars SPARK_SQL_PERF_JAR_PATH JAR_PATH --query-names q1,q2 --config-filename config.properties
```