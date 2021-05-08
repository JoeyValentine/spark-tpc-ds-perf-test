package edu.sogang.benchmark

import com.databricks.spark.sql.perf.tpcds.{TPCDS, TPCDSTables}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.io.IOException
import java.util.Properties
import scala.annotation.tailrec
import scala.sys.exit

object RunBench {

  def runExperiment(configFileName: String, queryNames: Seq[String], ss: SparkSession): Unit = {

    val prop = new Properties()
    val inputSteam = this.getClass.getClassLoader.getResourceAsStream(configFileName)

    try {
      prop.load(inputSteam)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      inputSteam.close()
    }

    // TPC-DS Table props
    val dsdgenDir = prop.get("dsdgenDir").asInstanceOf[String] // location of dsdgen
    val scaleFactor = prop.get("scaleFactor").asInstanceOf[String] // scaleFactor defines the size of the dataset to generate (in GB).
    val useDoubleForDecimal = prop.get("useDoubleForDecimal").asInstanceOf[String].toBoolean // true to replace DecimalType with DoubleType
    val useStringForDate = prop.get("useStringForDate").asInstanceOf[String].toBoolean // true to replace DateType with StringType

    val dataDir = prop.get("dataDir").asInstanceOf[String] // root directory of location to create data in.
    val format = prop.get("format").asInstanceOf[String] // valid spark format like parquet "parquet".
    val databaseName = prop.get("databaseName").asInstanceOf[String] // name of database to create.
    val overwrite = prop.get("overwrite").asInstanceOf[String].toBoolean
    val discoverPartitions = prop.get("discoverPartitions").asInstanceOf[String].toBoolean

    // Experiment props
    val iterations = prop.get("iterations").asInstanceOf[String].toInt // how many iterations of queries to run.
    val resultLocation = prop.get("resultLocation").asInstanceOf[String] // place to write results
    val forkThread = prop.get("forkThread").asInstanceOf[String].toBoolean
    val timeout = prop.get("timeout").asInstanceOf[String].toInt // timeout, in seconds.

    // Run:
    val tables = new TPCDSTables(ss.sqlContext,
      dsdgenDir = dsdgenDir,
      scaleFactor = scaleFactor,
      useDoubleForDecimal = useDoubleForDecimal,
      useStringForDate = useStringForDate)

    //    tables.genData(
    //      location = dataDir,
    //      format = format,
    //      overwrite = true, // overwrite the data that is already there
    //      partitionTables = true, // create the partitioned fact tables
    //      clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
    //      filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
    //      tableFilter = "", // "" means generate all tables
    //      numPartitions = 100) // how many dsdgen partitions to run - number of input tasks.

    // Create the specified database
    ss.sql(s"create database $databaseName")

    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(dataDir, format, databaseName, overwrite, discoverPartitions)

    // For CBO only, gather statistics on all columns:
    tables.analyzeTables(databaseName, analyzeColumns = true)

    val tpcds = new TPCDS(ss.sqlContext)

    val queries = tpcds.tpcds2_4Queries.filter(query => queryNames.contains(query.name.split("-")(0)))

    // Run:
    ss.sql(s"use $databaseName")
    val experiment = tpcds.runExperiment(
      queries,
      iterations = iterations,
      resultLocation = resultLocation,
      forkThread = forkThread)
    experiment.waitForFinish(timeout)
  }

  def main(args: Array[String]): Unit = {

    val usage =
      """
      Usage: ./bin/spark-submit --class edu.sogang.benchmark.RunBench --jars SPARK_SQL_PERF_JAR_PATH JAR_PATH --config-filename config.properties
      """

    val argList = args.toList
    type ArgMap = Map[Symbol, Any]

    @tailrec
    def parseArgument(map: ArgMap, list: List[String]): ArgMap = {
      list match {
        case Nil =>
          map
        case "--query-names" :: value :: tail =>
          parseArgument(map ++ Map('queryNames -> value), tail)
        case "--config-filename" :: value :: tail =>
          parseArgument(map ++ Map('confFileName -> value), tail)
        case option :: tail =>
          println("Unknown option : " + option)
          println(usage)
          exit(1)
      }
    }

    val parsedArgs = parseArgument(Map(), argList)

    val tpcdsQueryNames =
        "q1,q2,q3,q4,q5,q6,q7,q8,q9,q10," +
        "q11,q12,q13,q14a,q14b,q15,q16,q17,q18,q19," +
        "q20,q21,q22,q23a,q23b,q24a,q24b,q25,q26,q27," +
        "q28,q29,q30,q31,q32,q33,q34,q35,q36,q37," +
        "q38,q39a,q39b,q40,q41,q42,q43,q44,q45,q46,q47," +
        "q48,q49,q50,q51,q52,q53,q54,q55,q56,q57,q58," +
        "q59,q60,q61,q62,q63,q64,q65,q66,q67,q68,q69," +
        "q70,q71,q72,q73,q74,q75,q76,q77,q78,q79," +
        "q80,q81,q82,q83,q84,q85,q86,q87,q88,q89," +
        "q90,q91,q92,q93,q94,q95,q96,q97,q98,q99," +
        "ss_max"

    val queryNames = parsedArgs.getOrElse(Symbol("queryNames"), tpcdsQueryNames).toString.split(",").toSeq

    val conf = new SparkConf()

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")

    conf.registerKryoClasses(
      Array(
        Class.forName("java.lang.invoke.SerializedLambda"),
        Class.forName("scala.Enumeration$Val"),
        Class.forName("scala.math.Ordering$$anon$1"),
        Class.forName("scala.math.BigDecimal$RoundingMode$"),
        Class.forName("scala.reflect.ClassTag$GenericClassTag"),
        Class.forName("org.apache.spark.sql.types.Metadata"),
        Class.forName("org.apache.spark.sql.types.ByteType$"),
        Class.forName("org.apache.spark.sql.types.DateType$"),
        Class.forName("org.apache.spark.sql.types.DecimalType"),
        Class.forName("org.apache.spark.sql.types.DoubleType$"),
        Class.forName("org.apache.spark.sql.types.LongType$"),
        Class.forName("org.apache.spark.sql.types.IntegerType$"),
        Class.forName("org.apache.spark.sql.types.StringType$"),
        Class.forName("org.apache.spark.sql.types.StructType"),
        Class.forName("org.apache.spark.sql.types.StructField"),
        Class.forName("[Lorg.apache.spark.sql.types.StructType;"),
        Class.forName("[Lorg.apache.spark.sql.types.StructField;"),
        Class.forName("org.apache.spark.sql.catalyst.InternalRow"),
        Class.forName("[Lorg.apache.spark.sql.catalyst.InternalRow;"),
        Class.forName("org.apache.spark.sql.execution.joins.LongHashedRelation"),
        Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
        Class.forName("org.apache.spark.sql.types.Decimal$DecimalIsFractional$"),
        Class.forName("org.apache.spark.sql.types.Decimal$DecimalAsIfIntegral$"),
        Class.forName("org.apache.spark.sql.execution.command.PartitionStatistics"),
        Class.forName("org.apache.spark.util.HadoopFSUtils$SerializableFileStatus"),
        Class.forName("org.apache.spark.util.HadoopFSUtils$SerializableBlockLocation"),
        Class.forName("[Lorg.apache.spark.util.HadoopFSUtils$SerializableBlockLocation;"),
        Class.forName("org.apache.spark.sql.execution.datasources.WriteTaskResult"),
        Class.forName("org.apache.spark.sql.execution.datasources.BasicWriteTaskStats"),
        Class.forName("org.apache.spark.sql.execution.datasources.ExecutedWriteSummary"),
        Class.forName("org.apache.spark.sql.catalyst.trees.Origin"),
        Class.forName("org.apache.spark.sql.catalyst.InternalRow$"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.Add"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.Literal"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.Cast"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.Round"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.Divide"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.EqualTo"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.Ascending$"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.CaseWhen"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.Coalesce"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.Descending$"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.NullsFirst$"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.NullsLast$"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.SortOrder"),
        Class.forName("[Lorg.apache.spark.sql.catalyst.expressions.SortOrder;"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.BoundReference"),
        Class.forName("org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering"),
      )
    )

    val ss = SparkSession.builder
      .config(conf)
//      .master("local[*]")
      .getOrCreate()

    val sc = ss.sparkContext
    sc.setLogLevel("WARN")

    try {
      runExperiment(parsedArgs(Symbol("confFileName")).toString, queryNames, ss)
    }
    finally {
      ss.stop()
    }

  }

}
