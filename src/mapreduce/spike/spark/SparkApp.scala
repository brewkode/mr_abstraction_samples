package mapreduce.spike.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

abstract class SparkApp{
  val DEFAULT_SPARK_HOME="/opt/spark"
  val DEFAULT_JAR_FILE="/some/work/dir/this_jar_file"
  def sparkContext(appName: String) = {
    val logLevel = Option(System.getenv("LOG_LEVEL")).getOrElse("WARN")
    Logger.getLogger("org.apache.spark").setLevel(Level.toLevel(logLevel))
    val master = Option(System.getenv("MASTER")).getOrElse("localhost")
    val sparkHome = Option(System.getenv("SPARK_HOME")).getOrElse(DEFAULT_SPARK_HOME)
    val JARS = Option(System.getenv("JARS")).getOrElse(DEFAULT_JAR_FILE).split(",").toList
    new SparkContext(master, appName, sparkHome, JARS)
  }
}