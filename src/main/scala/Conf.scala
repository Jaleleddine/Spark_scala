import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, not, lower, desc}
import com.typesafe.config.{Config, ConfigFactory}

/**
 * @author FlorentF9
 * Example application with configuration file.
 */
object Conf {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils\\bin\\winutils.exe")
    System.setProperty("hadoop.home.dir", "C:\\Users\\HAJLAOUI\\Documents\\Kafka_pluralsight_Exercices\\hadoop-2.10.0\\hadoop-2.10.0\\bin")

    // Load configuration into Settings class
    val conf: Config = ConfigFactory.load()

    val settings: Settings = new Settings(conf)

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Word Count (config)")
      .getOrCreate()
    import spark.implicits._

    // Business logic
    val document = spark.read.textFile(settings.inputFile)

    val result   = document.flatMap(_.split(" "))
      .filter(not(lower(col("value")).isin(settings.stopWords: _*)))
      .groupBy("value")
      .count()
      .filter(col("count") >= settings.minCount)
      .orderBy(desc("count"))
    //result.coalesce(1).write.csv(settings.outputFile)
    //result.write.csv(settings.outputFile)
    //result  ("csv").save(settings.outputFile)
    //result.coalesce(1).write.option("header", "true").csv("sample_file.csv")
    println(result.collect())
    //spark.stop()

  }
}
