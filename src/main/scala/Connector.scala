import org.apache.spark.sql.SparkSession

object Connector extends App {

  val spark = SparkSession.builder()
    .appName("BulkEmailSender")
    .config("spark.master", "local")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val host = "*******"
  val port = 5432
  val dbName = "**********"
  val url = s"jdbc:postgresql://$host:$port/$dbName"
  val user = "root"
  val password = "*****"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.*********")
    .load()

  employeesDF.select("firstname","email").write.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "topic1")
    .save()

}
