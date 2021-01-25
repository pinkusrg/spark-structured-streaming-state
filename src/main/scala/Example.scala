import StateHelper.MappingFunction
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.plans.logical.NoTimeout
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}

object Example {

  case class Word(value:String)
  case class WordCount(value:String,count:Long)
  case class WordCountState(value:String,count:Long)

  val checkpointLocation = "file:///home/knoldus/Documents/c1"

  val spark = SparkSession.builder()
    .appName("Streaming Aggregations")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setCheckpointDir(checkpointLocation)

  def streamingCount() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount: DataFrame = lines.selectExpr("count(*) as lineCount")

    // aggregations with distinct are not supported
    // otherwise Spark will need to keep track of EVERYTHING

    lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregations without watermark
      .start()
      .awaitTermination()
  }

  def wordCount() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()



    import spark.implicits._
    implicit val enc: Encoder[WordCount] = Encoders.product[WordCount]
    implicit val enc1: Encoder[Word] = Encoders.product[Word]

    val socketDs = lines.as[String]
    val wordsDs =  socketDs.flatMap(value => value.split(" ")).as[Word]
//    val countDs = wordsDs.groupBy("value").count().as[WordCount]

    GroupStateTimeout.EventTimeTimeout()
    wordsDs
      .groupByKey(r => r.value)
      .flatMapGroupsWithState[Set[WordCountState],WordCountState](
        outputMode = OutputMode.Append(), timeoutConf = GroupStateTimeout.ProcessingTimeTimeout()
      )(MappingFunction)
      .writeStream
      .format("json")
      .option("path", "file:///home/knoldus/Documents/spark_output")
//      .format("console")
      .option("checkpointLocation",checkpointLocation)
      .outputMode(OutputMode.Append())
      //      .outputMode("append") // append and update not supported on aggregations without watermark
      .start()
      .awaitTermination()
  }
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    wordCount

//    println(List(WordCount("a",2),WordCount("a",2)))
//    println(Set(WordCount("a",2),WordCount("a",2)))

  }
}
