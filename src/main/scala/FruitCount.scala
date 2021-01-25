import Example.WordCount
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}

object FruitCount {

  /*
  Input value in topic 'fruit' as any string> apple
                                            > mango
                                            > mango
   */
  case class Fruit(topic:String, value: String)
  case class FruitCountState(fruit:String, count: Long)

  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._
  implicit val fenc: Encoder[Fruit] = Encoders.product[Fruit]
  implicit val fCenc: Encoder[FruitCountState] = Encoders.product[FruitCountState]


  def readAndWriteToKafka() = {
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "fruit")
      .load()

    kafkaDF
      .select(col("topic"), expr("cast(value as string) as value"))
      .as[Fruit]
      .groupByKey(r => r.value)
      .flatMapGroupsWithState[Set[FruitCountState],Fruit](
        outputMode = OutputMode.Append(), timeoutConf = GroupStateTimeout.ProcessingTimeTimeout()
      )(StateHelper.fruitMappingFunction)
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "sink-fruit")
      .option("checkpointLocation", "src/main/resources/c1") // without checkpoints the writing to Kafka will fail
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    readAndWriteToKafka
  }
}
