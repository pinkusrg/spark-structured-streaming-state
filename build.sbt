name := "sparkx"

version := "0.1"

scalaVersion := "2.12.11"

val spark2Version = "2.4.6"

val core = "org.apache.spark" %% "spark-core" % spark2Version
val sql = "org.apache.spark" %% "spark-sql" % spark2Version
val streaming = "org.apache.spark" %% "spark-streaming" % spark2Version
val sqlKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % spark2Version


libraryDependencies ++= Seq(
 core,sql,streaming,sqlKafka
)