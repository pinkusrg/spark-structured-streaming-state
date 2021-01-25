import org.apache.spark.sql.SparkSession

object A1 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("directDrop")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
//
//    val simpleData = Seq(("James", "Sales", 3000),
//      ("Michael", "Sales", 4600),
//      ("Robert", "Sales", 4100),
//      ("Maria", "Finance", 3000),
//      ("James", "Sales", 3000),
//      ("Scott", "Finance", 3300),
//      ("Jen", "Finance", 3900),
//      ("Jeff", "Marketing", 3000),
//      ("Kumar", "Marketing", 2000),
//      ("Saif", "Sales", 4100)
//    )


    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 2),
      ("Saif", "Sales", 2),
      ("Kumar", "Marketing", 2),
    )

    val df = simpleData.toDF("employee_name", "department", "salary")
    df.show()

    //Distinct all columns
    val distinctDF = df.distinct()
    println("Distinct count: "+distinctDF.count())
    distinctDF.show(false)

    val df2 = df.dropDuplicates()
    println("Distinct count: "+df2.count())
    df2.show(false)

    //Distinct using dropDuplicates
    val dropDisDF = df.dropDuplicates("department")
    println("Distinct count of department & salary : "+dropDisDF.count())
    dropDisDF.show(false)

  }
}
