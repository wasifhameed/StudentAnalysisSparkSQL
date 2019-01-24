package exercises

import model.Student
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object studentClassAnalytics extends App {


  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Project - Class Analytics")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  val sqlContext = spark.sqlContext

  import spark.implicits._


  val dataRDD = sc.textFile("students.txt").map(l => l.split(",")).map(w => Row.fromSeq(w))



  val schema = Seq[Student]().toDF.schema
  val df = spark.createDataFrame(dataRDD, schema)
  val df2 = castColumnsType(df)
  df2.printSchema

  //The underscore (must have a space in between function and underscore)
  //turns the function into a partially applied function
  sqlContext.udf.register("genderColumnClean", genderColumnClean _)
  sqlContext.udf.register("sqlColumnClean", sqlColumnClean _)
  sqlContext.udf.register("nosqlColumnClean", nosqlColumnClean _)
  sqlContext.udf.register("jobInterviewColumnClean", jobInterviewColumnClean _)
  sqlContext.udf.register("cloudColumnClean", cloudColumnClean _)
  sqlContext.udf.register("hadoopColumnClean", hadoopColumnClean _)

  val myExpression = "genderColumnClean(gender)"
  val sqlExpression ="sqlColumnClean(isGoodAtSQL)"
  val nosqlExpression ="nosqlColumnClean(isGoodAtNoSQL)"
  val jobInterviewExpression ="jobInterviewColumnClean(hasJobInterviews)"
  val cloudExpression = "cloudColumnClean(isGoodAtCloud)"
  val hadoopExpression = "hadoopColumnClean(isGoodAtHadoop)"

  val df3 = df2.withColumn("gender", expr(myExpression))
  val dfClean01= df3.withColumn("isGoodAtSQL",expr(sqlExpression))
  val dfClean02= dfClean01.withColumn("isGoodAtNoSQL",expr(nosqlExpression))
  val dfClean03= dfClean02.withColumn("hasJobInterviews",expr(jobInterviewExpression))
  val dfClean04= dfClean03.withColumn("isGoodAtCloud",expr(cloudExpression))
  val dfFinal= dfClean04.withColumn("isGoodAtHadoop",expr(hadoopExpression))

  dfFinal.show()


  //Count of total number of siblings
  val siblings = dfFinal.select(sum(col("siblings"))).withColumnRenamed("sum(siblings)","Total Number of Siblings")
  siblings.show()

  // count of pople who know sql but dont know nosql and have job interviews
  val sqlOnlyAndHasInterview= dfFinal.where($"isGoodAtSQL" === "Y" and $"isGoodAtNoSQL" === "" and $"hasJobInterviews" ==="Y").count()
  print("The number of people who know SQL, but don't know NoSQL and has Job Interview is ", sqlOnlyAndHasInterview)
  print("\n")



  //Top 10 Technology knowing people. If two people kno same number of technologies, order by hours of daily work

  val topFivePeopleByTechnology =dfFinal.withColumn("Technologies" , concat(col("isGoodAtSQL"), col("isGoodAtNoSQL"), col("isGoodAtHadoop"),col("isGoodAtCloud") )).orderBy($"Technologies".desc,$"hourDailyWorks".desc).limit(10)
  topFivePeopleByTechnology.show()

  val ff = dfFinal.groupBy($"gender").agg(avg($"hourDailyRest"),avg($"hourDailySports"))
  ff.show()
  val d = ff.withColumn("Leisure", $"avg(hourDailyRest)" + $"avg(hourDailySports)")
  d.show()
  /**
    * Cast columns to different type than String; Keep the same name for the column
    * If a value cannot be casted, a null will be returned
    *
    * @param df
    * @return
    */
  def castColumnsType(df: DataFrame) = df.select(
    df.columns.map {
      case age @ "age" => df(age).cast(IntegerType).as(age)
      case siblings @ "siblings" => df(siblings).cast(IntegerType).as(siblings)
      case yearIT @ "yearIT" => df(yearIT).cast(IntegerType).as(yearIT)
      case hourDailyStudy @ "hourDailyStudy" => df(hourDailyStudy).cast(IntegerType).as(hourDailyStudy)
      case hourDailyRest @ "hourDailyRest" => df(hourDailyRest).cast(IntegerType).as(hourDailyRest)
      case hourDailySports @ "hourDailySports" => df(hourDailySports).cast(IntegerType).as(hourDailySports)
      case hourDailyWorks @ "hourDailyWorks" => df(hourDailyWorks).cast(IntegerType).as(hourDailyWorks)
      case hourDailySleep @ "hourDailySleep" => df(hourDailySleep).cast(IntegerType).as(hourDailySleep)
      case spokenLanguageCount @ "spokenLanguageCount" => df(spokenLanguageCount).cast(IntegerType).as(spokenLanguageCount)
      case other         => df(other)
    }: _*
  )

  def booleanColumnClean(isGoodAtSQL: String): String =  isGoodAtSQL.trim match{
    case null => null
    case "y" => "Y"
    case "n" => ""
    case "yes" => "Y"
    case "no" => ""
    case "Y" => "Y"
    case "N" => ""
    case "Yes" => "Y"
    case "No" => ""
    case "YES" => "Y"
    case "NO" => ""
    case "not yet" => ""
    case _ => null
  }
  def sqlColumnClean(isGoodAtSQL: String): String =  isGoodAtSQL.trim match{
    case null => null
    case "y" => "Y"
    case "n" => ""
    case "yes" => "Y"
    case "no" => ""
    case "Y" => "Y"
    case "N" => ""
    case "Yes" => "Y"
    case "No" => ""
    case "YES" => "Y"
    case "NO" => ""
    case "not yet" => ""
    case _ => null
  }
  def nosqlColumnClean(isGoodAtNoSQL: String): String =  isGoodAtNoSQL.trim match{
    case null => null
    case "y" => "Y"
    case "n" => ""
    case "yes" => "Y"
    case "no" => ""
    case "Y" => "Y"
    case "N" => ""
    case "Yes" => "Y"
    case "No" => ""
    case "YES" => "Y"
    case "NO" => ""
    case "not yet" => ""
    case _ => null
  }
  def cloudColumnClean(isGoodAtCloud: String): String =  isGoodAtCloud.trim match{
    case null => null
    case "y" => "Y"
    case "n" => ""
    case "yes" => "Y"
    case "no" => ""
    case "Y" => "Y"
    case "N" => ""
    case "Yes" => "Y"
    case "No" => ""
    case "YES" => "Y"
    case "NO" => ""
    case "not yet" => ""
    case _ => null
  }
  def hadoopColumnClean(isGoodAtHadoop: String): String =  isGoodAtHadoop.trim match{
    case null => null
    case "y" => "Y"
    case "n" => ""
    case "yes" => "Y"
    case "no" => ""
    case "Y" => "Y"
    case "N" => ""
    case "Yes" => "Y"
    case "No" => ""
    case "YES" => "Y"
    case "NO" => ""
    case "not yet" => ""
    case _ => null
  }
  def jobInterviewColumnClean(hasJobInterviews: String): String =  hasJobInterviews.trim match{
    case null => null
    case "y" => "Y"
    case "n" => ""
    case "yes" => "Y"
    case "no" => ""
    case "Y" => "Y"
    case "N" => ""
    case "Yes" => "Y"
    case "No" => ""
    case "YES" => "Y"
    case "NO" => ""
    case "not yet" => ""
    case _ => null
  }
  def genderColumnClean(gender: String): String = gender.trim match {
    case null => null
    case "MALE" | "Male" | "M" | "m" | "male" => "M"
    case "f" | "female" | "Female" | "F" => "F"
    case _ => null
  }





}