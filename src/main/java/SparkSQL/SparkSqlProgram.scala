package SparkSQL

import config.SparkConfig
import model.{Data, Person}

object SparkSqlProgram{
    def main(args: Array[String]): Unit ={
      System.out.println("------Spark Sql Program Starting:-----");

      val sparkConfig= new SparkConfig
      import sparkConfig.sqlContext.implicits._

      //Reading a list of people from txt files
      val people=sparkConfig.sparkContext.textFile(
        "/Users/Olivier/Desktop/Hamubere/SparkExample/src/main/resources/data/input.txt",
        1);
      println("people")
      people.foreach(println)
      //Reading from csv files

      val data= sparkConfig.sparkContext.textFile(
        "/Users/Olivier/Desktop/Hamubere/SparkExample/src/main/resources/data/aids.csv")


      val persons=people.map(l=>l.split(",").map(_.trim()))
      val person=persons.map(p=>Person(p(0),p(1),p(2).toInt))
      // person.foreach(p=>println(p.firstName))

      val info= data.map(inf=>inf.split(",").map(_.trim))
      val information=info.map(i=>Data(i(0),i(1),i(2),i(3), i(4), i(5), i(6)))


    // lines.map(line->{line.length();}).collect();
     val personDF =person.toDF//Dataframe= RDD+Schema
      personDF.registerTempTable("Person")

      val informationDF= information.toDF
          informationDF.registerTempTable("Data")

      println("People Information:")
      println(sparkConfig.sqlContext.sql("select * from Person"))

      println()
      println("Data information:")
      println("Data information 1:\n"+sparkConfig.sqlContext.sql("select * from Data where quarter=1").collectAsList())

      println("Data information 2:\n"+sparkConfig.sqlContext.sql("select count(*) from Data group by quarter").collectAsList())


      println("-----Closing Spark SQL Program---")
    }
}