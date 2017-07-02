package com.example

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Stack Overflow Analysis")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    //val df = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/lucien/dev/meetup/meetupStack/resources/data/post.csv")


    val df = spark.read.csv("/home/lucien/dev/meetup/meetupStack/resources/data/post.csv")
    val header = Seq("Id","PostTypeId","AcceptedAnswerId","ParentId","CreationDate",
      "DeletionDte","Score","ViewCount","Body","OwnerUserId","OwnerDisplayName",
      "LastEditorUserId","LastEditorDisplayName","LastEditDate","LastActivityDate",
      "Title","Tags","AnswerCount","CommentCount","FavoriteCount","ClosedDate",
      "CommunityOwnedDate")

    val dfWithHeader = df.toDF(header: _*)

    // Nettoyages des caractères à la con

    // Créer un DF id, creation, date where contains scala
    val dfScala = dfWithHeader.filter($"tags".contains("scala"))

    dfScala.count



    df.show




  }

}
