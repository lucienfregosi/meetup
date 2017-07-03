package com.example

import java.text.SimpleDateFormat

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.Vectors


object Main {

  case class Post(PostTypeId: String, CreationDate: String, Tags: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Stack Overflow Analysis")
      .master("local[2]")
      .getOrCreate()

    import spark.sqlContext.implicits._

    //val df = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/home/lucien/dev/meetup/meetupStack/resources/data/post.csv")


    val df = spark.read.csv("/home/lucien/dev/meetup/meetupStack/resources/data/post.csv")
    val header = Seq("Id","PostTypeId","AcceptedAnswerId","ParentId","CreationDate",
      "DeletionDte","Score","ViewCount","Body","OwnerUserId","OwnerDisplayName",
      "LastEditorUserId","LastEditorDisplayName","LastEditDate","LastActivityDate",
      "Title","Tags","AnswerCount","CommentCount","FavoriteCount","ClosedDate",
      "CommunityOwnedDate")


    val dfWithHeader = df.toDF(header: _*)

    // Nettoyages des caractères à la con. Remplacer &lt; et &gt;
    val dfCleaned = dfWithHeader.withColumn("Tags",regexp_replace(col("Tags"),"&lt;","")).withColumn("Tags",regexp_replace(col("Tags"),"&gt;"," "))

    // Créer un DF id, creation, date
    val dfShort = dfCleaned.select("PostTypeId","CreationDate","Tags")

    // Filtre ceux qui sont des questions PostTypeId = 1
    val dfQuestions = dfShort.filter($"PostTypeId" === 1)

    // Garder ceux qui contiennent le mot scala

    val dfScala = dfQuestions.filter($"Tags".contains("grammaire")).as[Post]

    // Calculer le nombre d'autres tages
    val dsPost = dfQuestions.select("Tags").as[String]


    // TODO enlever ceux qui sont scala
    // Split des tags et groupement par tag
    val dsTag = dsPost.flatMap(x => x.split(" "))
    val dsGroupTag = dsTag.groupByKey(_.toLowerCase)

    // Petit hack sur le nom du groupement
    // Comptage et tri de la liste de tags
    val counts = dsGroupTag.count().withColumnRenamed("count(1)","cnt").sort(desc("cnt"))

    // Faire la distribution mensuelle
    val srcDateFormat   = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val targetDateFormat = new SimpleDateFormat("yyyy-MM")
    val dfDistributedMonthly = dfScala.map(x => targetDateFormat.format(srcDateFormat.parse(x.CreationDate.toString)))


    // On les groupe par mois
    val dfDateGrouped = dfDistributedMonthly.groupBy("value").count().sort("value").toDF().select("count").as[Long]
    //dfDateGrouped.coalesce(1).write.csv("test.csv")


    // The dataset is sampled from an ARIMA(1, 0, 1) model generated in R.

    val ts = Vectors.dense(dfDateGrouped.map(_.toDouble).collect().toList.toArray)
    val arimaModel = ARIMA.fitModel(1, 0, 0, ts)
    println("coefficients: " + arimaModel.coefficients.mkString(","))
    val forecast = arimaModel.forecast(ts, 50)
    println("forecast of next 50 observations: " + forecast.toArray.mkString(","))


    // Trou
    // Faire une liste de Trois languages
    // Enregistrer dans Elasticsearch
    // Voir les requetes sur kibana








  }

}
