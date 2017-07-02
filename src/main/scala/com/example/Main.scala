package com.example

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._

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

    // Nettoyages des caractères à la con. Remplacer &lt; et &gt;
    val dfCleaned = dfWithHeader.withColumn("Tags",regexp_replace(col("Tags"),"&lt;","")).withColumn("Tags",regexp_replace(col("Tags"),"&gt;"," "))

    // Créer un DF id, creation, date
    val dfShort = dfCleaned.select("PostTypeId","CreationDate","Tags")

    // Filtre ceux qui sont des questions PostTypeId = 1
    val dfQuestions = dfShort.filter($"PostTypeId" === 1)

    // Garder ceux qui contiennent le mot scala
    val dfScala = dfQuestions.filter($"Tags".contains("scala"))

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


    // Récupérer la même chose avec une liste de languages


    // Prévision de chacun des languages pour les 3 ans à venir


    // Affichage sur un graphique


    df.show




  }

}
