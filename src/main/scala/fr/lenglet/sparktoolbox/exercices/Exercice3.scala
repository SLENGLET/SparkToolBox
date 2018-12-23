package fr.lenglet.sparktoolbox.exercices

/**
  * Date : 05/01/2018
  * Exercice : read 2 txt files from hdfs, personne.txt,travel.txt, create RDD Personne
  * and RDD Travel, map the RDD to PairRDD , leftOuterJoin  and print the result on driver
  *
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
//import org.apache.spark.HashPartitioner
import com.typesafe.config.ConfigFactory

//i
object Exercice3 {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Exercice3"))

    val pers = ConfigFactory.load().getString("cluster.personne.value")
    val trav = ConfigFactory.load().getString("cluster.travel.value")

    val rddPers = sc.textFile(pers)
    val rddTrav	= sc.textFile(trav)

    val rawPers = rawPersonne(rddPers).map(x => (x.id,x))
    val rawTrav = rawTravel(rddTrav).map(x => (x.id,x))

    val rawUnion = rawPers.leftOuterJoin(rawTrav)


    println("############# BEGIN Print on driver  #############")

    rawUnion.collect().foreach(println)

    println("############# END Print on driver  #############")

  }

  def rawPersonne(lines: RDD[String]): RDD[Personne] =
    lines.map(line => {
      val arr = line.split(",")
      Personne(id=arr(0).toInt,nom=arr(1),prenom=arr(2),age=arr(3).toInt)
    })

  def rawTravel(lines: RDD[String]): RDD[Travel] =
    lines.map(line => {
      val arr = line.split(",")
      Travel(id=arr(0).toInt,country=arr(1),city=arr(2))
    })
}

