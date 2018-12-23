package fr.lenglet.sparktoolbox.exercices

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
//import org.apache.spark.HashPartitioner
import com.typesafe.config.ConfigFactory



object Exercice1 {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf())
    val valuein = ConfigFactory.load().getString("cluster.personne.value")
    val valueout = ConfigFactory.load().getString("cluster.hdfsfileoutput.value")
    val lines = sc.textFile(valuein)
    val raw = rawPersonne(lines)
    val rdd1 = raw.map(x => (x.nom,x)).groupByKey()
    rdd1.saveAsTextFile(valueout)

  }

  def rawPersonne(lines: RDD[String]): RDD[Personne] =
    lines.map(line => {
      val arr = line.split(",")
      Personne(id=arr(0).toInt,nom=arr(1),prenom=arr(2),age=arr(3).toInt)
    })

}
