package fr.lenglet.sparktoolbox.exercices

import java.io.IOException

import main.scala.fr.lenglet.sparktoolbox.read.hdfs.HdfsRead
import main.scala.fr.lenglet.sparktoolbox.read.hdfs.HdfsReadConfiguration
import main.scala.fr.lenglet.sparktoolbox.write.hdfs.HdfsWrite
import main.scala.fr.lenglet.sparktoolbox.write.hdfs.HdfsWriteConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
//import org.apache.spark.HashPartitioner
import com.typesafe.config.ConfigFactory


object Exercice1 {

  def main(args: Array[String]): Unit = {

    try {
      val sc = new SparkContext(new SparkConf())
      val hdread = new HdfsRead()
      val hdwrite = new HdfsWrite()
      val lines = sc.textFile(hdread.setFileHdfsInput(HdfsReadConfiguration.personne))
      val raw = rawPersonne(lines)
      val rdd1 = raw.map(x => (x.nom, x)).groupByKey()
      rdd1.saveAsTextFile(hdwrite.setFileHdfsOutput(HdfsWriteConfiguration.hdfsfileoutput))
    }
    catch {
      case unformat => {
        println("### unformat exception ###" + unformat)
      }
    }
  }

  def rawPersonne(lines: RDD[String]): RDD[Personne] =
    lines.map(line => {
      val arr = line.split(",")
      Personne(id = arr(0).toInt, nom = arr(1), prenom = arr(2), age = arr(3).toInt)
    })
}
