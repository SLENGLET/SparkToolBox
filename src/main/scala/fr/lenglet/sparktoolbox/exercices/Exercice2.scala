package fr.lenglet.sparktoolbox.exercices

import java.io.IOException

import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import main.scala.fr.lenglet.sparktoolbox.read

/**
  * Date : 07/04/2018
  * Exercice : read messages from kafka topic , print result ( datanode side , not on driver with "collect" )
  *
  */

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.SparkConf
//import _root_.kafka.serializer.DefaultDecoder
//import _root_.kafka.serializer.StringDecoder
//import org.apache.spark.storage.StorageLevel
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.streaming.Duration;
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.unsafe.types.UTF8String


object Exercice2 {
  def main(args: Array[String]) {


    val Array(brokers, zookep, topics) = args


    val sparkConf = new SparkConf().setAppName("Exercice2")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "zookeeper.connect" -> zookep,
      "group.id" -> "default",
      "zookeeper.connection.timeout.ms" -> "1000",
      "security.protocol" -> "SASL_PLAINTEXT",
      "sasl.kerberos.service.name" -> "kafka",
      "auto.offset.reset" -> "latest",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))


    val messages2 = messages.map(x => (x.offset(), x.value()))

    messages2.foreachRDD { rdd =>
      if (!rdd.partitions.isEmpty)

        rdd.foreachPartition(partition =>
          partition.foreach(p => {

            try {

              val hMap = messageToMap(p)

              //val hnom = hMap.getOrElse("nom", "noname")
              //val hpnom = hMap.getOrElse("prenom", "nosurname")
              //val hpid = hMap.getOrElse("id", 999)
              val hnom = hMap.getOrElse("nom", "noname")
              val hpnom = hMap.getOrElse("prenom", "nosurname")
              val hpid = hMap.getOrElse("id", 999)
              //jsonNode.hasNonNull("nom")
              if (hnom.contains("no value") || hpnom.contains("no value") || hpid.equals(999)) {
                println("attributs de messages manquants")
                println("id : " + hpid + "nom : " + hnom + "prenom : " + hpnom)

              } else {
                println("message OK")
                //println("offset" + p.offset() + " topic : " + p.topic() + " Attention Nom extrait du Message json vide")
                println("id : " + hpid + "nom : " + hnom + "prenom : " + hpnom)
              }

            }

            catch {

              case unformat => {
                println("Got this unknown exception: " + mapError(unformat).get("stacktrace"))
              }
              case io: IOException => {
                println("io exception")
              }
              case js: JsonMappingException => {
                println("json exception")
              }
            }
          })
        )
    }

    ssc.start()
    ssc.awaitTermination()
    //ssc.stop()
  }


  def messageToMap(c: (Long, String)): Map[String, String] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val json = c._2
    //record to Hmap
    val hMap = mapper.readValue(json, classOf[Map[String, String]])
    //record to jsonnode
    //val jsonNode = mapper.readTree(json);
    hMap
  }

  def mapError(t: Throwable): Map[String, String] = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val error = t.getMessage.replace('\n', ' ').replace('\r', ' ').replace('\\', '\\')
    val utf8 = UTF8String.fromString(error)
    val json = "{\"id\": 1, \"stacktrace\":\"'" + utf8 + "'\"}"
    //record to Hmap
    val hMap = mapper.readValue(json, classOf[Map[String, String]])
    //record to jsonnode
    //val jsonNode = mapper.readTree(json);
    hMap
  }
}

