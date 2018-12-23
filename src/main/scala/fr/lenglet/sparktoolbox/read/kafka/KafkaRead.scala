package main.scala.fr.lenglet.sparktoolbox.read.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.InputDStream
import main.scala.fr.lenglet.sparktoolbox.read.kafka.KafkaReadConfiguration

trait KafkaReadInterface {

  def setTopic(topics: String): Set[String] {

  }

  def setKafkaParams(brokers: String, zookep: String): Map[String, String] {

  }
}

class KafkaRead extends KafkaReadInterface {

  override def setTopic(topics: String): Set[String] = {
    val topicsSet = topics.split(",").toSet
    topicsSet
  }

  override def setKafkaParams(brokers: String, zookep: String): Map[String, String] = {
    val MapKafKaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "zookeeper.connect" -> zookep,
      "group.id" -> KafkaReadConfiguration.groupid,
      "zookeeper.connection.timeout.ms" -> KafkaReadConfiguration.zookeeperconnectiontimeoutms,
      "security.protocol" -> KafkaReadConfiguration.securityprotocol,
      "sasl.kerberos.service.name" -> KafkaReadConfiguration.saslkerberosservicename,
      "auto.offset.reset" -> KafkaReadConfiguration.autooffsetreset,
      "key.deserializer" -> KafkaReadConfiguration.keydeserializer,
      "value.deserializer" -> KafkaReadConfiguration.valuedeserializer)
    MapKafKaParams
  }
}


