package main.scala.fr.lenglet.sparktoolbox.write.kafka

import org.apache.kafka.clients.producer._
import java.util.Properties

//https://gist.github.com/fancellu/f78e11b1808db2727d76
//https://www.programcreek.com/scala/index.php?api=org.apache.kafka.clients.producer.ProducerRecord


trait KafkaWriteInterface extends Serializable {
  def setTopic(topics: String): Set[String] {

  }

  def setKafkaParams(brokers: String, zookep: String): Map[String, String] {

  }

  def writeMessages(bootstrap: String, mess: String, key: String): Unit {


  }
}

class KafkaWrite extends KafkaWriteInterface {
  override def setTopic(topics: String): Set[String] = {
    val topicsSet = topics.split(",").toSet
    topicsSet
  }

  override def setKafkaParams(brokers: String, zookep: String): Map[String, String] = {
    val MapKafKaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "zookeeper.connect" -> zookep,
      "group.id" -> KafkaWriteConfiguration.groupid,
      "zookeeper.connection.timeout.ms" -> KafkaWriteConfiguration.zookeeperconnectiontimeoutms,
      "security.protocol" -> KafkaWriteConfiguration.securityprotocol,
      "sasl.kerberos.service.name" -> KafkaWriteConfiguration.saslkerberosservicename,
      "auto.offset.reset" -> KafkaWriteConfiguration.autooffsetreset,
      "key.deserializer" -> KafkaWriteConfiguration.keyserializer,
      "value.deserializer" -> KafkaWriteConfiguration.valueserializer)
    MapKafKaParams
  }

  def writeMessages(bootstrap: String, mess: String, key: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer", KafkaWriteConfiguration.keyserializer)
    props.put("value.serializer", KafkaWriteConfiguration.valueserializer)
    props.put("request.required.acks", KafkaWriteConfiguration.requestrequiredacks)
    props.put("security.protocol", KafkaWriteConfiguration.securityprotocol)
    props.put("producer.type", KafkaWriteConfiguration.producertype)
    props.put("sasl.kerberos.service.name", KafkaWriteConfiguration.saslkerberosservicename)
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord(KafkaWriteConfiguration.topicoutput, key, mess)
    producer.send(record)
    producer.close()
  }
}


