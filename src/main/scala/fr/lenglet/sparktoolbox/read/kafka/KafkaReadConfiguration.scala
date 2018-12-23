package main.scala.fr.lenglet.sparktoolbox.read.kafka

object KafkaReadConfiguration {

  val groupid ="default"
  val zookeeperconnectiontimeoutms = "1000"
  val securityprotocol ="SASL_PLAINTEXT"
  val saslkerberosservicename = "kafka"
  val autooffsetreset = "latest"
  val keydeserializer = "org.apache.kafka.common.serialization.StringDeserializer"
  val valuedeserializer="org.apache.kafka.common.serialization.StringDeserializer"

}
