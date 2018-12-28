package main.scala.fr.lenglet.sparktoolbox.write.hdfs

import com.typesafe.config.ConfigFactory


trait HdfsWriteInterface {
  def setFileHdfsOutput(valueout: String): String {

  }

}

class HdfsWrite extends HdfsWriteInterface {
  override def setFileHdfsOutput(vOut: String): String = {
    val valueout = ConfigFactory.load().getString(vOut)
    valueout
  }

}