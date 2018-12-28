package main.scala.fr.lenglet.sparktoolbox.read.hdfs

import com.typesafe.config.ConfigFactory

trait HdfsReadInterface {
  def setFileHdfsInput(valuein: String): String {

  }

}

class HdfsRead extends HdfsReadInterface {
  override def setFileHdfsInput(vIn: String): String = {
    val valuein = ConfigFactory.load().getString(vIn)
    valuein
  }
}
