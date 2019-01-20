package main.scala.fr.lenglet.sparktoolbox.write.orientdb

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph
import org.apache.tinkerpop.gremlin.structure.T


trait OrientDBWriteInterface extends Serializable {

  def saveVertex(OG: OrientGraph, Oname: String, Oclass: String): Unit {
  }

  def saveEdge(id: String): Unit {
  }
}


class OrientDBWrite extends OrientDBWriteInterface {

  override def saveVertex(OG: OrientGraph, Oname: String, Oclass: String): Unit = {

    OG.addVertex(T.label,Oclass,"name",""+Oname+"")
  }

  override def saveEdge(id: String): Unit = {
  }
}
