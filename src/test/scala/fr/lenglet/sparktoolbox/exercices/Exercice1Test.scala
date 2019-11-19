package test.scala.fr.lenglet.sparktoolbox.exercices

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import fr.lenglet.sparktoolbox.exercices.Exercice1.rawPersonne
import org.scalatest.FunSuite


class Exercice1Test extends FunSuite with SharedSparkContext with RDDComparisons {

  test("Test rawPersonne") {

    val list = List("1,\"Dupont\",\"Stef\",44", "2,\"Dupont\",\"Raph\",41")
    val list2 = List("2,\"Dupont\",\"Raph\",41", "1,\"Dupont\",\"Stef\",44")
    val rddList = sc.parallelize(list)
    val rddList2 = sc.parallelize(list2)
    val resultRDD = rawPersonne(rddList)
    val resultRDD2 = rawPersonne(rddList2)

    val result = resultRDD.map(x => x.age).reduce(_ + _)
    val result2 = resultRDD.map(x => x.age).reduce(_ + _)

    assertRDDEquals(resultRDD, resultRDD2)
    assert(result === 85)
    assert(result2 === 85)
  }
}
