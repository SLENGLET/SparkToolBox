package test

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.FunSuite
import fr.lenglet.sparktoolbox.exercices.Exercice1.rawPersonne


class TestExercice1 extends FunSuite with SharedSparkContext with RDDComparisons {

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
