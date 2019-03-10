package test

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.FunSuite
import fr.lenglet.sparktoolbox.exercices.Exercice1.rawPersonne


class TestExercice1 extends FunSuite with SharedSparkContext with RDDComparisons {

  test("Test Personne") {

    val list = List("1,\"Dupont\",\"Stef\",44","2,\"Dupont\",\"Raph\",41")
    val rddList = sc.parallelize(list)
    val resultRDD  = rawPersonne(rddList)
    val result = resultRDD.map(x => x.age).reduce(_+_)

    assert(result === 85 )
  }

}
