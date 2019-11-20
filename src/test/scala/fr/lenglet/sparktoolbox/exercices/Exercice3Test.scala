package fr.lenglet.sparktoolbox.exercices

import com.holdenkarau.spark.testing.SharedSparkContext
import _root_.fr.lenglet.sparktoolbox.exercices.Exercice3.{rawPersonne, rawTravel}
import org.scalatest.FunSuite


class Exercice3Test extends FunSuite with SharedSparkContext {

  test("Test Union rawPersonne rawTravel") {


    val rddP = sc.textFile("src/main/resources/exercices/personne.txt")
    val rddT = sc.textFile("src/main/resources/exercices/travel.txt")

    val expected1 = "Hadoop"
    val expected2 = "Luigi"

    val resultRDD = rawPersonne(rddP)
    val resultRDDT = rawTravel(rddT)

    val rawPers = resultRDD.map(x => (x.id, (x.nom, x.prenom)))
    val rawTrav = resultRDDT.map(x => (x.id, (x.city, x.country)))

    val rawUnion = rawPers.leftOuterJoin(rawTrav)

    // Test 3.1
    assert(rawUnion.count() == 10)
    var rawGroup = rawUnion.groupByKey()

    // Test 3.2
    assert(rawGroup.count() == 5)

    rawGroup.collect().foreach(x => {

      if (x._1 == 4) {

        var test =  x._2.toList
        // Test 3.3
        assert(test(0)._1._1  === expected1)
        assert(test(0)._1._2  === expected2)
      }
    })
  }
}
