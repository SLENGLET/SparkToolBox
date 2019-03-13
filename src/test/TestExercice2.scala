package test

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.FunSuite
import fr.lenglet.sparktoolbox.exercices.Exercice2.messageToMap


class TestExercice2 extends FunSuite {

  test("Test messageToMap match OK") {
    val jsonTest = "{\"id\": 1,  \"nom\":\"unit\", \"prenom\":\"test\"}"

    val hMap = messageToMap(1,jsonTest)
    val hnom = hMap.getOrElse("nom", "noname")
    val hprenom = hMap.getOrElse("prenom", "nosurname")
    val hpid = hMap.getOrElse("id", 999)

    assert(hnom === "unit" )
    assert(hprenom === "test" )
    assert(hpid === 1 )
  }

  test("Test messageToMap match KO") {
    val jsonTest = "{\"ID\": 1,  \"NOM\":\"unit\", \"PRENOM\":\"test\"}"

    val hMap = messageToMap(1,jsonTest)
    val hnom = hMap.getOrElse("nom", "noname")
    val hprenom = hMap.getOrElse("prenom", "nosurname")
    val hpid = hMap.getOrElse("id", 999)

    assert(hnom === "noname" )
    assert(hprenom === "nosurname" )
    assert(hpid === 999 )
  }
}

