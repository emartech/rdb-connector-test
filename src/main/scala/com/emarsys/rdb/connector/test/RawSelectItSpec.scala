package com.emarsys.rdb.connector.test

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.Materializer
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Connector
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._

/*
For positive results use the A and B table definitions and preloaded data defined in the SimpleSelect.
*/

trait RawSelectItSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val executionContext: ExecutionContextExecutor

  implicit val materializer: Materializer

  val connector: Connector

  override def afterAll(): Unit = ()

  override def beforeAll(): Unit = ()

  val simpleSelect: String
  val badSimpleSelect: String
  val simpleSelectNoSemicolon: String

  val uuid = uuidGenerate

  val postfixTableName = s"_raw_select_table_$uuid"

  val aTableName = s"a$postfixTableName"
  val bTableName = s"b$postfixTableName"

  val awaitTimeout = 5.seconds


  s"RawSelectItSpec $uuid" when {

    "#rawSelect" should {
      "list table values" in {

        val result = getStreamResult(connector.rawSelect(simpleSelect, None))

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v1", "1", "1"),
          Seq("v2", "2", "0"),
          Seq("v3", "3", "1"),
          Seq("v4", "-4", "0"),
          Seq("v5", null, "0"),
          Seq("v6", "6", null),
          Seq("v7", null, null)
        ))
      }

      "list table values with limit" in {

        val limit = 2

        val result = getStreamResult(connector.rawSelect(simpleSelect, Option(limit)))

        result.size shouldEqual limit + 1
      }

      "fail the future if query is bad" in {

        assertThrows[Exception](Await.result(connector.rawSelect(badSimpleSelect, None).flatMap(_.right.get.runWith(Sink.seq)), awaitTimeout))
      }
    }

    "#validateProjectedRawSelect" should {
      "return ok if ok" in {
        Await.result(connector.validateProjectedRawSelect(simpleSelect, Seq("A1")), awaitTimeout) shouldBe Right()
      }

      "return ok if no ; in query" in {
        Await.result(connector.validateProjectedRawSelect(simpleSelectNoSemicolon, Seq("A1")), awaitTimeout) shouldBe Right()
      }

      "return error if not ok" in {
        Await.result(connector.validateProjectedRawSelect(simpleSelect, Seq("NONEXISTENT_COLUMN")), awaitTimeout) shouldBe a[Left[_, _]]
      }
    }

    "#validateRawSelect" should {
      "return ok if ok" in {
        Await.result(connector.validateRawSelect(simpleSelect), awaitTimeout) shouldBe Right()
      }

      "return ok if no ; in query" in {

        Await.result(connector.validateRawSelect(simpleSelectNoSemicolon), awaitTimeout) shouldBe Right()
      }

      "return error if not ok" in {

        Await.result(connector.validateRawSelect(badSimpleSelect), awaitTimeout) shouldBe a[Left[_, _]]
      }
    }

    "#projectedRawSelect" should {

      "project one col as expected" in {

        val result = getStreamResult(connector.projectedRawSelect(simpleSelect, Seq("A1")))

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1"),
          Seq("v1"),
          Seq("v2"),
          Seq("v3"),
          Seq("v4"),
          Seq("v5"),
          Seq("v6"),
          Seq("v7")
        ))

      }

      "project more col as expected and allow null values" in {
        val result = getStreamResult(connector.projectedRawSelect(simpleSelect, Seq("A2", "A3"), allowNullFieldValue = true))

        checkResultWithoutRowOrder(result, Seq(
          Seq("A2", "A3"),
          Seq("1", "1"),
          Seq("2", "0"),
          Seq("3", "1"),
          Seq("-4", "0"),
          Seq(null, "0"),
          Seq("6", null),
          Seq(null, null)
        ))
      }

      "project more col as expected and disallow null values" in {
        val result = getStreamResult(connector.projectedRawSelect(simpleSelect, Seq("A2", "A3")))

        checkResultWithoutRowOrder(result, Seq(
          Seq("A2", "A3"),
          Seq("1", "1"),
          Seq("2", "0"),
          Seq("3", "1"),
          Seq("-4", "0")
        ))
      }
    }
  }

  def checkResultWithoutRowOrder(result: Seq[Seq[String]], expected: Seq[Seq[String]]): Unit = {
    result.size shouldEqual expected.size
    result.head.map(_.toUpperCase) shouldEqual expected.head.map(_.toUpperCase)
    result.foreach(expected contains _)
  }

  def getStreamResult(s: ConnectorResponse[Source[Seq[String], NotUsed]]): Seq[Seq[String]] = {
    val resultE = Await.result(s, awaitTimeout)

    resultE shouldBe a[Right[_, _]]
    val resultStream: Source[Seq[String], NotUsed] = resultE.right.get

    Await.result(resultStream.runWith(Sink.seq), awaitTimeout)
  }
}
