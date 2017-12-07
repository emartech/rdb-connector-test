package com.emarsys.rdb.connector.test

import java.util.UUID

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

/*
For positive test results you need to implement an initDb function which creates two tables with the given names and
columns and must insert the sample data.

Tables:
A(A1: string, A2: ?int, A3: ?boolean)
B(B1: string, B2: string, B3: string, B4: ?string)

(We will reuse these table definitions with these data.
Please use unique and not null constraint on A1
If you want to reuse them too and your DB has indexes add index to A3 pls.)

Sample data:
A:
  ("v1", 1, true)
  ("v2", 2, false)
  ("v3", 3, true)
  ("v4", -4, false)
  ("v5", NULL, false)
  ("v6", 6, NULL)
  ("v7", NULL, NULL)

B:
  ("b,1", "b.1", "b:1", "b\"1")
  ("b;2", "b\\2", "b'2", "b=2")
  ("b!3", "b@3", "b#3", NULL)
  ("b$4", "b%4", "b 4", NULL)
 */
trait SimpleSelectItSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  val uuid = UUID.randomUUID().toString

  val postfixTableName = s"_simple_select_table_$uuid"

  val aTableName = s"a$postfixTableName"
  val bTableName = s"b$postfixTableName"
  val connector: Connector

  val awaitTimeout = 5.seconds

  implicit val materializer: Materializer

  override def beforeAll(): Unit = {
    initDb()
  }

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
  }

  def initDb(): Unit

  def cleanUpDb(): Unit

  def checkResultWithoutRowOrder(result: Seq[Seq[String]], expected: Seq[Seq[String]]): Unit = {
    result.size shouldEqual expected.size
    result.head.map(_.toUpperCase) shouldEqual expected.head.map(_.toUpperCase)
    result.foreach(expected contains _)
  }

  def getSimpleSelectResult(simpleSelect: SimpleSelect): Seq[Seq[String]] = {
    val resultE = Await.result(connector.simpleSelect(simpleSelect), awaitTimeout)

    resultE shouldBe a[Right[_, _]]
    val resultStream: Source[Seq[String], NotUsed] = resultE.right.get

    Await.result(resultStream.runWith(Sink.seq), awaitTimeout)
  }

  s"SimpleSelectItSpec $uuid" when {

    "#simpleSelect FIELDS" should {
      "list table values" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName))

        val result = getSimpleSelectResult(simpleSelect)

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

      "list table with specific values" in {
        val simpleSelect = SimpleSelect(AllField, TableName(bTableName))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("B1", "B2", "B3", "B4"),
          Seq("b,1", "b.1", "b:1", "b\"1"),
          Seq("b;2", "b\\2", "b'2", "b=2"),
          Seq("b!3", "b@3", "b#3", null),
          Seq("b$4", "b%4", "b 4", null)
        ))
      }

      "list table values with specific fields" in {
        val simpleSelect = SimpleSelect(SpecificFields(Seq(FieldName("A3"), FieldName("A1"))), TableName(aTableName))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A3", "A1"),
          Seq("1", "v1"),
          Seq("0", "v2"),
          Seq("1", "v3"),
          Seq("0", "v4"),
          Seq("0", "v5"),
          Seq(null, "v6"),
          Seq(null, "v7")
        ))
      }

    }
    "#simpleSelect LIMIT" should {

      "list table values with LIMIT 2" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName), limit = Some(2))

        val result = getSimpleSelectResult(simpleSelect)

        result.size shouldEqual 3
        result.head.map(_.toUpperCase) shouldEqual Seq("A1", "A2", "A3").map(_.toUpperCase)
      }
    }

    "#simpleSelect simple WHERE" should {
      "list table values with IS NULL" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName), where = Some(IsNull(FieldName("A2"))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v5", null, "0"),
          Seq("v7", null, null)
        ))
      }

      "list table values with NOT NULL" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName), where = Some(NotNull(FieldName("A2"))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v1", "1", "1"),
          Seq("v2", "2", "0"),
          Seq("v3", "3", "1"),
          Seq("v4", "-4", "0"),
          Seq("v6", "6", null)
        ))
      }

      "list table values with EQUAL" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName), where = Some(EqualToValue(FieldName("A1"), Value("v3"))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v3", "3", "1")
        ))
      }
    }

    "#simpleSelect compose WHERE" should {
      "list table values with OR" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName),
          where = Some(Or(Seq(
            EqualToValue(FieldName("A1"), Value("v1")),
            EqualToValue(FieldName("A1"), Value("v2")),
            IsNull(FieldName("A2"))
          ))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v1", "1", "1"),
          Seq("v2", "2", "0"),
          Seq("v5", null, "0"),
          Seq("v7", null, null)
        ))
      }

      "list table values with AND" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName),
          where = Some(And(Seq(
            EqualToValue(FieldName("A1"), Value("v7")),
            IsNull(FieldName("A2"))
          ))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v7", null, null)
        ))
      }

      "empty result when list table values with AND" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName),
          where = Some(And(Seq(
            EqualToValue(FieldName("A1"), Value("v7")),
            NotNull(FieldName("A2"))
          ))))

        val result = getSimpleSelectResult(simpleSelect)

        result shouldEqual Seq.empty
      }

      "list table values with OR + AND" in {
        val simpleSelect = SimpleSelect(AllField, TableName(aTableName),
          where = Some(Or(Seq(
            EqualToValue(FieldName("A1"), Value("v1")),
            And(Seq(
              IsNull(FieldName("A2")),
              IsNull(FieldName("A3"))
            ))
          ))))

        val result = getSimpleSelectResult(simpleSelect)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v1", "1", "1"),
          Seq("v7", null, null)
        ))
      }

    }

  }
}

