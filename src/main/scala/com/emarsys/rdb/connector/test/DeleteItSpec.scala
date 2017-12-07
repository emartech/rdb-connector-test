package com.emarsys.rdb.connector.test

import java.util.UUID

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, NullValue, StringValue}
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

/*
For positive results use the A table definition and preloaded data defined in the SimpleSelect.
Make sure you have index on A3.
*/

trait DeleteItSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  val connector: Connector
  def initDb(): Unit
  def cleanUpDb(): Unit
  implicit val materializer: Materializer

  val uuid = UUID.randomUUID().toString
  val tableName = s"delete_tables_table_$uuid"

  val awaitTimeout = 5.seconds

  val simpleDeleteCiterion =  Seq(
    Map("A2" -> StringValue("2")),
    Map("A3" -> StringValue("1")))

  val complexDeleteCriterion = Seq(
    Map("A2" -> StringValue("2"), "A3" -> BooleanValue(true)),
    Map("A2" -> StringValue("3"), "A3" -> BooleanValue(false)))

  val notMatchingComplexDeleteCriterion = Seq(
    Map("A2" -> StringValue("123456"), "A3" -> BooleanValue(true)),
    Map("A2" -> StringValue("7891011"), "A3" -> BooleanValue(true)))

  val nullValueDeleteCriterion = Seq(Map("A2" -> null))

  val simpleSelect = SimpleSelect(AllField, TableName(tableName),
    where = Some(
      And(Seq(
        EqualToValue(FieldName("A2"), Value("2")),
        EqualToValue(FieldName("A3"), Value("1"))))
    ))

  val complexSelect = SimpleSelect(AllField, TableName(tableName),
    where = Some(
      Or(Seq(
        And(Seq(
          EqualToValue(FieldName("A2"), Value("2")),
          EqualToValue(FieldName("A3"), Value("1")))),
        And(Seq(
          EqualToValue(FieldName("A2"), Value("2")),
          EqualToValue(FieldName("A3"), Value("1")))))
      )))

  val simpleNullSelect = SimpleSelect(AllField, TableName(tableName),
    where = Some(IsNull(FieldName("A2"))))

  override def beforeAll(): Unit = {
    initDb()
  }

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
  }

  s"DeleteSpec $uuid" when {

    "#delete" should {
      "delete nothing is data is empty" in {
        Await.result(connector.delete(tableName, Seq()), awaitTimeout) shouldBe Right(0)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(8)), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(8)
      }

      "delete matching rows by simple criterions" in {
        Await.result(connector.delete(tableName, simpleDeleteCiterion), awaitTimeout) shouldBe Right(1)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(7)), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(7)
        Await.result(connector.simpleSelect(simpleSelect), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(0)
      }

      "delete matching rows by complex criterions" in {
        Await.result(connector.delete(tableName, complexDeleteCriterion), awaitTimeout) shouldBe Right(2)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(6)), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(6)
        Await.result(connector.simpleSelect(complexSelect), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(0)
      }

      "delete nothing if complex criterion does not match any result" in {
        Await.result(connector.delete(tableName, notMatchingComplexDeleteCriterion), awaitTimeout) shouldBe Right(0)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(8)), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(8)
      }

      "accept null values" in {
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(5)), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(5)
        Await.result(connector.delete(tableName, nullValueDeleteCriterion), awaitTimeout) shouldBe Right(3)
        Await.result(connector.simpleSelect(simpleNullSelect), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(0)
      }
    }
  }

  private def simpleSelectAllWithExpectedResultSize(number: Int) = SimpleSelect(AllField, TableName(tableName),
    where = Some(
      Or(Seq(EqualToValue(FieldName("A1"), Value(number.toString)), NotNull(FieldName("A1"))))))


}
