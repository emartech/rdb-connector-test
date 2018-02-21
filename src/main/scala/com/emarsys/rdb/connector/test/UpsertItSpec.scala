package com.emarsys.rdb.connector.test

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{IntValue, NullValue, StringValue}
import com.emarsys.rdb.connector.common.models.DataManipulation.Record
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.NonExistingFields
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

trait UpsertItSpec extends WordSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  val connector: Connector
  def initDb(): Unit
  def cleanUpDb(): Unit
  implicit val materializer: Materializer

  val uuid = uuidGenerate
  val tableName = s"upsert_tables_table_$uuid"

  val awaitTimeout = 5.seconds

  override def beforeEach(): Unit = {
    initDb()
  }

  override def afterEach(): Unit = {
    cleanUpDb()
  }

  override def afterAll(): Unit = {
    connector.close()
  }

  val simpleSelectV1 = SimpleSelect(AllField, TableName(tableName),
    where = Some(
      And(Seq(
        EqualToValue(FieldName("A1"), Value("v1")),
        IsNull(FieldName("A2")),
        IsNull(FieldName("A3"))
      ))
    ))

  val simpleSelectV1new = SimpleSelect(AllField, TableName(tableName),
    where = Some(
      And(Seq(
        EqualToValue(FieldName("A1"), Value("v1new")),
        EqualToValue(FieldName("A2"), Value("777")),
        IsNull(FieldName("A3"))
      ))
    ))

  val simpleSelectAll = SimpleSelect(AllField, TableName(tableName))

  val insertAndUpdateData: Seq[Record] = Seq(
    Map("A1" -> StringValue("v1"), "A2" -> NullValue, "A3" -> NullValue),
    Map("A1" -> StringValue("v1new"), "A2" -> IntValue(777), "A3" -> NullValue))

  val upsertNonExistingFieldFieldData: Seq[Record] = Seq(
    Map("a" -> StringValue("1")),
    Map("a" -> StringValue("2")))


  s"UpsertSpec $uuid" when {

    "#upsert" should {

      "validation error" in {
        Await.result(connector.upsert(tableName, upsertNonExistingFieldFieldData), awaitTimeout) shouldBe Left(FailedValidation(NonExistingFields(Set("a"))))
      }

      "upsert successfully more records" in {
        Await.result(connector.upsert(tableName, insertAndUpdateData), awaitTimeout) shouldBe Right(2)
        Await.result(connector.simpleSelect(simpleSelectAll), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(8 + 1)
        Await.result(connector.simpleSelect(simpleSelectV1), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(1 + 1)
        Await.result(connector.simpleSelect(simpleSelectV1new), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(1 + 1)
      }

    }
  }

}
