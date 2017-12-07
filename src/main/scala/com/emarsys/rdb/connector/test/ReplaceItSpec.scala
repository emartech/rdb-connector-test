package com.emarsys.rdb.connector.test

import java.util.UUID

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, NullValue, StringValue}
import com.emarsys.rdb.connector.common.models.DataManipulation.Record
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.NonExistingFields
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

trait ReplaceItSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  val connector: Connector
  def initDb(): Unit
  def cleanUpDb(): Unit
  implicit val materializer: Materializer

  val uuid = UUID.randomUUID().toString
  val tableName = s"replace_tables_table_$uuid"

  val awaitTimeout = 5.seconds

  override def beforeAll(): Unit = {
    initDb()
  }

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
  }

  def simpleSelectAllWithExpectedResultSize(number: Int) = SimpleSelect(AllField, TableName(tableName),
    where = Some(
      Or(Seq(EqualToValue(FieldName("A1"), Value(number.toString)), NotNull(FieldName("A1"))))))

  val replaceMultipleData: Seq[Record] =  Seq(
    Map("A1" -> StringValue("v1new"), "A3" -> BooleanValue(true)),
    Map("A1" -> StringValue("v2new"), "A3" -> BooleanValue(false)),
    Map("A1" -> StringValue("v3new"), "A3" -> BooleanValue(false)))

  val replaceSingleData: Seq[Record] =  Seq(
    Map("A1" -> StringValue("vxxx"), "A3" -> BooleanValue(true)))

  val replaceNonExistingFieldFieldData: Seq[Record] =  Seq(
    Map("a" -> StringValue("1")),
    Map("a" -> StringValue("2")))

  s"ReplaceSpec $uuid" when {

    "#replace" should {

      "validation error" in {
        Await.result(connector.replaceData(tableName, replaceNonExistingFieldFieldData), awaitTimeout) shouldBe Left(FailedValidation(NonExistingFields(Set("a"))))
      }

      "replace successfully with zero record" in {
        Await.result(connector.replaceData(tableName, Seq.empty), awaitTimeout) shouldBe Right(0)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(1)), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(1)}

      "replace successfully with one record" in {

        val simpleSelect = SimpleSelect(AllField, TableName(tableName),
          where = Some(
            EqualToValue(FieldName("A1"), Value("vxxx"))
          ))

        Await.result(connector.replaceData(tableName, replaceSingleData), awaitTimeout) shouldBe Right(1)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(2)), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
        Await.result(connector.simpleSelect(simpleSelect), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
      }

      "replace successfully with more records" in {
        val simpleSelectT = SimpleSelect(AllField, TableName(tableName),
          where = Some(
            EqualToValue(FieldName("A1"), Value("v1new"))
          ))
        val simpleSelectF = SimpleSelect(AllField, TableName(tableName),
          where = Some(
            EqualToValue(FieldName("A1"), Value("v2new"))
          ))
        val simpleSelectT2 = SimpleSelect(AllField, TableName(tableName),
          where = Some(
            EqualToValue(FieldName("A1"), Value("v3new"))
          ))

        Await.result(connector.replaceData(tableName, replaceMultipleData), awaitTimeout) shouldBe Right(3)
        Await.result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(4)), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(4)
        Await.result(connector.simpleSelect(simpleSelectT), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
        Await.result(connector.simpleSelect(simpleSelectF), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
        Await.result(connector.simpleSelect(simpleSelectT2), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)

      }
    }
  }

}
