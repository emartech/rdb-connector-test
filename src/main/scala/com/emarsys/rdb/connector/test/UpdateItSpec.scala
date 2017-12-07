package com.emarsys.rdb.connector.test

import java.util.UUID

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, NullValue, StringValue}
import com.emarsys.rdb.connector.common.models.DataManipulation.UpdateDefinition
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.TableModel
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.{InvalidOperationOnView, NonExistingFields}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import concurrent.duration._
import scala.concurrent.Await

/*
For positive results use the A table definition and preloaded data defined in the SimpleSelect.
Make sure you have index on A3.
*/

trait UpdateItSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  val connector: Connector
  def initDb(): Unit
  def cleanUpDb(): Unit
  implicit val materializer: Materializer

  val uuid = UUID.randomUUID().toString
  val tableName = s"metadata_list_tables_table_$uuid"

  val awaitTimeout = 5.seconds

  override def beforeAll(): Unit = {
    initDb()
  }

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
  }

  s"UpdateSpec $uuid" when {

    "#update" should {

      "validation error" in {
        val updateData = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("a" -> StringValue("2"))))
        Await.result(connector.update(tableName, updateData), awaitTimeout) shouldBe Left(FailedValidation(NonExistingFields(Set("a"))))
      }

      "update successfully one definition" in {
        val updateData =  Seq(UpdateDefinition(Map("A3" -> BooleanValue(true)), Map("A1" -> StringValue("vxxx"))))
        val simpleSelect = SimpleSelect(AllField, TableName(tableName),
          where = Some(
            EqualToValue(FieldName("A1"), Value("vxxx"))
          ))

        Await.result(connector.update(tableName, updateData), awaitTimeout) shouldBe Right(2)
        Await.result(connector.simpleSelect(simpleSelect), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
      }

      "update successfully more definition" in {
        val simpleSelectT = SimpleSelect(AllField, TableName(tableName),
          where = Some(
            EqualToValue(FieldName("A1"), Value("vt"))
          ))
        val simpleSelectF = SimpleSelect(AllField, TableName(tableName),
          where = Some(
            EqualToValue(FieldName("A1"), Value("vf"))
          ))
        val simpleSelectN = SimpleSelect(AllField, TableName(tableName),
          where = Some(
            EqualToValue(FieldName("A1"), Value("vn"))
          ))


        val updateData =  Seq(
          UpdateDefinition(Map("A3" -> BooleanValue(true)), Map("A1" -> StringValue("vt"))),
          UpdateDefinition(Map("A3" -> BooleanValue(false)), Map("A1" -> StringValue("vf"))),
          UpdateDefinition(Map("A3" -> NullValue), Map("A1" -> StringValue("vn")))
        )

        Await.result(connector.update(tableName, updateData), awaitTimeout) shouldBe Right(7)
        Await.result(connector.simpleSelect(simpleSelectT), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
        Await.result(connector.simpleSelect(simpleSelectF), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(3)
        Await.result(connector.simpleSelect(simpleSelectN), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
      }
    }
  }

}
