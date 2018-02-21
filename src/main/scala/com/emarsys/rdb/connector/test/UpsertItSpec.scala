package com.emarsys.rdb.connector.test

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, IntValue, NullValue, StringValue}
import com.emarsys.rdb.connector.common.models.DataManipulation.UpdateDefinition
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.NonExistingFields
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}

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

  s"UpsertSpec $uuid" when {

    "#upsert" should {

      "validation error" in {
        val upsertData = Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("a" -> StringValue("2"))))
        Await.result(connector.upsert(tableName, upsertData), awaitTimeout) shouldBe Left(FailedValidation(NonExistingFields(Set("a"))))
      }

      "upsert successfully more definition" in {
        val simpleSelectV3 = SimpleSelect(AllField, TableName(tableName),
          where = Some(
            EqualToValue(FieldName("A1"), Value("v3"))
          ))
        val simpleSelectV10 = SimpleSelect(AllField, TableName(tableName),
          where = Some(
            EqualToValue(FieldName("A1"), Value("v10"))
          ))
        val simpleSelectAll = SimpleSelect(AllField, TableName(tableName))


        val upsertData =  Seq(
          UpdateDefinition(Map("A1" -> StringValue("v3")), Map("A1" -> StringValue("v3"), "A2" -> IntValue(0), "A3" -> BooleanValue(false))),
          UpdateDefinition(Map("A1" -> StringValue("v10")), Map("A1" -> StringValue("v10"), "A2" -> IntValue(10), "A3" -> BooleanValue(true)))
        )

        Await.result(connector.upsert(tableName, upsertData), awaitTimeout) shouldBe Right(3)
        Await.result(connector.simpleSelect(simpleSelectV3), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(1+1)
        Await.result(connector.simpleSelect(simpleSelectV10), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(1+1)
        Await.result(connector.simpleSelect(simpleSelectAll), awaitTimeout).map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(8+1)
      }
    }
  }

}
