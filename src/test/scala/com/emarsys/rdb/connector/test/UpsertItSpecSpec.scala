package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, IntValue, NullValue, StringValue}
import com.emarsys.rdb.connector.common.models.DataManipulation.UpdateDefinition
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.NonExistingFields
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Future

class UpsertItSpecSpec extends TestKit(ActorSystem()) with UpsertItSpec with MockitoSugar with BeforeAndAfterAll {

    implicit val materializer: Materializer = ActorMaterializer()

    implicit val executionContext = system.dispatcher

    override val connector = mock[Connector]

    override def initDb(): Unit = ()

    override def cleanUpDb(): Unit = ()

    override def afterAll {
      TestKit.shutdownActorSystem(system)
    }

  val upsertData =  Seq(
    UpdateDefinition(Map("A1" -> StringValue("v3")), Map("A1" -> StringValue("v3"), "A2" -> IntValue(0), "A3" -> BooleanValue(false))),
    UpdateDefinition(Map("A1" -> StringValue("v10")), Map("A1" -> StringValue("v10"), "A2" -> IntValue(10), "A3" -> BooleanValue(true)))
  )

  when(connector.upsert(tableName, upsertData)).thenReturn(Future.successful(Right(3)))
  when(connector.upsert(tableName, Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))))).thenReturn(Future.successful(Left(FailedValidation(NonExistingFields(Set("a"))))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("v3"))
    )))).thenReturn(Future(Right(Source(List(Seq("A1"), Seq("a"))))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("v10"))
    )))).thenReturn(Future(Right(Source(List(Seq("A1"), Seq("a"))))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName))))
    .thenReturn(Future(Right(Source(List(Seq("A1"), Seq("a"),Seq("b"),Seq("c"),Seq("d"),Seq("e"),Seq("f"),Seq("g"),Seq("h"))))))

}
