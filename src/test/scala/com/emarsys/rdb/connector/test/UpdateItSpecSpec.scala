package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, NullValue, StringValue}
import com.emarsys.rdb.connector.common.models.DataManipulation.UpdateDefinition
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, TableModel}
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.NonExistingFields
import com.emarsys.rdb.connector.common.models.{Connector, DataManipulation, SimpleSelect}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Future

class UpdateItSpecSpec extends TestKit(ActorSystem()) with UpdateItSpec with MockitoSugar with BeforeAndAfterAll {

    implicit val materializer: Materializer = ActorMaterializer()

    implicit val executionContext = system.dispatcher

    override val connector = mock[Connector]

    override def initDb(): Unit = ()

    override def cleanUpDb(): Unit = ()

    override def afterAll {
      TestKit.shutdownActorSystem(system)
    }

  val updateData =  Seq(
    UpdateDefinition(Map("A1" -> StringValue("vt")), Map("A3" -> BooleanValue(true))),
    UpdateDefinition(Map("A1" -> StringValue("vf")), Map("A3" -> BooleanValue(false))),
    UpdateDefinition(Map("A1" -> StringValue("vn")), Map("A3" -> NullValue))
  )

  when(connector.update(tableName, Seq(UpdateDefinition(Map("A1" -> StringValue("vxxx")), Map("A3" -> BooleanValue(true)))))).thenReturn(Future.successful(Right(2)))
  when(connector.update(tableName, updateData)).thenReturn(Future.successful(Right(7)))
  when(connector.update(tableName, Seq(UpdateDefinition(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))))).thenReturn(Future.successful(Left(FailedValidation(NonExistingFields(Set("a"))))))


  when(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("vxxx"))
    )))).thenReturn(Future(Right(Source(List(Seq("a"),Seq("s"))))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("vt"))
    )))).thenReturn(Future(Right(Source(List(Seq("a"),Seq("s"))))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("vf"))
    )))).thenReturn(Future(Right(Source(List(Seq("a"),Seq("s"), Seq("d"))))))

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("vn"))
    )))).thenReturn(Future(Right(Source(List(Seq("a"),Seq("s"))))))

}
