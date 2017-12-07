package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.NonExistingFields
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Future

class ReplaceSpecSpec extends TestKit(ActorSystem()) with ReplaceItSpec with MockitoSugar with BeforeAndAfterAll {

    implicit val materializer: Materializer = ActorMaterializer()

    implicit val executionContext = system.dispatcher

    override val connector = mock[Connector]

    override def initDb(): Unit = ()

    override def cleanUpDb(): Unit = ()

    override def afterAll {
      TestKit.shutdownActorSystem(system)
    }


  when(connector.replaceData(tableName, replaceMultipleData)).thenReturn(Future.successful(Right(3)))
  when(connector.replaceData(tableName, replaceSingleData)).thenReturn(Future.successful(Right(1)))
  when(connector.replaceData(tableName, Seq.empty)).thenReturn(Future.successful(Right(0)))

  when(connector.replaceData(tableName, replaceNonExistingFieldFieldData))
    .thenReturn(Future.successful(Left(FailedValidation(NonExistingFields(Set("a"))))))


  Seq("v1new","v2new","v3new","vxxx").foreach(selectUniqueValueMock)
  Seq(1,2,4).foreach(selectExactNumberMock)

  private def selectUniqueValueMock(value: String) = {
    when(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName),
      where = Some(
        EqualToValue(FieldName("A1"), Value(value))
      )))).thenReturn(Future(Right(Source(List(Seq("columnName"),Seq(value))))))
  }

  private def selectExactNumberMock(number: Int) = {
    when(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName),
      where = Some(
        Or(Seq(EqualToValue(FieldName("A1"), Value(number.toString)), NotNull(FieldName("A1"))))))))
      .thenReturn(Future(Right(Source(List.fill(number)(Seq("foo"))))))
  }



}
