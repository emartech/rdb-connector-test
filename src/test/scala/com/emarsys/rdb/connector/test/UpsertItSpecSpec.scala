package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.NonExistingFields
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

  when(connector.upsert(tableName, insertAndUpdateData)).thenReturn(Future.successful(Right(2)))
  when(connector.upsert(tableName, upsertNonExistingFieldFieldData))
    .thenReturn(Future.successful(Left(FailedValidation(NonExistingFields(Set("a"))))))

  when(connector.simpleSelect(simpleSelectAll)).thenReturn(Future(Right(Source(List(Seq("A1"), Seq("a"), Seq("a"), Seq("a"), Seq("a"), Seq("a"), Seq("a"), Seq("a"), Seq("a"))))))
  when(connector.simpleSelect(simpleSelectV1)).thenReturn(Future(Right(Source(List(Seq("A1"), Seq("a"))))))
  when(connector.simpleSelect(simpleSelectV1new)).thenReturn(Future(Right(Source(List(Seq("A1"), Seq("a"))))))

}
