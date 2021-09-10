/*
 * Copyright 2021 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.hmrc.mongo.transaction

import org.mongodb.scala.MongoException
import org.mongodb.scala.bson.BsonDocument
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import uk.gov.hmrc.mongo.MongoComponent
import uk.gov.hmrc.mongo.MongoUtils

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}
import ExecutionContext.Implicits.global

class TransactionSpec
  extends AnyWordSpecLike
     with Matchers
     with ScalaFutures
     with IntegrationPatience
     with BeforeAndAfterEach
     with Transactions {

  val mongoComponent = {
    val databaseName: String = "test-" + this.getClass.getSimpleName
    MongoComponent(mongoUri = s"mongodb://localhost:27017/$databaseName")
  }

  val collection =
    mongoComponent.database.getCollection[BsonDocument](collectionName = "myobject")

  "Transaction" should {
    "support Observable" in {
      implicit val ts = TransactionConfiguration()

      val res =
        withSessionAndTransaction(session =>
          for {
            _   <- collection.insertOne(session, BsonDocument())
            _   <- collection.insertOne(session, BsonDocument())
            _   <- collection.deleteOne(session, BsonDocument())
            res <- collection.find(session) // GOTCHA: if forget to pass session, it will silently stop..
          } yield res
        ).toFuture().futureValue

      res.size shouldBe 1

      val list = collection.find().toFuture
      list.futureValue.size shouldBe 1
    }

    "support Future" in {
      implicit val ts = TransactionConfiguration()

      val res =
        withSessionAndTransaction(session =>
          for {
            _   <- collection.insertOne(session, BsonDocument()).toFuture()
            _   <- collection.insertOne(session, BsonDocument()).toFuture()
            _   <- collection.deleteOne(session, BsonDocument()).toFuture()
            res <- collection.find(session).toFuture() // GOTCHA: if forget to pass session, it will silently stop..
          } yield res
        ).futureValue

      res.size shouldBe 1

      val list = collection.find().toFuture
      list.futureValue.size shouldBe 1
    }

    "rollback on error for Futures" in {
      implicit val ts = TransactionConfiguration()
      val attempts = new AtomicInteger(0)

      withSessionAndTransaction { session =>
        attempts.incrementAndGet
        for {
          _   <- collection.insertOne(session, BsonDocument()).toFuture()
          _   <- collection.insertOne(session, BsonDocument()).toFuture()
          _   =  sys.error("Fail")
        } yield ()
      }.failed.futureValue

      val list = collection.find().toFuture
      list.futureValue.size shouldBe 0

      attempts.get shouldBe 1 // no retries
    }

    "retry on transient transaction errors for Futures" in {
      implicit val ts = TransactionConfiguration()
      val attempts = new AtomicInteger(0)

      val e = new MongoException("Fail")
      e.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)

      withSessionAndTransaction { session =>
        val i = attempts.incrementAndGet
        for {
          _   <- collection.insertOne(session, BsonDocument()).toFuture()
          _   <- collection.insertOne(session, BsonDocument()).toFuture()
          _   =  if (i == 1) throw e
        } yield ()
      }.futureValue

      val list = collection.find().toFuture
      list.futureValue.size shouldBe 2

      attempts.get shouldBe 2 // retried once
    }

    // TODO onRecover doesn't work for Observables?
    /*"rollback on error for Observables" in {
      implicit val ts = TransactionConfiguration()

      withSessionAndTransaction(session =>
        for {
          _   <- collection.insertOne(session, BsonDocument())
          _   <- collection.insertOne(session, BsonDocument())
          _   =  sys.error("Fail")
        } yield ()
      ).toFuture().failed.futureValue

      val list = collection.find().toFuture
      list.futureValue.size shouldBe 0
    }*/
  }

  def prepareDatabase(): Unit =
    (for {
      exists <- MongoUtils.existsCollection(mongoComponent, collection)
      _      <- if (exists) collection.deleteMany(BsonDocument()).toFuture
                else Future.successful(())
     } yield ()
    ).futureValue

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    prepareDatabase()
  }
}
