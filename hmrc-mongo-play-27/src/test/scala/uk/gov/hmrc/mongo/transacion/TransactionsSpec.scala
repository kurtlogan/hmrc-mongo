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
import org.mongodb.scala.Observable
import org.mongodb.scala.Subscription
import org.mongodb.scala.Observer

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

  val collectionName = "myobject"
  val collection =
    mongoComponent.database.getCollection[BsonDocument](collectionName = collectionName)

  "Transactions" when {
    "using Future" should {
      "commit" in {
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

      "rollback on error" in {
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

      "retry on transient transaction errors" in {
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
    }

    "using Observable" should {
      "commit" in {
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

      "rollback on error for failed Observables" in {
        implicit val ts = TransactionConfiguration()
        val attempts = new AtomicInteger(0)

        withSessionAndTransaction { session =>
          attempts.incrementAndGet
          for {
            _ <- collection.insertOne(session, BsonDocument())
            _ <- collection.insertOne(session, BsonDocument())
            _ <- failWith[Unit](new RuntimeException("Fail")) // doesn't work with `sys.error("Fail")`
          } yield ()
        }.toFuture().failed.futureValue

        val list = collection.find().toFuture
        list.futureValue.size shouldBe 0

        attempts.get shouldBe 1 // no retries
      }

      "retry on transient transaction errors" in {
        implicit val ts = TransactionConfiguration()
        val attempts = new AtomicInteger(0)

        val e = new MongoException("Fail")
        e.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)

        withSessionAndTransaction { session =>
          val i = attempts.incrementAndGet
          for {
            _   <- collection.insertOne(session, BsonDocument())
            _   <- collection.insertOne(session, BsonDocument())
            _   <- if (i == 1)
                    failWith[Unit](e) // doesn't work with `throw e`
                  else Observable[Unit](Seq(()))
          } yield ()
        }.toFuture().futureValue

        val list = collection.find().toFuture
        list.futureValue.size shouldBe 2

        attempts.get shouldBe 2 // retried once
      }

      // onRecover doesn't work for Observables?
      // requires wrapping all observables with "wrap" to fix map/flatmap
      /*"rollback on error for Observables" in {
        implicit val ts = TransactionConfiguration()
        val attempts = new AtomicInteger(0)

        val e = new MongoException("Fail")
        e.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)

        val obs =
          withSessionAndTransaction(session =>
            for {
              _ <- wrap(collection.insertOne(session, BsonDocument()))
              _ <- wrap(collection.insertOne(session, BsonDocument()))
              _ =  sys.error("Fail")
            } yield ()
          )

        obs.toFuture().failed.futureValue

        val list = collection.find().toFuture
        list.futureValue.size shouldBe 0

        attempts.get shouldBe 1 // no retries
      }*/

      // shows that wrap fixes Map and FlatMap
      // TODO onRecover doesn't work for Observables?
      /*"rollback on error for Observables2" in {
        val successfulObservable = Observable[Int]((1 to 100).toStream)

        val failedObservable = new Observable[Int] {
          val delegate: Observable[Int] = Observable[Int]((1 to 100).toStream)
          val failOn = 10
          val errorMessage: String = "Failed"

          override def subscribe(observer: Observer[_ >: Int]): Unit =
                    delegate.subscribe(
                      new Observer[Int] {
                        var failed = false
                        var subscription: Option[Subscription] = None
                        override def onError(throwable: Throwable): Unit =
                          observer.onError(throwable)

                        override def onSubscribe(sub: Subscription): Unit =
                          observer.onSubscribe(sub)

                        override def onComplete(): Unit =
                          if (!failed) observer.onComplete()

                        override def onNext(tResult: Int): Unit = {
                          /*if (!failed) {
                            if (tResult == failOn) {
                              failed = true
                              onError(new MongoException(errorMessage))
                            } else {
                              observer.onNext(tResult)
                            }
                          }*/
                          throw new MongoException(errorMessage)
                        }
                      }
                    )
        }


        val obs =
          // this goes into recover
          //failedObservable

          // this doesn't go into recover
          //successfulObservable.map[Int](_ => sys.error("Fail2"))
          // this does...
          //wrap2(successfulObservable).map[Int](_ => sys.error("Fail2"))

          // this doesn't go into recover
          //successfulObservable.flatMap[Int](_ => sys.error("Fail2"))
          wrap(successfulObservable).flatMap[Int](_ => sys.error("Fail2"))

        // so... map & flatmap do not work as

        val obs2 = obs.recover {
          case t => println(s">>>>>>> Recover!!: $t"); throw t
        }

        import scala.concurrent.duration.DurationInt
        scala.concurrent.Await.result(obs2.toFuture(), 10.seconds)

        // Also: DocumentationTransactionsExampleSpec
      }*/
    }
  }

  def prepareDatabase(): Unit =
    (for {
      exists <- MongoUtils.existsCollection(mongoComponent, collection)
      _      <- if (exists) collection.deleteMany(BsonDocument()).toFuture
                // until Mongo 4.4 implicit collection creation (on insert/upsert) will fail when in a transaction
                // create explicitly
                else mongoComponent.database.createCollection(collectionName).toFuture
     } yield ()
    ).futureValue

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    prepareDatabase()
  }
}
