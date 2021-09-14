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

import org.mongodb.scala._
import play.api.Logger
import uk.gov.hmrc.mongo.MongoComponent

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import java.util.concurrent.atomic.AtomicReference
import org.mongodb.scala.internal.Init
import org.mongodb.scala.internal.WaitingOnChild
import org.mongodb.scala.internal.LastChildNotified
import org.mongodb.scala.internal.LastChildResponded
import org.mongodb.scala.internal.Done
import org.mongodb.scala.internal.State


/** Effectively provides the behaviour not available on `org.mongodb.scala.ClientSession` (driver-scala, nor
 * `com.mongodb.reactivestreams.client.ClientSession` driver-reactive-streams) but is available on
 * `com.mongodb.client.ClientSession` (driver-core)
 */
trait Transactions {
  def mongoComponent: MongoComponent

  private val logger = Logger(this.getClass)

  // Same as com.mongodb.client.internal.ClientSessionImpl.MAX_RETRY_TIME_LIMIT_MS
  private val maxRetryTimeLimitMs = 2.minutes.toMillis

  //--  Future ---

  def withSessionAndTransaction[A](
    f: ClientSession => Future[A]
  )(implicit
    tc: TransactionConfiguration,
    ec: ExecutionContext
  ): Future[A] =
    withClientSession(session =>
      withTransaction(session, f(session))
    )

  def withClientSession[A](
    f: ClientSession => Future[A]
  )(implicit
    tc: TransactionConfiguration,
    ec: ExecutionContext
  ): Future[A] =
    for {
      session <- tc.clientSessionOptions.fold(mongoComponent.client.startSession)(mongoComponent.client.startSession _).toFuture()
      f2      =  f(session)
      _       =  f2.onComplete(_ => session.close())
      res     <- f2
    } yield res

  /** A transaction is started before running the continuation. It will be committed or aborted depending on whether the callback executes
    * successfully or not.
    * It also provides retries for network issues
    * See https://github.com/mongodb/mongo-java-driver/tree/master/driver-core/src/test/resources/transactions-convenient-api#retry-timeout-is-enforced
    * And https://docs.mongodb.com/manual/core/retryable-writes/#retryable-writes-and-multi-document-transactions
    */
  // based on implementation: https://github.com/mongodb/mongo-java-driver/blob/1294de8a3daf0bcc0403d127f8c932511a51ea18/driver-sync/src/main/com/mongodb/client/internal/ClientSessionImpl.java#L203-L245
  def withTransaction[A](
    session: ClientSession,
    f      : => Future[A]
  )(implicit
    tc: TransactionConfiguration,
    ec: ExecutionContext
  ): Future[A] = {
    val startTimeMs = System.currentTimeMillis()

    def retryFor[B](cond: MongoException => Boolean)(f: () => Future[B]): Future[B] =
      f().recoverWith {
        case e: MongoException if cond(e) && System.currentTimeMillis() - startTimeMs < maxRetryTimeLimitMs =>
          logger.error(s"Failed with ${e.getLocalizedMessage} - will retry")
          retryFor(cond)(f)
      }

    retryFor(_.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)){ () =>
      for {
        _       <- Future.successful(tc.transactionOptions.fold(session.startTransaction)(session.startTransaction _))
        res     <- f.recoverWith { case e1 =>
                     session.abortTransaction().toSingle().toFuture()
                       .recoverWith {
                         case e2 => logger.error(s"Error aborting transaction: ${e2.getMessage}", e2)
                                    Future.failed(e1)
                       }
                       .flatMap(_ => Future.failed(e1))
                   }
        _       <- retryFor(e =>
                     !e.isInstanceOf[MongoExecutionTimeoutException]
                     && e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)
                   )(() => session.commitTransaction().toSingle().toFuture())
        } yield res
    }
  }

  //--  Observable ---

  def withSessionAndTransaction[A](
    f: ClientSession => Observable[A]
  )(implicit
    tc: TransactionConfiguration
  ): Observable[A] =
    withClientSession(session =>
      withTransaction(session, f(session))
    )

  def withClientSession[A](
    f: ClientSession => Observable[A]
  )(implicit
    tc: TransactionConfiguration
  ): Observable[A] =
    for {
      session <- tc.clientSessionOptions.fold(mongoComponent.client.startSession)(mongoComponent.client.startSession _)
      res     <- f(session).recover { case e => session.close(); throw e }
      _       =  session.close()
      } yield res

    /** A transaction is started before running the continuation. It will be committed or aborted depending on whether the callback executes
    * successfully or not.
    * It also provides retries for network issues
    * See https://github.com/mongodb/mongo-java-driver/tree/master/driver-core/src/test/resources/transactions-convenient-api#retry-timeout-is-enforced
    * And https://docs.mongodb.com/manual/core/retryable-writes/#retryable-writes-and-multi-document-transactions
    */
  // based on implementation: https://github.com/mongodb/mongo-java-driver/blob/1294de8a3daf0bcc0403d127f8c932511a51ea18/driver-sync/src/main/com/mongodb/client/internal/ClientSessionImpl.java#L203-L245
  def withTransaction[A](
    session: ClientSession,
    f      : => Observable[A]
  )(implicit
    tc: TransactionConfiguration
  ): Observable[A] = {
    val startTimeMs = System.currentTimeMillis()

    def retryFor[B](cond: MongoException => Boolean)(f: () => Observable[B]): Observable[B] =
      f().recoverWith {
        case e: MongoException if cond(e) && System.currentTimeMillis() - startTimeMs < maxRetryTimeLimitMs =>
          logger.error(s"Failed with ${e.getLocalizedMessage} - will retry")
          retryFor(cond)(f)
      }

    retryFor(_.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)){ () =>
      tc.transactionOptions.fold(session.startTransaction)(session.startTransaction _)
      for {
        res <- f.recoverWith { case e1 =>
                 completeWith(session.abortTransaction(), ())
                   .recover {
                     case e2 => logger.error(s"Error aborting transaction: ${e2.getMessage}", e2)
                                failWith(e1) // throwing breaks MapObservable/FlatMapObservable...
                   }
                   .flatMap[A](_ => failWith(e1))  // throwing breaks MapObservable/FlatMapObservable...
               }
        _   <- retryFor(e =>
                 !e.isInstanceOf[MongoExecutionTimeoutException]
                 && e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)
               )(() => completeWith(session.commitTransaction(), ()))
      } yield res
    }
  }

  def failWith[A](e: Throwable) = new Observable[A] {
    val delegate: Observable[Unit] = Observable[Unit](Seq(()))
    override def subscribe(observer: Observer[_ >: A]): Unit =
      delegate.subscribe(
        new Observer[Unit] {
          override def onError(throwable: Throwable): Unit =
            observer.onError(throwable)

          override def onSubscribe(sub: Subscription): Unit =
            observer.onSubscribe(sub)

          override def onComplete(): Unit =
            () // not reached

          override def onNext(tResult: Unit): Unit =
            onError(e)
        }
      )
  }

  /** Observable[Void] by definition will never emit a value to be mapped (see org.mongodb.scala.internal.MapObservable).
    * When converting to Future, it works since it completes with `None` (see org.mongodb.scala.Observable.headOption).
    * This function converts an Observable[Void] to a provided value to continue
    */
  def completeWith[A](obs: Observable[Void], f: => A): Observable[A] =
    new Observable[A] {
      override def subscribe(observer: Observer[_ >: A]): Unit =
        obs.subscribe(
          new Observer[Void] {
            override def onError(throwable: Throwable): Unit =
              observer.onError(throwable)

            override def onSubscribe(subscription: Subscription): Unit =
              observer.onSubscribe(subscription)

            override def onComplete(): Unit = {
              observer.onNext(f)
              observer.onComplete()
            }

            override def onNext(tResult: Void): Unit =
              ??? // by definition never called
          }
        )
    }

  def wrap[A](observable: Observable[A]): Observable[A] = new Observable[A] {
    override def subscribe(observer: Observer[_ >: A]): Unit =
      observable.subscribe(observer)

    override def map[S](mapFunction: A => S): Observable[S] =
      MapObservable2(this, mapFunction)

    override def flatMap[S](mapFunction: A => Observable[S]): Observable[S] =
      FlatMapObservable2(this, mapFunction)
   }

   // fixes org.mongodb.scala.internal.MapObservable by not swallowing exceptions!
   private case class MapObservable2[T, S](
     observable: Observable[T],
     s         : T => S,
     f         : Throwable => Throwable = t => t
   ) extends Observable[S] {
     override def subscribe(observer: Observer[_ >: S]): Unit = {
       observable.subscribe(
         new Observer[T] {
           override def onError(throwable: Throwable): Unit =
             observer.onError(f(throwable))

           override def onSubscribe(subscription: Subscription): Unit =
             observer.onSubscribe(subscription)

           override def onComplete(): Unit =
             observer.onComplete()

           override def onNext(tResult: T): Unit =
             try {
               observer.onNext(s(tResult))
             } catch { // this is what differs from org.mongodb.scala.internal.MapObservable
               case t: Throwable => observer.onError(f(t))
             }
         }
       )
     }
   }

   private case class FlatMapObservable2[T, S](
     observable: Observable[T],
     f: T => Observable[S]
   ) extends Observable[S] {
     override def subscribe(observer: Observer[_ >: S]): Unit = {
       observable.subscribe(
         new Observer[T] {
           @volatile private var outerSubscription: Option[Subscription] = None
           @volatile private var demand: Long = 0
           private val state = new AtomicReference[State](Init)

           override def onSubscribe(subscription: Subscription): Unit = {
             val masterSub = new Subscription() {
               override def isUnsubscribed: Boolean = subscription.isUnsubscribed
               override def unsubscribe(): Unit = subscription.unsubscribe()
               override def request(n: Long): Unit = {
                 require(n > 0L, s"Number requested must be greater than zero: $n")
                 val localDemand = addDemand(n)
                 state.get() match {
                   case Init              => subscription.request(1L)
                   case WaitingOnChild(s) => s.request(localDemand)
                   case _                 => // noop
                 }
               }
             }
             outerSubscription = Some(masterSub)
             state.set(Init)
             observer.onSubscribe(masterSub)
           }

          override def onComplete(): Unit =
            state.get() match {
              case Done  => // ok
              case internal.Error => // ok
              case Init if state.compareAndSet(Init, Done) =>
                observer.onComplete()
              case w @ WaitingOnChild(_) if state.compareAndSet(w, LastChildNotified) =>
              // letting the child know that we delegate onComplete call to it
              case LastChildNotified =>
              // wait for the child to do the delegated onCompleteCall
              case LastChildResponded if state.compareAndSet(LastChildResponded, Done) =>
                observer.onComplete()
              case other =>
                // state machine is broken, let's fail
                // normally this won't happen
                throw new IllegalStateException(s"Unexpected state in FlatMapObservable `onComplete` handler: ${other}")
            }

          override def onError(throwable: Throwable): Unit =
            observer.onError(throwable)

          override def onNext(tResult: T): Unit =
            try {
              f(tResult)
                .subscribe(
                  new Observer[S]() {
                    override def onError(throwable: Throwable): Unit = {
                      state.set(internal.Error)
                      observer.onError(throwable)
                    }

                    override def onSubscribe(subscription: Subscription): Unit = {
                      state.set(WaitingOnChild(subscription))
                      if (demand > 0) subscription.request(demand)
                    }

                    override def onComplete(): Unit = {
                      state.get() match {
                        case Done                                                                            => // no need to call parent's onComplete
                        case internal.Error                                                                  => // no need to call parent's onComplete
                        case LastChildNotified if state.compareAndSet(LastChildNotified, LastChildResponded) =>
                          // parent told us to call onComplete
                          observer.onComplete()
                        case _ if demand > 0 =>
                          // otherwise we are not the last child, let's tell the parent
                          // it's not dealing with us anymore.
                          // Init -> * will be handled by possible later items in the stream
                          state.set(Init)
                          addDemand(-1) // reduce demand by 1 as it will be incremented by the outerSubscription
                          outerSubscription.foreach(_.request(1))
                        case _ =>
                          // no demand
                          state.set(Init)
                      }
                    }

                    override def onNext(tResult: S): Unit = {
                      addDemand(-1)
                      observer.onNext(tResult)
                    }
                  }
                )
              } catch { // this is what differs from org.mongodb.scala.internal.MapObservable
                case t: Throwable => observer.onError(t)
              }

          /**
          * Adds extra demand and protects against Longs rolling over
          *
          * @param extraDemand the amount of extra demand
          * @return the updated demand
          */
          private def addDemand(extraDemand: Long): Long = {
            this.synchronized {
              demand += extraDemand
              if (demand < 0) {
                if (extraDemand < 0) {
                  throw new IllegalStateException("Demand cannot be reduced to below zero")
                }
                demand = Long.MaxValue
              }
            }
            demand
          }
        }
       )
     }
   }
}


case class TransactionConfiguration(
  clientSessionOptions: Option[ClientSessionOptions] = None, // will default to that defined on client
  transactionOptions  : Option[TransactionOptions]   = None  // will default to that defined by ClientSessionOptions
)

object TransactionConfiguration {
   // TODO naming?
   /** A TransactionConfiguration with causal consistency, and free of rollbacks and phantom reads */
  lazy val strict: TransactionConfiguration =
    TransactionConfiguration(
      clientSessionOptions = Some(
                               ClientSessionOptions.builder()
                                 .causallyConsistent(true)
                                 .build()
                             ),
      transactionOptions   = Some(
                               TransactionOptions.builder()
                                 .readConcern(ReadConcern.MAJORITY)
                                 .writeConcern(WriteConcern.MAJORITY)
                                 .build()
                             )
    )
}
