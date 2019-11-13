/*
 * Copyright 2019 HM Revenue & Customs
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

package uk.gov.hmrc.mongo.play

import org.joda.time.{DateTime, DateTimeZone, LocalDate, LocalDateTime}
import org.bson.codecs.configuration.{CodecRegistries, CodecRegistry}
import org.scalacheck.{Arbitrary, Gen, Prop}
import org.scalatest.{AppendedClues, Matchers, OptionValues, WordSpecLike}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.compatible.Assertion
import org.scalatest.Matchers.{equal => equal2, _}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.mongodb.scala.{Completed, MongoCollection, MongoDatabase}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.model.{Filters, Updates}
import play.api.libs.functional.syntax._
import play.api.libs.json._
import uk.gov.hmrc.mongo.component.MongoComponent
import uk.gov.hmrc.mongo.play.json.{Codecs, MongoJodaFormats}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag

import ExecutionContext.Implicits.global

class PlayMongoCollectionSpec
  extends WordSpecLike
     with ScalaFutures
     with ScalaCheckDrivenPropertyChecks {

  import PlayMongoCollectionSpec._

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(5.seconds)

  val mongoComponent = {
    val databaseName: String = "test-" + this.getClass.getSimpleName
    MongoComponent(mongoUri = s"mongodb://localhost:27017/$databaseName")
  }

  val playMongoCollection = new PlayMongoCollection[MyObject](
      mongoComponent = mongoComponent
    , collectionName = "myobject"
    , domainFormat   = myObjectFormat
    , optRegistry    = Some(CodecRegistries.fromCodecs(
                         Codecs.playFormatCodec(stringWrapperFormat)
                       , Codecs.playFormatCodec(booleanWrapperFormat)
                       , Codecs.playFormatCodec(intWrapperFormat)
                       , Codecs.playFormatCodec(longWrapperFormat)
                       , Codecs.playFormatCodec(doubleWrapperFormat)
                       , Codecs.playFormatCodec(bigDecimalWrapperFormat)
                       , Codecs.playFormatCodec(MongoJodaFormats.localDateFormats)
                       , Codecs.playFormatCodec(MongoJodaFormats.localDateTimeFormats)
                       , Codecs.playFormatCodec(MongoJodaFormats.dateTimeFormats)
                       , Codecs.playFormatCodec(sumFormat)
                       ))
    , indexes        = Seq.empty
    )

  "PlayMongoCollection.collection" should {

    "read and write object with fields" in {
      forAll(myObjectGen) { myObj =>
        dropDatabase()
        val result = playMongoCollection.collection.insertOne(myObj).toFuture
        result.futureValue shouldBe Completed()

        val writtenObj = playMongoCollection.collection.find().toFuture
        writtenObj.futureValue shouldBe List(myObj)
      }
    }

    "filter by fields" in {
      forAll(myObjectGen) { myObj =>
        dropDatabase()

        val result = playMongoCollection.collection.insertOne(myObj).toFuture
        result.futureValue shouldBe Completed()

        def checkFind(key: String, value: Any): Assertion =
          playMongoCollection.collection
            .find(filter = Filters.equal(key, value))
            .toFuture
            .futureValue shouldBe List(myObj)

        checkFind("string"       , myObj.string       )
        checkFind("boolean"      , myObj.boolean      )
        checkFind("int"          , myObj.int          )
        checkFind("long"         , myObj.long         )
        checkFind("double"       , myObj.double       )
        checkFind("bigDecimal"   , myObj.bigDecimal   )
        checkFind("dateTime"     , myObj.dateTime     )
        checkFind("localDate"    , myObj.localDate    )
        checkFind("localDateTime", myObj.localDateTime)
        // checkFind("sum"          , myObj.sum          )
      }
    }

    "update fields" in {
      forAll(myObjectGen) { originalObj =>
        forAll(myObjectGen suchThat(_ != originalObj)) { targetObj =>
          dropDatabase()

          val result = playMongoCollection.collection.insertOne(originalObj).toFuture
          result.futureValue shouldBe Completed()

          def checkUpdate(key: String, value: Any): Assertion =
            playMongoCollection.collection
              .updateOne(filter = new com.mongodb.BasicDBObject(), update = Updates.set(key, value))
              .toFuture
              .futureValue
              .wasAcknowledged shouldBe true

          checkUpdate("string"       , targetObj.string       )
          checkUpdate("boolean"      , targetObj.boolean      )
          checkUpdate("int"          , targetObj.int          )
          checkUpdate("long"         , targetObj.long         )
          checkUpdate("double"       , targetObj.double       )
          checkUpdate("bigDecimal"   , targetObj.bigDecimal   )
          checkUpdate("dateTime"     , targetObj.dateTime     )
          checkUpdate("localDate"    , targetObj.localDate    )
          checkUpdate("localDateTime", targetObj.localDateTime)
          // checkUpdate("sum"          , targetObj.sum          )

          val writtenObj = playMongoCollection.collection.find().toFuture
          writtenObj.futureValue shouldBe List(targetObj)
        }
      }
    }
  }

  def dropDatabase() =
    mongoComponent.database
      .drop()
      .toFuture
      .futureValue
}

object PlayMongoCollectionSpec {

  case class StringWrapper    (unwrap: String    ) extends AnyVal
  case class BooleanWrapper   (unwrap: Boolean   ) extends AnyVal
  case class IntWrapper       (unwrap: Int       ) extends AnyVal
  case class LongWrapper      (unwrap: Long      ) extends AnyVal
  case class DoubleWrapper    (unwrap: Double    ) extends AnyVal
  case class BigDecimalWrapper(unwrap: BigDecimal) extends AnyVal

  sealed trait Sum
  object Sum {
    case object Sum1 extends Sum
    case object Sum2 extends Sum
  }

  case class MyObject(
    // Wrappers
    string    : StringWrapper
  , boolean   : BooleanWrapper
  , int       : IntWrapper
  , long      : LongWrapper
  , double    : DoubleWrapper
  , bigDecimal: BigDecimalWrapper
    // Sum type (WIP)
  , sum       : Sum
    // Joda time
  , dateTime       : DateTime
  , localDate      : LocalDate
  , localDateTime  : LocalDateTime
  )

  implicit lazy val stringWrapperFormat: Format[StringWrapper] =
    implicitly[Format[String]].inmap(StringWrapper.apply, unlift(StringWrapper.unapply))

  implicit lazy val booleanWrapperFormat: Format[BooleanWrapper] =
    implicitly[Format[Boolean]].inmap(BooleanWrapper.apply, unlift(BooleanWrapper.unapply))

  implicit lazy val intWrapperFormat: Format[IntWrapper] =
    implicitly[Format[Int]].inmap(IntWrapper.apply, unlift(IntWrapper.unapply))

  implicit lazy val longWrapperFormat: Format[LongWrapper] =
    implicitly[Format[Long]].inmap(LongWrapper.apply, unlift(LongWrapper.unapply))

  implicit lazy val doubleWrapperFormat: Format[DoubleWrapper] =
    implicitly[Format[Double]].inmap(DoubleWrapper.apply, unlift(DoubleWrapper.unapply))

  implicit lazy val bigDecimalWrapperFormat: Format[BigDecimalWrapper] =
    implicitly[Format[BigDecimal]].inmap(BigDecimalWrapper.apply, unlift(BigDecimalWrapper.unapply))


  // TODO this is ineffective - codec is looked up by val.getClass
  // i.e. classOf[Sum.Sum1] not classOf[Sum]
  // Note, codec macro would generate a codec for both classOf[Sum.Sum1] and classOf[Sum.Sum2]
  implicit lazy val sumFormat: Format[Sum] = new Format[Sum] {
    override def reads(js: JsValue) =
      js.validate[String]
        .flatMap { case "Sum1" => JsSuccess(Sum.Sum1)
                   case "Sum2" => JsSuccess(Sum.Sum2)
                   case other  => JsError(__, s"Unexpected Sum value $other")
                 }

    override def writes(sum: Sum) =
      sum match {
        case Sum.Sum1 => JsString("Sum1")
        case Sum.Sum2 => JsString("Sum2")
      }
  }

  import MongoJodaFormats.Implicits._

  val myObjectFormat =
    ( (__ \ "string"       ).format[StringWrapper]
    ~ (__ \ "boolean"      ).format[BooleanWrapper]
    ~ (__ \ "int"          ).format[IntWrapper]
    ~ (__ \ "long"         ).format[LongWrapper]
    ~ (__ \ "double"       ).format[DoubleWrapper]
    ~ (__ \ "bigDecimal"   ).format[BigDecimalWrapper]
    ~ (__ \ "sum"          ).format[Sum]
    ~ (__ \ "dateTime"     ).format[DateTime]
    ~ (__ \ "localDate"    ).format[LocalDate]
    ~ (__ \ "localDateTime").format[LocalDateTime]
    )(MyObject.apply _, unlift(MyObject.unapply))


  def myObjectGen =
    for {
      s       <- Arbitrary.arbitrary[String]
      b       <- Arbitrary.arbitrary[Boolean]
      i       <- Arbitrary.arbitrary[Int]
      l       <- Arbitrary.arbitrary[Long]
      d       <- Arbitrary.arbitrary[Double]
      bd      <- Arbitrary.arbitrary[BigDecimal]
                   // TODO is it reasonable to only handle BigDecimal within Decimal128 range?
                   .suchThat(bd => scala.util.Try(new org.bson.types.Decimal128(bd.bigDecimal)).isSuccess)
      instant <- Gen.choose(0L, System.currentTimeMillis * 2) // Keep Dates within range (ArithmeticException for any Long.MAX_VALUE)
    } yield
      MyObject(
          string        = StringWrapper(s)
        , boolean       = BooleanWrapper(b)
        , int           = IntWrapper(i)
        , long          = LongWrapper(l)
        , double        = DoubleWrapper(d)
        , bigDecimal    = BigDecimalWrapper(bd)
        , sum           = Sum.Sum1
        , dateTime      = new DateTime(instant, DateTimeZone.UTC) // Mongo db assumes UTC (timezone is not stored in db - when read back, it will represent the same instant, but with timezone UTC)
        , localDate     = new LocalDate(instant)
        , localDateTime = new LocalDateTime(instant)
        )
}