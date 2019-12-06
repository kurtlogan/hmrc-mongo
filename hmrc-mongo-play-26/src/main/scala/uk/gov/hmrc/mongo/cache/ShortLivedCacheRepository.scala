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

package uk.gov.hmrc.mongo.cache

import com.google.inject.Inject
import play.api.libs.json.Format
import uk.gov.hmrc.mongo.cache.collection.PlayMongoCacheCollection
import uk.gov.hmrc.mongo.{MongoComponent, TimestampSupport}

import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

class ShortLivedCacheRepository[A: ClassTag] @Inject()(
  mongoComponent: MongoComponent,
  collectionName: String = "short-lived-cache",
  format: Format[A],
  ttl: Duration = 5.minutes, // TODO any reason to provide default value?
  timestampSupport: TimestampSupport)(implicit ec: ExecutionContext)
    extends PlayMongoCacheCollection(
      mongoComponent   = mongoComponent,
      collectionName   = collectionName,
      ttl              = ttl,
      timestampSupport = timestampSupport
    ) {

  implicit val f = format

  val dataKey = "dataKey"

  def cache(key: String, body: A): Future[Unit] =
    put(key, dataKey, body)

  def fetch(key: String): Future[Option[A]] =
    get(key, dataKey)
}
