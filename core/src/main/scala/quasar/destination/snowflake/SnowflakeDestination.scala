/*
 * Copyright 2020 Precog Data
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

package quasar.destination.snowflake

import scala._, Predef._

import cats.data.NonEmptyList
import cats.effect._

import doobie.Transactor

import monocle.Prism

import quasar.api.{ColumnType, Label}
import quasar.api.push.TypeCoercion
import quasar.api.destination._
import quasar.connector.MonadResourceErr
import quasar.connector.destination._
import quasar.connector.render.RenderConfig
import quasar.lib.jdbc.destination.WriteMode

import org.slf4s.Logger

import scala.concurrent.duration._
import net.snowflake.client.jdbc.SnowflakeType

final class SnowflakeDestination[F[_]: ConcurrentEffect: MonadResourceErr: Timer: ContextShift](
    val transactor: Resource[F, Transactor[F]],
    writeMode: WriteMode,
    schema: String,
    hygienicIdent: String => String,
    retryTimeout: FiniteDuration,
    maxRetries: Int,
    val blocker: Blocker,
    val logger: Logger)
    extends Flow.Sinks[F] with Destination[F] {

  type Type = SnowflakeType
  type TypeId = SnowflakeTypeId

  val typeIdOrdinal: Prism[Int, SnowflakeTypeId] = SnowflakeTypeId.ordinalPrism

  implicit val typeIdLabel: Label[TypeId] = SnowflakeTypeId.label

  def coerce(tpe: ColumnType.Scalar): TypeCoercion[SnowflakeTypeId] = tpe match {
    case ColumnType.Boolean =>
      TypeCoercion.Satisfied(NonEmptyList.one(SnowflakeTypeId.BOOLEAN))

    case ColumnType.LocalDateTime => 
      TypeCoercion.Satisfied(NonEmptyList.of(
        SnowflakeTypeId.TIMESTAMP_NTZ))

    case ColumnType.OffsetTime => 
      TypeCoercion.Unsatisfied(List(), None)

    case ColumnType.Interval => 
      TypeCoercion.Unsatisfied(List(), None)

    case ColumnType.String => 
      TypeCoercion.Satisfied(NonEmptyList.of(
        SnowflakeTypeId.VARCHAR,
        SnowflakeTypeId.BINARY))

    case ColumnType.Null => 
      TypeCoercion.Satisfied(
        NonEmptyList.of(
          SnowflakeTypeId.BYTEINT))

    case ColumnType.Number => 
      TypeCoercion.Satisfied(
        NonEmptyList.of(
          SnowflakeTypeId.NUMBER,
          SnowflakeTypeId.FLOAT))
      
    case ColumnType.OffsetDateTime => 
      TypeCoercion.Satisfied(
        NonEmptyList.of(
          SnowflakeTypeId.TIMESTAMP_TZ))

    case ColumnType.LocalTime => 
      TypeCoercion.Satisfied(NonEmptyList.one(SnowflakeTypeId.TIME))

    case ColumnType.OffsetDate => 
      TypeCoercion.Unsatisfied(List(), None)

    case ColumnType.LocalDate => 
      TypeCoercion.Satisfied(NonEmptyList.one(SnowflakeTypeId.DATE))

  }

  def construct(id: SnowflakeTypeId): Either[SnowflakeType,Constructor[SnowflakeType]] = ???

  def destinationType: DestinationType =
    SnowflakeDestinationModule.destinationType

  def tableBuilder(args: Flow.Args, xa: Transactor[F], logger: Logger): Resource[F, TempTable.Builder[F]] =
    Resource.eval(TempTable.builder[F](writeMode, schema, hygienicIdent, args, xa, logger))

  def render: RenderConfig[Byte] = RenderConfig.Csv(includeHeader = false)

  val sinks: NonEmptyList[ResultSink[F, SnowflakeType]] =
    flowSinks
}
