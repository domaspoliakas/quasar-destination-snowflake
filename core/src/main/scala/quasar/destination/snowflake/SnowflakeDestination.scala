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

import cats.data.{NonEmptyList, Ior}
import cats.effect.{ConcurrentEffect, Timer, Resource, ContextShift}

import doobie.Transactor

import monocle.Prism

import quasar.api.{ColumnType, Label, Labeled}
import quasar.api.push.TypeCoercion
import quasar.api.push.param.Formal
import quasar.api.destination._
import quasar.connector.MonadResourceErr
import quasar.connector.destination._
import quasar.connector.render.RenderConfig
import quasar.lib.jdbc.destination.WriteMode
import quasar.lib.jdbc.destination.flow.{FlowSinks, FlowArgs, Flow, Retry}

import org.slf4s.Logger

import scala.concurrent.duration._
import quasar.api.push.param.IntegerStep

final class SnowflakeDestination[F[_]: ConcurrentEffect: MonadResourceErr: Timer: ContextShift](
    xa: Transactor[F],
    writeMode: WriteMode,
    schema: String,
    hygienicIdent: String => String,
    retryTimeout: FiniteDuration,
    maxRetries: Int,
    logger: Logger)
    extends Destination[F]
    with FlowSinks[F, SnowflakeType, Byte] {

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


  private def stepOne: IntegerStep = IntegerStep.Factor(0, 1)

  def construct(id: SnowflakeTypeId): Either[SnowflakeType,Constructor[SnowflakeType]] = id match {
    case SnowflakeTypeId.NUMBER => 
    // TODO double check the bounds are ok
    Right(Constructor.Binary(
      Labeled("precision", Formal.integer(Some(Ior.both(1, 38)), Some(stepOne), Some(36))),
      Labeled("scale", Formal.integer(Some(Ior.both(0, 37)), Some(stepOne), Some(3))),
      SnowflakeType.NUMBER(_, _)))
    case SnowflakeTypeId.FLOAT => 
    Right(Constructor.Binary(
      Labeled("precision", Formal.integer(Some(Ior.both(1, 38)), Some(stepOne), Some(36))),
      Labeled("scale", Formal.integer(Some(Ior.both(0, 37)), Some(stepOne), Some(3))),
      SnowflakeType.NUMBER(_, _)))

    case SnowflakeTypeId.VARCHAR => ???
    case SnowflakeTypeId.BINARY => ???
    case SnowflakeTypeId.BOOLEAN => ???
    case SnowflakeTypeId.DATE => ???
    case SnowflakeTypeId.TIME => ???
    case SnowflakeTypeId.TIMESTAMP_LTZ => ???
    case SnowflakeTypeId.TIMESTAMP_NTZ => ???
    case SnowflakeTypeId.TIMESTAMP_TZ => ???
    case SnowflakeTypeId.VARIANT => ???
    case SnowflakeTypeId.OBJECT => ???
    case SnowflakeTypeId.ARRAY => ???
    case SnowflakeTypeId.GEOGRAPHY => ???
    case SnowflakeTypeId.BYTEINT => ???
  }

  def destinationType: DestinationType =
    SnowflakeDestinationModule.destinationType

  def flowResource(args: FlowArgs[SnowflakeType]): Resource[F, Flow[Byte]] =
    TempTableFlow(xa, logger, writeMode, schema, hygienicIdent, args) map { (flow: Flow[Byte]) =>
      flow.mapK(Retry[F](maxRetries, retryTimeout))
    }

  def render(args: FlowArgs[ColumnType.Scalar]) = RenderConfig.Csv(includeHeader = false)

  val flowTransactor = xa
  val flowLogger = logger

  val sinks: NonEmptyList[ResultSink[F, SnowflakeType]] =
    flowSinks
}
