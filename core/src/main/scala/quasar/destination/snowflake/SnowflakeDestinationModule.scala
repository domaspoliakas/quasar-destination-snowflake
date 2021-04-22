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

import scala.Predef._
import scala._

import quasar.api.destination.DestinationError.InitializationError
import quasar.api.destination.{DestinationError, DestinationType}
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{Destination, DestinationModule, PushmiPullyu}
import quasar.concurrent._

import java.sql.SQLException
import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext
import scala.util.{Either, Random}

import argonaut._, Argonaut._

import cats.MonadError
import cats.data.EitherT
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.hikari.HikariTransactor

import org.slf4s.LoggerFactory

import scalaz.NonEmptyList

object SnowflakeDestinationModule extends DestinationModule {
  def destinationType: DestinationType =
    DestinationType("snowflake", 1L)

  val SnowflakeDriverFqcn = "net.snowflake.client.jdbc.SnowflakeDriver"
  val Redacted = "<REDACTED>"
  val PoolSize: Int = 10

  def sanitizeDestinationConfig(config: Json): Json =
    config.as[SnowflakeConfig].result.fold(_ => Json.jEmptyObject, cfg =>
      cfg.copy(user = User(Redacted), password = Password(Redacted)).asJson)

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json,
      pushPull: PushmiPullyu[F])
      : Resource[F, Either[InitializationError[Json], Destination[F]]] = {
    val init = for {
      cfg <- EitherT.fromEither[Resource[F, ?]](config.as[SnowflakeConfig].result) leftMap {
        case (err, _) => DestinationError.malformedConfiguration((destinationType, config, err))
      }
      poolSuffix <- EitherT.right(Resource.liftF(Sync[F].delay(Random.alphanumeric.take(5).mkString)))
      connectPool <- EitherT.right(boundedPool[F](s"snowflake-dest-connect-$poolSuffix", PoolSize))
      transactPool <- EitherT.right(Blocker.cached[F](s"snowflake-dest-transact-$poolSuffix"))

      jdbcUri = SnowflakeConfig.configToUri(cfg)

      transactor <- EitherT.right[InitializationError[Json]](
        HikariTransactor.newHikariTransactor[F](
          SnowflakeDriverFqcn,
          jdbcUri,
          cfg.user.value,
          cfg.password.value,
          connectPool,
          transactPool))

      _ <- isLive(transactor, config)

      logger <- EitherT.right[InitializationError[Json]]{
        Resource.liftF(Sync[F].delay(LoggerFactory(s"quasar.lib.destination.snowflake-$poolSuffix")))
      }

    } yield new SnowflakeDestination(
      transactor,
      cfg.sanitizeIdentifiers,
      logger): Destination[F]

    init.value
  }

  private def isLive[F[_]: Sync](xa: Transactor[F], config: Json)
      : EitherT[Resource[F, ?], InitializationError[Json], Unit] = {
    val MER = MonadError[Resource[F, ?], Throwable]

    MER.attemptT(fr0"SELECT current_version()"
      .query[Unit].stream.transact(xa).compile.resource.drain) leftSemiflatMap {

      case (ex: SQLException) if ex.getErrorCode === ErrorCode.AccessDenied =>
        DestinationError.accessDenied(
          (destinationType, config, ex.getSQLState)).pure[Resource[F, ?]]

      case (ex: SQLException) if ex.getErrorCode === ErrorCode.ConnectionFailed =>
        DestinationError.connectionFailed(
          (destinationType, config, ex)).pure[Resource[F, ?]]

      case (ex: SQLException) =>
        DestinationError.invalidConfiguration(
          (destinationType,
            config,
            NonEmptyList(ex.getMessage, ex.getErrorCode.toString, ex.getSQLState)))
          .pure[Resource[F, ?]]

      case ex => MER.raiseError(ex)
    }
  }

  private object ErrorCode {
    val AccessDenied: Int = 390100
    val ConnectionFailed: Int = 200015
  }

  private def boundedPool[F[_]: Sync](name: String, threadCount: Int): Resource[F, ExecutionContext] =
    Resource.make(
      Sync[F].delay(
        Executors.newFixedThreadPool(
          threadCount,
          NamedDaemonThreadFactory(name))))(es => Sync[F].delay(es.shutdown()))
      .map(ExecutionContext.fromExecutor(_))
}
