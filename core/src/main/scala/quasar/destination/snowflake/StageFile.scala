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

import slamdata.Predef._

import cats.effect._
import cats.effect.concurrent.{Ref, Deferred}
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2._
import fs2.concurrent.Queue
import fs2.io

import java.util.UUID
import net.snowflake.client.jdbc.SnowflakeConnection
import org.slf4s.Logger

sealed trait StageFile[F[_]] {
  def ingest(chunk: Chunk[Byte]): F[Unit]
  def done: Resource[F, Option[Fragment]]
}

object StageFile {
  private val Compressed = true

  private final case class StageFileState[F[_]](
      q: Queue[F, Option[Chunk[Byte]]],
      isDone: Deferred[F, Unit],
      name: String)

  // Thread unsafe, `ingest` and `done` shouldn't be call in parallel
  def apply[F[_]: ConcurrentEffect: ContextShift](
      xa: Transactor[F],
      connection: SnowflakeConnection,
      blocker: Blocker,
      logger: Logger)
      : F[StageFile[F]] = {
    val debug = (s: String) => Sync[F].delay(logger.debug(s))

    for {
      rq <- Ref.of[F, Option[StageFileState[F]]](None)
    } yield {
      def getOrStart: F[StageFileState[F]] = rq.get flatMap {
        case Some(q) => q.pure[F]
        case None => for {
          q <- Queue.unbounded[F, Option[Chunk[Byte]]]
          unique <- Sync[F].delay(UUID.randomUUID.toString)
          name = s"precog_$unique"
          doneDeferred <- Deferred[F, Unit]
          state = StageFileState(q, doneDeferred, name)
          _ <- rq.set(state.some)
          _ <- ConcurrentEffect[F].start {
            debug(s"Starting staging to file: @~/$name") >>
            io.toInputStreamResource(q.dequeue.unNoneTerminate.flatMap(Stream.chunk(_))).use({ is =>
              blocker.delay[F, Unit](connection.uploadStream("@~", "/", is, name, Compressed))
            }) >>
            doneDeferred.complete(()) >>
            debug(s"Finished staging to file: @~/$name")
          }
        } yield state
      }
      new StageFile[F] {
        def ingest(c: Chunk[Byte]): F[Unit] =
          getOrStart.flatMap(_.q.enqueue1(c.some))

        def done: Resource[F, Option[Fragment]] = Resource.liftF(rq.get) flatMap { _.traverse { state =>
          val acquire =
            state.q.enqueue1(None) >>
            state.isDone.get >>
            rq.set(None) as
            Fragment.const0(state.name)

          val release: Fragment => F[Unit] = sf => {
            val fragment = fr0"rm @~/" ++ sf
            debug("Cleaning staging file @~/$name up") >>
            fragment.query[Unit].option.void.transact(xa)
          }
          Resource.make(acquire)(release)
        }}
      }
    }
  }
}
