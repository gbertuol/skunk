// Copyright (c) 2018 by Rob Norris
// This software is licensed under the MIT License (MIT).
// For more information see LICENSE or https://opensource.org/licenses/MIT

package example

import cats.effect._
import natchez.Trace
import skunk._
import skunk.codec.all._
import skunk.implicits._

object Replication extends IOApp {

  def putStrLn[F[_]: Sync](a: Any): F[Unit] =
    Sync[F].delay(println(a))

  def session[F[_]: Concurrent: ContextShift: Trace]: Resource[F, Session[F]] =
    Session.single(
      host = "localhost",
      port = 5432,
      user = "postgres",
      database = "world",
      extraOptions = Map(
        "replication" -> "database"
      ),
      debug = true
    )

  def run(args: List[String]): IO[ExitCode] = {
    import natchez.Trace.Implicits.noop

    session[IO].use { s =>
      for {
        id <- s.unique(identifySystem)
        _ <- putStrLn[IO](id)
      } yield ExitCode.Success
    }
  }

  case class SystemIdentity(systemId: String, timeline: Int, xlogpos: String, dbname: String)
  val SystemIdentityDecoder: Decoder[SystemIdentity] =
    (text ~ int4 ~ text ~ text).map { case id ~ t ~ xlog ~ db => SystemIdentity(id, t, xlog, db) }

  val identifySystem: Query[Void, SystemIdentity] = sql"IDENTIFY_SYSTEM".query(SystemIdentityDecoder)

}


