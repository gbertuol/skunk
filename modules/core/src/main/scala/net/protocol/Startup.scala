// Copyright (c) 2018 by Rob Norris
// This software is licensed under the MIT License (MIT).
// For more information see LICENSE or https://opensource.org/licenses/MIT

package skunk.net.protocol

import cats.MonadError
import cats.implicits._
import natchez.Trace
import natchez.TraceValue.StringValue
import skunk.exception.StartupException
import skunk.net.MessageSocket
import skunk.net.message._

trait Startup[F[_]] {
  def apply(user: String, database: String, extraOptions: Map[String, String]): F[Unit]
}

object Startup {

  def apply[F[_]: MonadError[?[_], Throwable]: Exchange: MessageSocket: Trace]: Startup[F] =
    new Startup[F] {
      override def apply(user: String, database: String, extraOptions: Map[String, String]): F[Unit] =
        exchange("startup") {
          val sm = StartupMessage(user, database, extraOptions)
          for {
            _ <- Trace[F].put(sm.properties.toIndexedSeq.map { case (k, v) => k -> StringValue(v) } :_*)
            _ <- send(sm)
            _ <- expect { case AuthenticationOk => }
            _ <- flatExpect {
                   case ReadyForQuery(_) => ().pure[F]
                   case ErrorResponse(info) =>
                    val e = new StartupException(info, sm.properties)
                    e.raiseError[F, Unit]
                 }
          } yield ()
        }
    }

}