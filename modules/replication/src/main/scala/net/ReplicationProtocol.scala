package skunk.net

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ContextShift, Resource}
import fs2.io.tcp.SocketGroup
import natchez.Trace
import skunk.util.Typer
import skunk.{Query, Void}

import scala.concurrent.duration.FiniteDuration

trait ReplicationProtocol[F[_]] {

  def execute[A](query: Query[Void, A], typer: Typer): F[List[A]]

  def startup(user: String, database: String, extraOptions: Map[String, String]): F[Unit]
}

object ReplicationProtocol {

  def apply[F[_]: Concurrent: ContextShift: Trace](
    host: String,
    port: Int,
    debug: Boolean,
    readTimeout: FiniteDuration,
    writeTimeout: FiniteDuration,
    sg: SocketGroup
  ): Resource[F, ReplicationProtocol[F]] =
    for {
      bms <- BufferedMessageSocket[F](host, port, 256, debug, readTimeout, writeTimeout, sg)
      sem <- Resource.liftF(Semaphore[F](1))
    } yield
      new ReplicationProtocol[F] {

        implicit val ms: MessageSocket[F] = bms
        implicit val ExchangeF: protocol.Exchange[F] = protocol.Exchange.apply[F](sem)

        override def execute[A](query: Query[Void, A], typer: Typer): F[List[A]] =
          protocol.Query[F].apply(query, typer)

        override def startup(user: String, database: String, extraOptions: Map[String, String]): F[Unit] =
          protocol.Startup[F].apply(user, database, extraOptions)
      }

}