// Copyright (c) 2018 by Rob Norris
// This software is licensed under the MIT License (MIT).
// For more information see LICENSE or https://opensource.org/licenses/MIT

package skunk.util

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._

trait Namer[F[_]] {
  def nextName(prefix: String): F[String]
}

object Namer {

  def apply[F[_]: Sync]: F[Namer[F]] =
    Ref[F].of(1).map { ctr =>
      new Namer[F] {
        override def nextName(prefix: String): F[String] =
          ctr.modify(n => (n + 1, s"${prefix}_$n"))
      }
    }

}