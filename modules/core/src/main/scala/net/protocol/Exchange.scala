// Copyright (c) 2018 by Rob Norris
// This software is licensed under the MIT License (MIT).
// For more information see LICENSE or https://opensource.org/licenses/MIT

package skunk.net.protocol

import cats.effect.Concurrent
import cats.effect.concurrent.Semaphore
import cats.effect.implicits._

trait Exchange[F[_]] {
  def apply[A](fa: F[A]): F[A]
}

object Exchange {

  def apply[F[_]: Concurrent](sem: Semaphore[F]): Exchange[F] =
    new Exchange[F] {
      override def apply[A](fa: F[A]): F[A] =
        sem.withPermit(fa).uncancelable
    }
}

