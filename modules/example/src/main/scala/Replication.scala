// Copyright (c) 2018 by Rob Norris
// This software is licensed under the MIT License (MIT).
// For more information see LICENSE or https://opensource.org/licenses/MIT

package example

import cats.effect._
import cats.implicits._
import natchez.Trace
import skunk._
import skunk.codec.LsnCodec
import skunk.codec.replication._
import skunk.implicits._
import skunk.util.Origin

object Replication extends IOApp {

  final val database = "world"
  final val slotName = "repl_slot_4"
  final val plugin = "pgoutput"
  final val publicationName = "publ_1"

  def putStrLn[F[_]: Sync](a: Any): F[Unit] =
    Sync[F].delay(println(a))

  def session[F[_]: Concurrent: ContextShift: Trace]: Resource[F, Session[F]] =
    Session.single(
      host = "localhost",
      port = 5432,
      user = "postgres",
      database = database,
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
        slotInfo <- s.option(getSlotInfo)
        _ <- putStrLn[IO](slotInfo)
        walStartLSN <- slotInfo match {
          case None => s.unique(createReplicationSlot)
          case Some(si) => si.confirmedFlushLSN.pure[IO]
        }
        _ <- putStrLn[IO](walStartLSN)
      } yield ExitCode.Success
    }
  }

  case class SystemIdentity(systemId: String, timeline: Int, xlogpos: String, dbname: String)
  val SystemIdentityDecoder: Decoder[SystemIdentity] =
    (text ~ int4 ~ text ~ text).map { case id ~ t ~ xlog ~ db => SystemIdentity(id, t, xlog, db) }

  val identifySystem: Query[Void, SystemIdentity] = sql"IDENTIFY_SYSTEM".query(SystemIdentityDecoder)

  case class SlotInfo(isActive: Boolean, confirmedFlushLSN: Long, restartLSN: Long, xmin: Option[Int])
  val SlotInfoDecoder: Decoder[SlotInfo] =
    (bool ~ pg_lsn ~ pg_lsn ~ xid.opt).map { case isActive ~ clsn ~ rlsn ~ xmin =>
      SlotInfo(isActive, clsn, rlsn, xmin)
    }

  // only works in simple query mode
  val getSlotInfo: Query[Void, SlotInfo] = {
    val sql = s"""
          SELECT
            active,
            confirmed_flush_lsn,
            restart_lsn,
            xmin
          FROM
            pg_replication_slots
          WHERE
            database = '$database'
            and slot_name = '$slotName'
            and plugin = '$plugin'
       """
    Query(sql, Origin.unknown, Void.codec, SlotInfoDecoder)
  }

  case class SlotCreated(slotName: String, consistentPoint: Long, snapshotName: String, outputPlugin: String) {
    val walStartLSN = consistentPoint
  }
  val SlotCreatedDecoder =
    (text ~ text ~ text ~ text).map { case slotName ~ consistentPoint ~ snapshotName ~ outputPlugin =>
      SlotCreated(slotName, LsnCodec.toLong(consistentPoint).getOrElse(-1), snapshotName, outputPlugin)
    }

  val createReplicationSlot: Query[Void, SlotCreated] =
    Query(s"""CREATE_REPLICATION_SLOT $slotName LOGICAL $plugin""", Origin.unknown, Void.codec, SlotCreatedDecoder)

  // only works in simple query mode
  val timelineHistory: Query[Int, String] = sql"TIMELINE_HISTORY $int4".query(text)
}

