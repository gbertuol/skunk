package skunk
package codec

import java.nio.ByteBuffer

import cats.implicits._
import skunk.data.Type

trait LsnCodec {

  val pg_lsn = Codec.simple[Long](LsnCodec.fromLong(_), LsnCodec.toLong(_), Type.pg_lsn)

}

object LsnCodec {

  val LSNPattern = """([A-F0-9]+)/([A-F0-9]+)""".r

  def toLong(lsn: String): Either[String, Long] = {
    lsn match {
      case LSNPattern(logicalXlog, segment) =>
        val bbuf = ByteBuffer.allocate(8)
        bbuf.putInt(java.lang.Long.parseLong(logicalXlog, 16).intValue)
        bbuf.putInt(java.lang.Long.parseLong(segment, 16).intValue)
        bbuf.position(0)
        bbuf.getLong.asRight
      case x => s"Expected $LSNPattern, got $x".asLeft
    }

  }

  def fromLong(lsn: Long): String = {
    val bbuf = ByteBuffer.allocate(8)
    bbuf.putLong(lsn)
    bbuf.position(0)
    val logicalXlog = bbuf.getInt
    val segment = bbuf.getInt
    String.format("%X/%X", logicalXlog, segment)
  }
}
