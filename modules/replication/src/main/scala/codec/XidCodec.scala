package skunk
package codec

import cats.implicits._
import skunk.data.Type

trait XidCodec {
  val xid: Codec[Int] = Codec.simple(_.toString, _.toInt.asRight, Type.xid)
}
