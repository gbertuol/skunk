// Copyright (c) 2018 by Rob Norris
// This software is licensed under the MIT License (MIT).
// For more information see LICENSE or https://opensource.org/licenses/MIT

package skunk.net.message

import scodec._
import scodec.codecs._

// TODO: SUPPORT OTHER PARAMETERS
case class StartupMessage(user: String, database: String, extraOptions: Map[String, String]) {

  // HACK: we will take a plist eventually
  val properties: Map[String, String] =
    Map("user" -> user, "database" -> database) ++ extraOptions

}

object StartupMessage {

  implicit val StartupMessageFrontendMessage: FrontendMessage[StartupMessage] =
    FrontendMessage.untagged {

      val version: Codec[Unit] =
        int32.applied(196608)

      // null-terminated list of fixed key-value pairs.
      val parameters: Codec[List[(String, String)]] = list(cstring pairedWith cstring) <~ byte.applied(0)

      def assembleParameters(m: StartupMessage): List[(String, String)] =
       (
         m.properties ++
         Map(
           // these specify connection properties that affect serialization and are REQUIRED by Skunk.
           // by appending it to extra we ensure these are kept in case extra had one of them
           //#config
           "client_min_messages" -> "WARNING",
           "DateStyle"           -> "ISO, MDY",
           "IntervalStyle"       -> "iso_8601",
           "client_encoding"     -> "UTF8",
           //#config
         )
        ).toList

      (version ~> parameters)
        .asEncoder
        .contramap(m => assembleParameters(m))

    }

}