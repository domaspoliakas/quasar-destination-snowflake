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

import scala._

import doobie._
import doobie.implicits._

sealed trait SnowflakeType extends Product with Serializable {
  def fragment: Fragment
}

object SnowflakeType {

  final case class NUMBER(precision: Int, scale: Int) extends SnowflakeType {
    def fragment: Fragment = fr0"NUMBER($precision, $scale)"
  }

  final case object FLOAT extends SnowflakeType {
    def fragment: Fragment = fr0"FLOAT"
  }

  final case class VARCHAR(size: Int) extends SnowflakeType {
    def fragment: Fragment = fr0"VARCHAR($size)"
  }

  final case object BINARY extends SnowflakeType {
    def fragment: Fragment = fr0"BINARY"
  }

  final case object BOOLEAN extends SnowflakeType {
    def fragment: Fragment = fr0"BOOLEAN"
  }

  final case object DATE extends SnowflakeType {
    def fragment: Fragment = fr0"DATE"
  }

  final case object BYTEINT extends SnowflakeType {
    def fragment: Fragment = fr0"BYTEINT"
  }

  final case class TIME(precision: Int) extends SnowflakeType {
    def fragment: Fragment = fr0"TIME($precision)"
  }

  final case object TIMESTAMP_LTZ extends SnowflakeType {
    def fragment: Fragment = fr0"TIMESTAMP_LTZ"
  }

  final case object TIMESTAMP_NTZ extends SnowflakeType {
    def fragment: Fragment = fr0"TIMESTAMP_NTZ"
  }

  final case object TIMESTAMP_TZ extends SnowflakeType {
    def fragment: Fragment = fr0"TIMESTAMP_TZ"
  }

  final case object VARIANT extends SnowflakeType {
    def fragment: Fragment = fr0"VARIANT"
  }

  final case object OBJECT extends SnowflakeType {
    def fragment: Fragment = fr0"OBJECT"
  }

  final case object ARRAY extends SnowflakeType {
    def fragment: Fragment = fr0"ARRAY"
  }

  final case object GEOGRAPHY extends SnowflakeType {
    def fragment: Fragment = fr0"GEOGRAPHY"
  }

}
