package quasar.destination.snowflake

import scala._

import quasar.api.Label

sealed abstract class SnowflakeTypeId(val ordinal: Int) extends Product with Serializable

object SnowflakeTypeId {
  import monocle.Prism

  // https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
  final case object NUMBER extends SnowflakeTypeId(0)
  final case object FLOAT extends SnowflakeTypeId(1)
  final case object VARCHAR extends SnowflakeTypeId(2)
  final case object BINARY extends SnowflakeTypeId(3)
  final case object BOOLEAN extends SnowflakeTypeId(4)
  final case object DATE extends SnowflakeTypeId(5)
  final case object TIME extends SnowflakeTypeId(6)
  final case object TIMESTAMP_LTZ extends SnowflakeTypeId(7)
  final case object TIMESTAMP_NTZ extends SnowflakeTypeId(8)
  final case object TIMESTAMP_TZ extends SnowflakeTypeId(9)
  final case object VARIANT extends SnowflakeTypeId(10)
  final case object OBJECT extends SnowflakeTypeId(11)
  final case object ARRAY extends SnowflakeTypeId(12)
  final case object GEOGRAPHY extends SnowflakeTypeId(13)
  // Technically the same as NUMBER, but specified separately 
  // for ColumnType.Null so that null column wouldn't be specifying precision
  final case object BYTEINT extends SnowflakeTypeId(14)

  val ordinalPrism: Prism[Int, SnowflakeTypeId] =
    Prism.partial[Int, SnowflakeTypeId]({
      case NUMBER.ordinal => NUMBER
      case FLOAT.ordinal => FLOAT
      case VARCHAR.ordinal => VARCHAR
      case BINARY.ordinal => BINARY
      case BOOLEAN.ordinal => BOOLEAN
      case DATE.ordinal => DATE
      case TIME.ordinal => TIME
      case TIMESTAMP_LTZ.ordinal => TIMESTAMP_LTZ
      case TIMESTAMP_NTZ.ordinal => TIMESTAMP_NTZ
      case TIMESTAMP_TZ.ordinal => TIMESTAMP_TZ
      case VARIANT.ordinal => VARIANT
      case OBJECT.ordinal => OBJECT
      case ARRAY.ordinal => ARRAY
      case GEOGRAPHY.ordinal => GEOGRAPHY
      case BYTEINT.ordinal => BYTEINT
    })(_.ordinal)

  val label = Label[SnowflakeTypeId] {
      case NUMBER => "NUMBER/DECIMAL/NUMERIC/INT/INTEGER/BIGINT/SMALLINT/TINYINT"
      case FLOAT => "FLOAT/FLOAT4/FLOAT8/DOUBLE/DOUBLE PRECISION/REAL"
      case VARCHAR => "VARCHAR/CHAR/CHARACTER/STRING/TEXT"
      case BINARY => "BINARY/VARBINARY"
      case BOOLEAN => "BOOLEAN"
      case DATE => "DATE"
      case TIME => "TIME"
      case TIMESTAMP_LTZ => "TIMESTAMP_LTZ"
      case TIMESTAMP_NTZ => "TIMESTAMP_NTZ/DATETIME"
      case TIMESTAMP_TZ => "TIMESTAMP_TZ"
      case VARIANT => "VARIANT"
      case OBJECT => "OBJECT"
      case ARRAY => "ARRAY"
      case GEOGRAPHY => "GEOGRAPHY"
      case BYTEINT => "BYTEINT"
  }
}
