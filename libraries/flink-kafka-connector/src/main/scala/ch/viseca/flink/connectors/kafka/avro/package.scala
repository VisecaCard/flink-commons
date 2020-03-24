/*
   Copyright 2020 Viseca Card Services SA

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package ch.viseca.flink.connectors.kafka

import java.nio.ByteBuffer
import java.time.Instant

import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.util.Utf8

/** Helper functions to process AVRO records */
package object avro {

  /** Implicit conversion for fields of AVRO [[IndexedRecord]] */
  implicit class AvroConverter(val record: IndexedRecord) extends AnyVal {
    /** Converts an optional AVRO decimal field to [[ Option[BigDecimal] ]], e.g. :
      * <pre>
      * [
      *   "null",
      *   {
      *     "type": "bytes",
      *     "scale": 3,
      *     "precision": 64
      *     "logicalType": "decimal"
      *   }
      * ]
      * </pre>
      *
      * @param fieldIndex index of the field
      * */
    def getDecimalOption(fieldIndex: Int): Option[BigDecimal] = {
      val recordSchema = record.getSchema
      val field = recordSchema.getFields.get(fieldIndex)
      getDecimalOptionImpl(field)
    }

    /** Converts an optional AVRO decimal field to [[ Option[BigDecimal] ]], e.g. :
      * <pre>
      * [
      *   "null",
      *   {
      *     "type": "bytes",
      *     "scale": 3,
      *     "precision": 64
      *     "logicalType": "decimal"
      *   }
      * ]
      * </pre>
      *
      * @param fieldName name of the field
      * */
    def getDecimalOption(fieldName: String): Option[BigDecimal] = {
      val recordSchema = record.getSchema
      val field = recordSchema.getField(fieldName)
      getDecimalOptionImpl(field)
    }

    private def getDecimalOptionImpl(field: Schema.Field) = {
      val fieldIndex = field.pos()
      val value = record.get(fieldIndex)
      value match {
        case null => None
        case (buf: ByteBuffer) => {
          val fieldSchema = field.schema()
          val decSchema = fieldSchema.getTypes.get(fieldSchema.getIndexNamed("bytes"))
          val decimalType = decSchema.getLogicalType.asInstanceOf[LogicalTypes.Decimal]
          val dec = BigDecimal(AvroConverter.decimalConversion.fromBytes(buf, fieldSchema, decimalType))
          Some(dec)
        }
      }
    }

    /** Converts an AVRO decimal field to [[BigDecimal]], e.g. :
      * <pre>
      * {
      *   "type": "bytes",
      *   "scale": 3,
      *   "precision": 64
      *   "logicalType": "decimal"
      * }
      * </pre>
      *
      * @param fieldIndex index of the field
      * */
    def getDecimal(fieldIndex: Int): BigDecimal = {
      val recordSchema = record.getSchema
      val field = recordSchema.getFields.get(fieldIndex)
      getDecimalImpl(field)
    }

    /** Converts an AVRO decimal field to [[BigDecimal]], e.g. :
      * <pre>
      * {
      *   "type": "bytes",
      *   "scale": 3,
      *   "precision": 64
      *   "logicalType": "decimal"
      * }
      * </pre>
      *
      * @param fieldName name of the field
      * */
    def getDecimal(fieldName: String): BigDecimal = {
      val recordSchema = record.getSchema
      val field = recordSchema.getField(fieldName)
      getDecimalImpl(field)
    }

    private def getDecimalImpl(field: Schema.Field) = {
      val fieldIndex = field.pos()
      val buf = record.get(fieldIndex).asInstanceOf[ByteBuffer]
      val fieldSchema = field.schema()
      val decimalType = fieldSchema.getLogicalType.asInstanceOf[LogicalTypes.Decimal]
      val dec = BigDecimal(AvroConverter.decimalConversion.fromBytes(buf, fieldSchema, decimalType))
      dec
    }

    /** Converts an optional AVRO long field to [[ Option[Instant] ]].
      * <p> The conversion is based on epoch time, i.e. if no logical type is given, epoch milli seconds.
      * The conversion is adjusted accordingly if a logical type is specified,
      * e.g. :</p>
      * <pre>
      * [
      *   "null",
      *   {
      *     "type": "long",
      *     "logicalType": "timestamp-millis"
      *   }
      * ]
      * </pre>
      *
      * @param fieldIndex index of the field
      * */
    def getInstantOption(fieldIndex: Int) = {
      val recordSchema = record.getSchema
      val field = recordSchema.getFields.get(fieldIndex)
      getInstantOptionImpl(field)
    }

    /** Converts an optional AVRO long field to [[ Option[Instant] ]].
      * <p> The conversion is based on epoch time, i.e. if no logical type is given, epoch milli seconds.
      * The conversion is adjusted accordingly if a logical type is specified,
      * e.g. :</p>
      * <pre>
      * [
      *   "null",
      *   {
      *     "type": "long",
      *     "logicalType": "timestamp-millis"
      *   }
      * ]
      * </pre>
      *
      * @param fieldName name of the field
      * */
    def getInstantOption(fieldName: String) = {
      val recordSchema = record.getSchema
      val field = recordSchema.getField(fieldName)
      getInstantOptionImpl(field)
    }

    private def getInstantOptionImpl(field: Schema.Field) = {
      val fieldIndex = field.pos()
      val value = record.get(fieldIndex)
      value match {
        case null => None
        case _ => {
          val epoch = value.asInstanceOf[Long]
          val fieldSchema = field.schema()
          val longSchema = fieldSchema.getTypes.get(fieldSchema.getIndexNamed("long"))
          val logType = longSchema.getLogicalType
          val instant =
            logType match {
              case null => //assume epoch millis
                Instant.ofEpochMilli(epoch)
              case _: LogicalTypes.TimestampMillis => Instant.ofEpochMilli(epoch)
              case _: LogicalTypes.TimestampMicros => {
                val secs = Math.floorDiv(epoch, 1000000)
                val mos = Math.floorMod(epoch, 1000000).toInt
                Instant.ofEpochSecond(secs, mos * 1000)
              }
            }
          Some(instant)
        }
      }
    }

    /** Converts an AVRO long field to [[Instant]].
      * <p> The conversion is based on epoch time, i.e. if no logical type is given, epoch milli seconds.
      * The conversion is adjusted accordingly if a logical type is specified,
      * e.g. :</p>
      * <pre>
      * [
      *   "null",
      *   {
      *     "type": "long",
      *     "logicalType": "timestamp-millis"
      *   }
      * ]
      * </pre>
      *
      * @param fieldIndex index of the field
      * */
    def getInstant(fieldIndex: Int) = {
      val recordSchema = record.getSchema
      val field = recordSchema.getFields.get(fieldIndex)
      getInstantImpl(field)
    }

    /** Converts an AVRO long field to [[Instant]].
      * <p> The conversion is based on epoch time, i.e. if no logical type is given, epoch milli seconds.
      * The conversion is adjusted accordingly if a logical type is specified,
      * e.g. :</p>
      * <pre>
      * [
      *   "null",
      *   {
      *     "type": "long",
      *     "logicalType": "timestamp-millis"
      *   }
      * ]
      * </pre>
      *
      * @param fieldName name of the field
      * */
    def getInstant(fieldName: String) = {
      val recordSchema = record.getSchema
      val field = recordSchema.getField(fieldName)
      getInstantImpl(field)
    }

    private def getInstantImpl(field: Schema.Field) = {
      val fieldIndex = field.pos()
      val value = record.get(fieldIndex).asInstanceOf[Long]
      val fieldSchema = field.schema()
      val logType = fieldSchema.getLogicalType
      val instant =
        logType match {
          case null => //assume epoch millis
            Instant.ofEpochMilli(value)
          case _: LogicalTypes.TimestampMillis => Instant.ofEpochMilli(value)
          case _: LogicalTypes.TimestampMicros => {
            val secs = Math.floorDiv(value, 1000000)
            val mos = Math.floorMod(value, 1000000).toInt
            Instant.ofEpochSecond(secs, mos * 1000)
          }
        }
      instant
    }

    /** Converts an (optional/non-optional) AVRO string field to [[String]].
      * <p> The conversion automatically converts from either String or Utf8 type.</p>
      *
      * @param fieldIndex index of the field
      * */
    def getString(fieldIndex: Int): String = {
      val value = record.get(fieldIndex)
      value match {
        case null => null
        case utf: Utf8 => utf.toString
        case str: String => str
      }
    }

    /** Converts an (optional/non-optional) AVRO string field to [[String]].
      * <p> The conversion automatically converts from either String or Utf8 type.</p>
      *
      * @param fieldName name of the field
      * */
    def getString(fieldName: String): String = {
      val recordSchema = record.getSchema
      val field = recordSchema.getField(fieldName)
      getString(field.pos())
    }
  }

  object AvroConverter {
    /** auxilliary conversion object */
    val decimalConversion = new DecimalConversion
  }

}
