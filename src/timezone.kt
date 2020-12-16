package com.example

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.Function
import org.jetbrains.exposed.sql.vendors.MysqlDialect
import org.jetbrains.exposed.sql.vendors.currentDialect
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ISO_INSTANT
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.util.*

val DATE_TIME_SPACE_SEPARATED_WITH_TIMEZONE_STRING_FORMATTER: DateTimeFormatter by lazy {
    DateTimeFormatterBuilder()
//            .parseCaseInsensitive()
      .appendPattern("yyyy-MM-dd HH:mm:ss")
      .optionalStart()
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
      .optionalEnd()
      .optionalStart()
      .appendOffset("+HH:MM", "+00:00")
      .optionalEnd()
      .optionalStart()
      .appendOffset("+HH", "+00") // H2 format.
      .optionalEnd()
      .toFormatter(Locale.ROOT)
}

val DATE_TIME_SPACE_SEPARATED_WITHOUT_TIMEZONE_STRING_FORMATTER: DateTimeFormatter by lazy {
    DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .appendPattern("yyyy-MM-dd HH:mm:ss")
      .optionalStart()
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
      .optionalEnd()
      .toFormatter(Locale.ROOT)
}

class JavaTimeZonedDateTimeColumnType : ColumnType(), IDateColumnType {
    override fun sqlType(): String = "TIMESTAMPTZ"

    override fun nonNullValueToString(value: Any): String {
        val instant = when (value) {
            is String -> return value
            is ZonedDateTime -> Instant.from(value)
            is java.sql.Timestamp -> Instant.ofEpochSecond(value.time / 1000, value.nanos.toLong())
            else -> error("Unexpected value: $value of ${value::class.qualifiedName}")
        }
        return "'${ISO_INSTANT.format(instant)}'"

    }

    override fun valueFromDB(value: Any): Any = when (value) {
        is ZonedDateTime -> value
        is java.sql.Date -> Instant.ofEpochMilli(value.time)
        is java.sql.Timestamp -> longToZonedDateTime(value.time / 1000, value.nanos.toLong())
        is Int -> longToZonedDateTime(value.toLong())
        is Long -> longToZonedDateTime(value)
        is String -> value.toLongOrNull()?.let { valueFromDB(it) } ?: try {
            ZonedDateTime.parse(value, DATE_TIME_SPACE_SEPARATED_WITH_TIMEZONE_STRING_FORMATTER)
        } catch (e: IllegalArgumentException) {
            println("Error while parsing $value with db $currentDialect")
            throw e
        }
        else -> valueFromDB(value.toString())
    }

    override fun notNullValueToDB(value: Any): Any = when (value) {
        is ZonedDateTime -> java.sql.Timestamp(value.toInstant().toEpochMilli())
        else -> value
    }

    override val hasTimePart: Boolean
        get() = true

    private fun longToZonedDateTime(millis: Long) = ZonedDateTime. ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC)
    private fun longToZonedDateTime(seconds: Long, nanos: Long) = ZonedDateTime. ofInstant(Instant.ofEpochSecond(seconds, nanos), ZoneOffset.UTC)

    companion object {
        internal val INSTANCE = JavaTimeZonedDateTimeColumnType()
    }
}

/**
 * A datetimetz column to store both a date and a time with timezone.
 *
 * @param name The column name
 */
fun Table.datetimetz(name: String): Column<ZonedDateTime> = registerColumn(name, JavaTimeZonedDateTimeColumnType())

class CurrentZonedDateTime : Function<ZonedDateTime>(JavaTimeZonedDateTimeColumnType.INSTANCE) {
    override fun toQueryBuilder(queryBuilder: QueryBuilder) = queryBuilder {
        +when {
            else -> "(now() at time zone 'utc')"
        }
    }
}