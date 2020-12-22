package com.example

import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.UUIDTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.`java-time`.CurrentDateTime
import org.jetbrains.exposed.sql.`java-time`.JavaLocalDateColumnType
import org.jetbrains.exposed.sql.`java-time`.JavaLocalDateTimeColumnType
import org.jetbrains.exposed.sql.`java-time`.datetime
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.*

object UserTable : UUIDTable("user") {
    val name = varchar("name", 255)
    val createdAt = datetime("created_at").defaultExpression(CurrentDateTime())
}

enum class Operation {
    EQ,
    CONTAINS,
    LE,
    LT,
    GT,
    GE,
    IN
}

data class FilterInput(val field: String, val value: String, val op: Operation)

fun resolveLocalDate(query: Query, col: Column<LocalDate>, value: String, op: Operation) {
    val parsed = LocalDate.parse(value)

    when(op) {
        Operation.EQ -> query.andWhere { col eq parsed }
        Operation.LT -> query.andWhere { col less parsed }
        Operation.LE -> query.andWhere { col lessEq parsed }
        Operation.GT -> query.andWhere { col greater parsed }
        Operation.GE -> query.andWhere { col greaterEq parsed }
        else -> {}
    }
}

fun resolveLocalDateTime(query: Query, col: Column<LocalDateTime>, value: String, op: Operation) {
    val parsed = LocalDateTime.parse(value)

    when(op) {
        Operation.EQ -> query.andWhere { col eq parsed }
        Operation.LT -> query.andWhere { col less parsed }
        Operation.LE -> query.andWhere { col lessEq parsed }
        Operation.GT -> query.andWhere { col greater parsed }
        Operation.GE -> query.andWhere { col greaterEq parsed }
        else -> {}
    }
}

fun uuidResolver(query: Query, col: Column<EntityID<UUID>>, value: List<UUID>, op: Operation) {
    when(op) {
        Operation.EQ -> query.andWhere { col eq value[0] }
        Operation.LT -> query.andWhere { col less value[0] }
        Operation.LE -> query.andWhere { col lessEq value[0] }
        Operation.GT -> query.andWhere { col greater value[0] }
        Operation.GE -> query.andWhere { col greaterEq value[0] }
        Operation.IN -> query.andWhere { col inList value }
        Operation.CONTAINS -> query.andWhere { col.castTo<String>(col.columnType) like value.toString() }
    }
}

fun <V: Comparable<V>> genericResolve(query: Query, col: Column<V>, value: List<V>, op: Operation) {
    when(op) {
        Operation.EQ -> query.andWhere { col eq value[0] }
        Operation.LT -> query.andWhere { col less value[0] }
        Operation.LE -> query.andWhere { col lessEq value[0] }
        Operation.GT -> query.andWhere { col greater value[0] }
        Operation.GE -> query.andWhere { col greaterEq value[0] }
        Operation.IN -> query.andWhere { col inList value }
        Operation.CONTAINS -> {
            when (val extracted = value[0]) {
                is Int, is Long, is Float, is Double, is LocalDate, is LocalDateTime -> {
                    val casted = col.castTo<String>(VarCharColumnType())
                    query.andWhere { casted like "%${extracted}%" }
                }
                is String -> query.andWhere { (col as Column<String>) like "%${extracted}%" }
            }
        }
    }
}


fun resolveFilters(query: Query, filters: List<FilterInput>) {
    val columnLookup = UserTable.columns.map { it.name to it }.toMap()

    for (it in filters) {
        val col = columnLookup[it.field] ?: continue
        if (it.value.isBlank()) continue

        when(col.columnType) {
            is IntegerColumnType -> {
                val values = it.value.split(",").map { it.toInt() }
                genericResolve(query, col  as Column<Int>, values, it.op)
            }
            is LongColumnType -> {
                val values = it.value.split(",").map { it.toLong() }
                genericResolve(query, col  as Column<Long>, values, it.op)
            }
            is FloatColumnType -> {
                val values = it.value.split(",").map { it.toFloat() }
                genericResolve(query, col  as Column<Float>, values, it.op)
            }
            is DoubleColumnType -> {
                val values = it.value.split(",").map { it.toDouble() }
                genericResolve(query, col  as Column<Double>, values, it.op)
            }
            is VarCharColumnType -> {
                val values = it.value.split(",")
                genericResolve(query, col as Column<String>, values, it.op)
            }
            is EntityIDColumnType<*> -> {
                if (it.op == Operation.CONTAINS) {
                    val casted = col.castTo<String>(VarCharColumnType())
                    query.andWhere { casted like "%${it.value}%" }

                    continue
                }

                // Assume to always do UUID
                val values = it.value.split(",").map { UUID.fromString(it) }
                uuidResolver(query, col as Column<EntityID<UUID>>, values, it.op)
            }
            is UUIDColumnType -> {
                if (it.op == Operation.CONTAINS) {
                    val casted = col.castTo<String>(VarCharColumnType())
                    query.andWhere { casted like "%${it.value}%" }

                    continue
                }

                val values = it.value.split(",").map { UUID.fromString(it) }
                uuidResolver(query, col as Column<EntityID<UUID>>, values, it.op)
            }
            is JavaLocalDateColumnType -> {
                if (it.op == Operation.CONTAINS) {
                    val casted = col.castTo<String>(VarCharColumnType())
                    query.andWhere { casted like "%${it.value}%" }

                    continue
                }

                val values = it.value.split(",").map { LocalDate.parse(it) }
                genericResolve(
                  query, col as Column<LocalDate>, values, it.op
                )
            }
            is JavaLocalDateTimeColumnType -> {
                if (it.op == Operation.CONTAINS) {
                    val casted = col.castTo<String>(VarCharColumnType())
                    query.andWhere { casted like "%${it.value}%" }

                    continue
                }

                val values = it.value
                  .split(",")
                  .map { LocalDateTime.parse(it) }

                genericResolve(
                  query, col as Column<LocalDateTime>, values, it.op
                )
            }
        }
    }
}

fun main() {
    initDb()
    transaction {
        val query = UserTable.selectAll()
        resolveFilters(query, listOf(FilterInput("created_at", "43", Operation.CONTAINS)))
        query.forEach { println(UserDto.fromRow(it)) }

    }
}