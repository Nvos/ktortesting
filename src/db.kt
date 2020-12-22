package com.example

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.selects.select
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.*
import java.util.stream.IntStream
import kotlin.streams.toList



fun initDb() {
    Database.connect(hikari())
    transaction {
        SchemaUtils.drop(UserTable)
        SchemaUtils.create(UserTable)
    }
//
    transaction {
        var index = 0L
        UserTable.batchInsert(IntStream.range(0, 100).toList().map { "User${it}" }) {
            this[UserTable.name] = it
            this[UserTable.createdAt] = LocalDateTime.now(ZoneId.of("UTC")).plusMinutes(index)
            index++
        }
    }

    transaction {
        val row = UserTable.selectAll().orderBy(UserTable.createdAt).first()

        println("Cursor = ${row[UserTable.id]} ${row[UserTable.createdAt]}")

        val next = UserTable.select {
            UserTable.createdAt greaterEq row[UserTable.createdAt] and (
                (UserTable.id greater row[UserTable.id]) or (UserTable.createdAt greater row[UserTable.createdAt])
            )
        }.limit(20)
            .orderBy(UserTable.createdAt, SortOrder.ASC)
            .orderBy(UserTable.id, SortOrder.ASC)
            .last()

        UserTable.select {
            UserTable.createdAt greaterEq next[UserTable.createdAt] and (
                    (UserTable.id greater next[UserTable.id]) or (UserTable.createdAt greater next[UserTable.createdAt])
                    )
        }.limit(20)
            .orderBy(UserTable.createdAt, SortOrder.ASC)
            .orderBy(UserTable.id, SortOrder.ASC)
            .forEach {
                println("${it[UserTable.name]} to ${it[UserTable.id].value}")
            }
    }
}

private fun hikari(): HikariDataSource {
    val config = HikariConfig()
    config.driverClassName = "org.postgresql.Driver"
    config.jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
    config.maximumPoolSize = 3
    config.isAutoCommit = false
    config.username ="postgres"
    config.password ="postgres"
    config.transactionIsolation = "TRANSACTION_REPEATABLE_READ"
    config.validate()
    return HikariDataSource(config)
}