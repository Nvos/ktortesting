package com.example

import org.jetbrains.exposed.dao.id.UUIDTable
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.`java-time`.CurrentDateTime
import org.jetbrains.exposed.sql.`java-time`.datetime
import java.time.ZoneId
import java.time.ZonedDateTime

object UserTable : UUIDTable("user") {
    val name = varchar("name", 255)

    val createdAt = datetimetz("created_at")
      .defaultExpression(CurrentZonedDateTime())
}