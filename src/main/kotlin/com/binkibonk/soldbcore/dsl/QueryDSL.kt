// ===== src/main/kotlin/com/binkibonk/soldbcore/dsl/QueryDSL.kt =====
package com.binkibonk.soldbcore.dsl

import com.binkibonk.soldbcore.api.QueryBuilder
import com.binkibonk.soldbcore.DatabaseError

/**
 * Simple Query DSL for more readable and type-safe query construction
 */

// SQL injection protection regex for column names
private val validIdentifierRegex = Regex("^[a-zA-Z_][a-zA-Z0-9_]*$")

private fun validateColumnName(columnName: String) {
    // Skip validation for wildcards and expressions with dots, spaces, or functions
    if (columnName == "*" || columnName.contains(".") || columnName.contains(" ") || columnName.contains("(")) {
        return // Allow complex expressions like "u.username", "COUNT(*)", etc.
    }
    if (!validIdentifierRegex.matches(columnName)) {
        throw DatabaseError.ValidationError("Invalid column name: $columnName. Only alphanumeric characters and underscores are allowed, must start with letter or underscore.")
    }
    if (columnName.length > 63) { // PostgreSQL identifier limit
        throw DatabaseError.ValidationError("Column name too long: $columnName. Maximum length is 63 characters.")
    }
}

/**
 * Safely quote SQL identifiers to prevent injection while allowing validated names
 */
private fun quoteIdentifier(identifier: String): String {
    // Don't quote wildcards, expressions, or already complex identifiers
    if (identifier == "*" || identifier.contains(".") || identifier.contains(" ") || identifier.contains("(")) {
        return identifier // Return as-is for complex expressions
    }
    return "\"$identifier\""
}

// Extension functions for more readable query building
fun QueryBuilder.whereEquals(column: String, value: Any): QueryBuilder {
    validateColumnName(column)
    return this.where("${quoteIdentifier(column)} = ?", value)
}

fun QueryBuilder.whereIn(column: String, values: List<Any>): QueryBuilder {
    validateColumnName(column)
    val placeholders = values.joinToString(", ") { "?" }
    return this.where("${quoteIdentifier(column)} IN ($placeholders)", *values.toTypedArray())
}

fun QueryBuilder.whereLike(column: String, pattern: String): QueryBuilder {
    validateColumnName(column)
    return this.where("${quoteIdentifier(column)} LIKE ?", pattern)
}

fun QueryBuilder.whereGreaterThan(column: String, value: Any): QueryBuilder {
    validateColumnName(column)
    return this.where("${quoteIdentifier(column)} > ?", value)
}

fun QueryBuilder.whereLessThan(column: String, value: Any): QueryBuilder {
    validateColumnName(column)
    return this.where("${quoteIdentifier(column)} < ?", value)
}

fun QueryBuilder.whereBetween(column: String, start: Any, end: Any): QueryBuilder {
    validateColumnName(column)
    return this.where("${quoteIdentifier(column)} BETWEEN ? AND ?", start, end)
}

fun QueryBuilder.whereIsNull(column: String): QueryBuilder {
    validateColumnName(column)
    return this.where("${quoteIdentifier(column)} IS NULL")
}

fun QueryBuilder.whereIsNotNull(column: String): QueryBuilder {
    validateColumnName(column)
    return this.where("${quoteIdentifier(column)} IS NOT NULL")
}

// Ordering helpers
fun QueryBuilder.orderByAsc(column: String): QueryBuilder {
    validateColumnName(column)
    return this.orderBy(quoteIdentifier(column), "ASC")
}

fun QueryBuilder.orderByDesc(column: String): QueryBuilder {
    validateColumnName(column)
    return this.orderBy(quoteIdentifier(column), "DESC")
}

// Pagination helpers
fun QueryBuilder.paginate(page: Int, pageSize: Int): QueryBuilder {
    return this.limit(pageSize).offset((page - 1) * pageSize)
}

/**
 * DSL builder for complex queries
 */
class QueryDSLBuilder(private val queryBuilder: QueryBuilder) {
    
    fun where(block: WhereBuilder.() -> Unit): QueryDSLBuilder {
        val whereBuilder = WhereBuilder()
        whereBuilder.block()
        queryBuilder.where(whereBuilder.condition, *whereBuilder.params.toTypedArray())
        return this
    }
    
    fun orderBy(column: String, direction: String = "ASC"): QueryDSLBuilder {
        queryBuilder.orderBy(column, direction)
        return this
    }
    
    fun limit(count: Int): QueryDSLBuilder {
        queryBuilder.limit(count)
        return this
    }
    
    fun offset(count: Int): QueryDSLBuilder {
        queryBuilder.offset(count)
        return this
    }
    
    suspend fun execute(): List<Map<String, Any>> {
        return queryBuilder.execute()
    }
    
    suspend fun executeCount(): Long {
        return queryBuilder.executeCount()
    }
}

class WhereBuilder {
    var condition: String = ""
    val params = mutableListOf<Any>()
    
    infix fun String.eq(value: Any) {
        validateColumnName(this)
        val quotedColumn = quoteIdentifier(this)
        condition = "$quotedColumn = ?"
        params.add(value)
    }
    
    infix fun String.neq(value: Any) {
        validateColumnName(this)
        val quotedColumn = quoteIdentifier(this)
        condition = "$quotedColumn != ?"
        params.add(value)
    }
    
    infix fun String.gt(value: Any) {
        validateColumnName(this)
        val quotedColumn = quoteIdentifier(this)
        condition = "$quotedColumn > ?"
        params.add(value)
    }
    
    infix fun String.lt(value: Any) {
        validateColumnName(this)
        val quotedColumn = quoteIdentifier(this)
        condition = "$quotedColumn < ?"
        params.add(value)
    }
    
    infix fun String.like(pattern: String) {
        validateColumnName(this)
        val quotedColumn = quoteIdentifier(this)
        condition = "$quotedColumn LIKE ?"
        params.add(pattern)
    }
    
    infix fun String.isIn(values: List<Any>) {
        validateColumnName(this)
        val quotedColumn = quoteIdentifier(this)
        val placeholders = values.joinToString(", ") { "?" }
        condition = "$quotedColumn IN ($placeholders)"
        params.addAll(values)
    }
    
    fun String.isNull() {
        validateColumnName(this)
        val quotedColumn = quoteIdentifier(this)
        condition = "$quotedColumn IS NULL"
    }
    
    fun String.isNotNull() {
        validateColumnName(this)
        val quotedColumn = quoteIdentifier(this)
        condition = "$quotedColumn IS NOT NULL"
    }
}

// Extension function to create DSL builder
fun QueryBuilder.dsl(): QueryDSLBuilder {
    return QueryDSLBuilder(this)
} 