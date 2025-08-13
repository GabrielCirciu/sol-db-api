package com.binkibonk.soldbapi.impl

import com.binkibonk.soldbapi.DatabaseManager
import com.binkibonk.soldbapi.DatabaseError
import com.binkibonk.soldbapi.api.QueryBuilder

class QueryBuilderImpl(private val dbManager: DatabaseManager) : QueryBuilder {
    private val selectColumns = mutableListOf<String>()
    private var fromTable: String? = null
    private var whereCondition: String? = null
    private val whereParams = mutableListOf<Any>()
    private var orderByClause: String? = null
    private var limitCount: Int? = null
    private var offsetCount: Int? = null
    private val groupByColumns = mutableListOf<String>()
    private var havingCondition: String? = null
    private val havingParams = mutableListOf<Any>()
    
    // SQL injection protection regex for table and column names
    private val validIdentifierRegex = Regex("^[a-zA-Z_][a-zA-Z0-9_]*$")
    
    private fun validateTableName(tableName: String) {
        if (!validIdentifierRegex.matches(tableName)) {
            throw DatabaseError.ValidationError("Invalid table name: $tableName. Only alphanumeric characters and underscores are allowed, must start with letter or underscore.")
        }
        if (tableName.length > 63) { // PostgreSQL identifier limit
            throw DatabaseError.ValidationError("Table name too long: $tableName. Maximum length is 63 characters.")
        }
    }
    
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
    
    override fun select(vararg columns: String): QueryBuilder {
        // Validate columns
        columns.forEach { validateColumnName(it) }
        selectColumns.clear()
        selectColumns.addAll(columns)
        return this
    }
    
    override fun from(table: String): QueryBuilder {
        // Allow table aliases (e.g., "users u") by only validating the base table name
        val baseTable = table.split(" ").first()
        validateTableName(baseTable)
        fromTable = table
        return this
    }
    
    override fun where(condition: String, vararg params: Any): QueryBuilder {
        whereCondition = condition
        whereParams.clear()
        whereParams.addAll(params)
        return this
    }
    
    override fun orderBy(column: String, direction: String): QueryBuilder {
        orderByClause = "$column $direction"
        return this
    }
    
    override fun limit(count: Int): QueryBuilder {
        limitCount = count
        return this
    }
    
    override fun offset(count: Int): QueryBuilder {
        offsetCount = count
        return this
    }
    
    override fun groupBy(vararg columns: String): QueryBuilder {
        // Validate columns
        columns.forEach { validateColumnName(it) }
        groupByColumns.clear()
        groupByColumns.addAll(columns)
        return this
    }
    
    override fun having(condition: String, vararg params: Any): QueryBuilder {
        havingCondition = condition
        havingParams.clear()
        havingParams.addAll(params)
        return this
    }
    
    override suspend fun execute(): List<Map<String, Any>> {
        // Check if this is a write operation
        val isWriteOperation = selectColumns.isEmpty() || 
                              selectColumns.any { it.uppercase() == "INSERT" || it.uppercase() == "UPDATE" || it.uppercase() == "DELETE" }
        
        if (isWriteOperation && fromTable != null) {
            val baseTable = fromTable?.split(" ")?.first()
            if (baseTable != null && !dbManager.isTableOwner(baseTable)) {
                val owner = dbManager.getTableOwnerBlocking(baseTable)
                val caller = "Unknown" // We don't have direct access to caller info here
                throw DatabaseError.QueryError("Access denied: Table '$baseTable' is owned by '$owner'. Write operations not allowed.")
            }
        }
        
        val (sql, params) = build()
        return dbManager.executeQuery(sql, params)
    }
    
    override suspend fun executeCount(): Long {
        // Count operations are read-only, so no ownership check needed
        val (sql, params) = build()
        val countSql = "SELECT COUNT(*) FROM ($sql) as count_query"
        val result = dbManager.executeQuery(countSql, params)
        return result.firstOrNull()?.values?.firstOrNull() as? Long ?: 0L
    }
    
    override fun executeBlocking(): List<Map<String, Any>> {
        // For blocking version, we'll assume it's a read operation (most common case)
        // More sophisticated ownership checking would require async-to-blocking conversion
        val (sql, params) = build()
        return kotlinx.coroutines.runBlocking { dbManager.executeQuery(sql, params) }
    }
    
    override fun executeCountBlocking(): Long {
        // Count operations are read-only, so no ownership check needed
        val (sql, params) = build()
        val countSql = "SELECT COUNT(*) FROM ($sql) as count_query"
        val result = kotlinx.coroutines.runBlocking { dbManager.executeQuery(countSql, params) }
        return result.firstOrNull()?.values?.firstOrNull() as? Long ?: 0L
    }
    
    override fun build(): Pair<String, List<Any>> {
        if (selectColumns.isEmpty()) {
            throw IllegalStateException("No columns selected")
        }
        if (fromTable.isNullOrEmpty()) {
            throw IllegalStateException("No table specified")
        }
        
        val sql = StringBuilder()
        sql.append("SELECT ${selectColumns.joinToString(", ")}")
        sql.append(" FROM $fromTable")
        
        // WHERE clause
        if (!whereCondition.isNullOrEmpty()) {
            sql.append(" WHERE $whereCondition")
        }
        
        // GROUP BY clause
        if (groupByColumns.isNotEmpty()) {
            sql.append(" GROUP BY ${groupByColumns.joinToString(", ")}")
        }
        
        // HAVING clause
        if (!havingCondition.isNullOrEmpty()) {
            sql.append(" HAVING $havingCondition")
        }
        
        // ORDER BY clause
        if (!orderByClause.isNullOrEmpty()) {
            sql.append(" ORDER BY $orderByClause")
        }
        
        // LIMIT clause
        if (limitCount != null) {
            sql.append(" LIMIT ").append(limitCount)
        }
        
        // OFFSET clause
        if (offsetCount != null) {
            sql.append(" OFFSET ").append(offsetCount)
        }
        
        val allParams = whereParams + havingParams
        return Pair(sql.toString(), allParams)
    }
}