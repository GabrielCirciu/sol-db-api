// ===== src/main/kotlin/com/binkibonk/soldbcore/impl/DatabaseTransactionImpl.kt =====
package com.binkibonk.soldbcore.impl

import com.binkibonk.soldbcore.api.DatabaseTransaction
import com.binkibonk.soldbcore.DatabaseError
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Statement

class DatabaseTransactionImpl(private val connection: Connection) : DatabaseTransaction {
    
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
        // After validation, we can safely quote the identifier
        return "\"$identifier\""
    }
    
    override suspend fun insert(table: String, data: Map<String, Any>): Long = withContext(Dispatchers.IO) {
        validateTableName(table)
        data.keys.forEach { validateColumnName(it) }
        
        val quotedTableName = quoteIdentifier(table)
        val quotedColumns = data.keys.joinToString(", ") { quoteIdentifier(it) }
        val placeholders = data.keys.joinToString(", ") { "?" }
        val sql = "INSERT INTO $quotedTableName ($quotedColumns) VALUES ($placeholders)"
        
        connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS).use { stmt ->
            setParameters(stmt, data.values.toList())
            stmt.executeUpdate()
            
            stmt.generatedKeys.use { rs ->
                if (rs.next()) rs.getLong(1) else -1L
            }
        }
    }
    
    override suspend fun insertBatch(table: String, data: List<Map<String, Any>>): List<Long> = withContext(Dispatchers.IO) {
        if (data.isEmpty()) return@withContext emptyList()
        
        validateTableName(table)
        data.first().keys.forEach { validateColumnName(it) }
        
        val quotedTableName = quoteIdentifier(table)
        val quotedColumns = data.first().keys.joinToString(", ") { quoteIdentifier(it) }
        val placeholders = data.first().keys.joinToString(", ") { "?" }
        val sql = "INSERT INTO $quotedTableName ($quotedColumns) VALUES ($placeholders)"
        
        connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS).use { stmt ->
            data.forEach { row ->
                setParameters(stmt, row.values.toList())
                stmt.addBatch()
            }
            
            stmt.executeBatch()
            
            val ids = mutableListOf<Long>()
            stmt.generatedKeys.use { rs ->
                while (rs.next()) {
                    ids.add(rs.getLong(1))
                }
            }
            ids
        }
    }
    
    override suspend fun update(table: String, data: Map<String, Any>, where: String, params: List<Any>): Int = withContext(Dispatchers.IO) {
        validateTableName(table)
        data.keys.forEach { validateColumnName(it) }
        
        val quotedTableName = quoteIdentifier(table)
        val setClause = data.keys.joinToString(", ") { "${quoteIdentifier(it)} = ?" }
        val sql = "UPDATE $quotedTableName SET $setClause" + if (where.isNotEmpty()) " WHERE $where" else ""
        
        connection.prepareStatement(sql).use { stmt ->
            val allParams = data.values.toList() + params
            setParameters(stmt, allParams)
            stmt.executeUpdate()
        }
    }
    
    override suspend fun delete(table: String, where: String, params: List<Any>): Int = withContext(Dispatchers.IO) {
        validateTableName(table)
        
        val quotedTableName = quoteIdentifier(table)
        val sql = "DELETE FROM $quotedTableName" + if (where.isNotEmpty()) " WHERE $where" else ""
        
        connection.prepareStatement(sql).use { stmt ->
            setParameters(stmt, params)
            stmt.executeUpdate()
        }
    }
    
    override suspend fun select(table: String, columns: List<String>, where: String?, params: List<Any>): List<Map<String, Any>> = withContext(Dispatchers.IO) {
        validateTableName(table)
        // Validate column names if not using wildcard
        if (!columns.contains("*")) {
            columns.forEach { validateColumnName(it) }
        }
        
        val quotedTableName = quoteIdentifier(table)
        val columnList = if (columns.contains("*")) "*" else columns.joinToString(", ") { quoteIdentifier(it) }
        val sql = "SELECT $columnList FROM $quotedTableName" + if (!where.isNullOrEmpty()) " WHERE $where" else ""
        
        executeQuery(sql, params)
    }
    
    override suspend fun executeQuery(sql: String, params: List<Any>): List<Map<String, Any>> = withContext(Dispatchers.IO) {
        connection.prepareStatement(sql).use { stmt ->
            setParameters(stmt, params)
            stmt.executeQuery().use { rs ->
                val results = mutableListOf<Map<String, Any>>()
                val meta = rs.metaData
                val columnCount = meta.columnCount
                
                while (rs.next()) {
                    val row = mutableMapOf<String, Any>()
                    for (i in 1..columnCount) {
                        val columnName = meta.getColumnName(i)
                        val value = rs.getObject(i)
                        if (value != null) {
                            row[columnName] = value
                        }
                    }
                    results.add(row)
                }
                results
            }
        }
    }
    
    override suspend fun executeUpdate(sql: String, params: List<Any>): Int = withContext(Dispatchers.IO) {
        connection.prepareStatement(sql).use { stmt ->
            setParameters(stmt, params)
            stmt.executeUpdate()
        }
    }
    
    private fun setParameters(stmt: PreparedStatement, params: List<Any>) {
        params.forEachIndexed { index, param ->
            stmt.setObject(index + 1, param)
        }
    }
}