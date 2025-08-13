package com.binkibonk.soldbcore.api

import com.binkibonk.soldbcore.data.TableInfo
import kotlinx.coroutines.flow.Flow

/**
 * Main API interface for database operations.
 * This interface is registered as a service that other plugins can use.
 */
interface DatabaseAPI {
    /**
     * Create a new table with the given schema
     * Only the creating plugin can modify this table
     */
    suspend fun createTable(name: String, schema: String): Boolean
    
    /**
     * Create a new table with the given schema (blocking version)
     * Only the creating plugin can modify this table
     */
    fun createTableBlocking(name: String, schema: String): Boolean
    
    /**
     * Drop a table if it exists
     * Only the owning plugin can drop the table
     */
    suspend fun dropTable(name: String): Boolean
    
    /**
     * Drop a table if it exists (blocking version)
     * Only the owning plugin can drop the table
     */
    fun dropTableBlocking(name: String): Boolean
    
    /**
     * Check if a table exists
     * Any plugin can use it
     */
    suspend fun tableExists(name: String): Boolean
    
    /**
     * Check if a table exists (blocking version)
     * Any plugin can use it
     */
    fun tableExistsBlocking(name: String): Boolean
    
    /**
     * Get information about a table's structure
     * Any plugin can use it
     */
    suspend fun getTableInfo(name: String): TableInfo?
    
    /**
     * Get information about a table's structure (blocking version)
     * Any plugin can use it
     */
    fun getTableInfoBlocking(name: String): TableInfo?
    
    /**
     * Insert a single record and return the generated ID
     * Only the owning plugin can insert into the table
     */
    suspend fun insert(table: String, data: Map<String, Any>): Long
    
    /**
     * Insert a single record and return the generated ID (blocking version)
     * Only the owning plugin can insert into the table
     */
    fun insertBlocking(table: String, data: Map<String, Any>): Long
    
    /**
     * Insert multiple records and return the generated IDs
     * Only the owning plugin can insert into the table
     */
    suspend fun insertBatch(table: String, data: List<Map<String, Any>>): List<Long>
    
    /**
     * Insert multiple records and return the generated IDs (blocking version)
     * Only the owning plugin can insert into the table
     */
    fun insertBatchBlocking(table: String, data: List<Map<String, Any>>): List<Long>
    
    /**
     * Update records matching the where clause
     * Only the owning plugin can update the table
     */
    suspend fun update(table: String, data: Map<String, Any>, where: String, params: List<Any>): Int
    
    /**
     * Update records matching the where clause (blocking version)
     * Only the owning plugin can update the table
     */
    fun updateBlocking(table: String, data: Map<String, Any>, where: String, params: List<Any>): Int
    
    /**
     * Delete records matching the where clause
     * Only the owning plugin can delete from the table
     */
    suspend fun delete(table: String, where: String, params: List<Any>): Int
    
    /**
     * Delete records matching the where clause (blocking version)
     * Only the owning plugin can delete from the table
     */
    fun deleteBlocking(table: String, where: String, params: List<Any>): Int
    
    /**
     * Select records from a table
     * Any plugin can read from any table
     */
    suspend fun select(table: String, columns: List<String>, where: String?, params: List<Any>): List<Map<String, Any>>
    
    /**
     * Select records from a table (blocking version)
     * Any plugin can read from any table
     */
    fun selectBlocking(table: String, columns: List<String>, where: String?, params: List<Any>): List<Map<String, Any>>
    
    /**
     * Execute a raw SQL query
     * Any plugin can execute read-only queries
     */
    suspend fun executeQuery(sql: String, params: List<Any>): List<Map<String, Any>>
    
    /**
     * Execute a raw SQL query (blocking version)
     * Any plugin can execute read-only queries
     */
    fun executeQueryBlocking(sql: String, params: List<Any>): List<Map<String, Any>>
    
    /**
     * Execute a raw SQL update/insert/delete
     * Only the owning plugin can execute write operations
     */
    suspend fun executeUpdate(sql: String, params: List<Any>): Int
    
    /**
     * Execute a raw SQL update/insert/delete (blocking version)
     * Only the owning plugin can execute write operations
     */
    fun executeUpdateBlocking(sql: String, params: List<Any>): Int
    
    /**
     * Execute operations within a transaction
     * Only the owning plugin can execute transactions
     */
    suspend fun <T> withTransaction(block: suspend (DatabaseTransaction) -> T): T
    
    /**
     * Get a query builder for constructing complex queries
     * Query builder operations follow the same ownership rules
     */
    fun queryBuilder(): QueryBuilder
    
    /**
     * Check if the database connection is healthy
     */
    suspend fun isHealthy(): Boolean
    
    /**
     * Check if the database connection is healthy (blocking version)
     */
    fun isHealthyBlocking(): Boolean
    
    /**
     * Get the owner plugin name for a table
     */
    suspend fun getTableOwner(tableName: String): String?
    
    /**
     * Get the owner plugin name for a table (blocking version)
     */
    fun getTableOwnerBlocking(tableName: String): String?
    
    /**
     * Check if the current calling plugin owns the table
     */
    suspend fun isTableOwner(tableName: String): Boolean
    
    /**
     * Check if the current calling plugin owns the table (blocking version)
     */
    fun isTableOwnerBlocking(tableName: String): Boolean
}