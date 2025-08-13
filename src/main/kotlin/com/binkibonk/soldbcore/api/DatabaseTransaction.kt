// ===== src/main/kotlin/com/binkibonk/soldbcore/api/DatabaseTransaction.kt =====
package com.binkibonk.soldbcore.api

interface DatabaseTransaction {
    suspend fun insert(table: String, data: Map<String, Any>): Long
    suspend fun insertBatch(table: String, data: List<Map<String, Any>>): List<Long>
    suspend fun update(table: String, data: Map<String, Any>, where: String, params: List<Any> = emptyList()): Int
    suspend fun delete(table: String, where: String, params: List<Any> = emptyList()): Int
    suspend fun select(table: String, columns: List<String> = listOf("*"), where: String? = null, params: List<Any> = emptyList()): List<Map<String, Any>>
    suspend fun executeQuery(sql: String, params: List<Any> = emptyList()): List<Map<String, Any>>
    suspend fun executeUpdate(sql: String, params: List<Any> = emptyList()): Int
}