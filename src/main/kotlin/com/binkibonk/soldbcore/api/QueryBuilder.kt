// ===== src/main/kotlin/com/binkibonk/soldbcore/api/QueryBuilder.kt =====
package com.binkibonk.soldbcore.api

interface QueryBuilder {
    fun select(vararg columns: String): QueryBuilder
    fun from(table: String): QueryBuilder
    fun where(condition: String, vararg params: Any): QueryBuilder
    fun orderBy(column: String, direction: String = "ASC"): QueryBuilder
    fun limit(count: Int): QueryBuilder
    fun offset(count: Int): QueryBuilder
    fun groupBy(vararg columns: String): QueryBuilder
    fun having(condition: String, vararg params: Any): QueryBuilder
    suspend fun execute(): List<Map<String, Any>>
    suspend fun executeCount(): Long
    fun executeBlocking(): List<Map<String, Any>>
    fun executeCountBlocking(): Long
    fun build(): Pair<String, List<Any>> // Returns SQL and parameters
}