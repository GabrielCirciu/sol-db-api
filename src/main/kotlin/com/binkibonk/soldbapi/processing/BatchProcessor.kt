package com.binkibonk.soldbapi.processing

import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext
import java.sql.PreparedStatement
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Level
import java.util.logging.Logger

class BatchProcessor(
    private val dataSource: HikariDataSource,
    private val logger: Logger
) : CoroutineScope {
    override val coroutineContext: CoroutineContext = Dispatchers.IO + SupervisorJob()
    private val batchOperations = ConcurrentHashMap<String, MutableList<BatchOperation>>()
    private val maxBatchSize = 100
    
    data class BatchOperation(
        val sql: String,
        val params: List<Any>,
        val callback: (Result<Any>) -> Unit
    )
    
    suspend fun addBatch(sql: String, params: List<Any>, callback: (Result<Any>) -> Unit) {
        val operations = batchOperations.computeIfAbsent(sql) { mutableListOf() }
        synchronized(operations) {
            operations.add(BatchOperation(sql, params, callback))
            if (operations.size >= maxBatchSize) {
                launch { flush(sql) }
            }
        }
    }
    
    suspend fun flush(sql: String? = null) = withContext(Dispatchers.IO) {
        val sqlsToFlush = if (sql != null) listOf(sql) else batchOperations.keys.toList()
        
        sqlsToFlush.forEach { currentSql ->
            val operations = batchOperations[currentSql] ?: return@forEach
            val toProcess = synchronized(operations) {
                if (operations.isEmpty()) return@forEach
                val copy = operations.toList()
                operations.clear()
                copy
            }
            
            try {
                dataSource.connection.use { conn ->
                    conn.prepareStatement(currentSql).use { stmt ->
                        toProcess.forEach { op ->
                            try {
                                setParameters(stmt, op.params)
                                stmt.addBatch()
                            } catch (e: Exception) {
                                op.callback(Result.failure(e))
                            }
                        }
                        
                        val results = stmt.executeBatch()
                        results.forEachIndexed { index, result ->
                            if (index < toProcess.size) {
                                toProcess[index].callback(Result.success(result))
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.log(Level.SEVERE, "Batch execution failed for SQL: $currentSql", e)
                toProcess.forEach { op ->
                    op.callback(Result.failure(e))
                }
            }
        }
    }
    
    suspend fun shutdown() {
        flush()
    }
    
    private fun setParameters(stmt: PreparedStatement, params: List<Any>) {
        params.forEachIndexed { index, param ->
            stmt.setObject(index + 1, param)
        }
    }
}