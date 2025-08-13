// ===== src/main/kotlin/com/binkibonk/soldbcore/scheduling/TaskScheduler.kt =====
package com.binkibonk.soldbcore.scheduling

import com.binkibonk.soldbcore.DatabaseManager
import kotlinx.coroutines.*
import kotlin.coroutines.CoroutineContext
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.logging.Logger

/**
 * This class handles scheduled tasks for the database manager
 * like batch flushing, health checks, and connection pool maintenance
 */
class TaskScheduler(
    private val databaseManager: DatabaseManager,
    private val logger: Logger,
    private val config: org.bukkit.configuration.Configuration
) : CoroutineScope {
    override val coroutineContext: CoroutineContext = Dispatchers.IO + SupervisorJob()
    
    private var batchFlushTask: Job? = null
    private var healthCheckTask: Job? = null
    private var connectionMaintenanceTask: Job? = null
    
    // Configuration
    private val batchFlushPeriod: Long = config.getLong("tasks.batch-flush.period", 5L)
    private val healthCheckPeriod: Long = config.getLong("tasks.health-check.period", 30L)
    private val connectionMaintenancePeriod: Long = config.getLong("tasks.connection-maintenance.period", 300L)
    private val batchFlushEnabled: Boolean = config.getBoolean("tasks.batch-flush.enabled", true)
    private val healthCheckEnabled: Boolean = config.getBoolean("tasks.health-check.enabled", true)
    private val connectionMaintenanceEnabled: Boolean = config.getBoolean("tasks.connection-maintenance.enabled", true)
    
    fun startScheduledTasks() {
        logger.info("Starting scheduled database tasks")
        
        // Start batch flush task
        if (batchFlushEnabled) {
            startBatchFlushTask()
        } else {
            logger.info("Batch flush task is disabled")
        }
        
        // Start health check task
        if (healthCheckEnabled) {
            startHealthCheckTask()
        } else {
            logger.info("Health check task is disabled")
        }
        
        // Start connection maintenance task
        if (connectionMaintenanceEnabled) {
            startConnectionMaintenanceTask()
        } else {
            logger.info("Connection maintenance task is disabled")
        }
        
        logger.info("All scheduled tasks started successfully")
    }
    
    private fun startBatchFlushTask() {
        batchFlushTask = launch {
            while (isActive) {
                try {
                    delay(batchFlushPeriod * 1000)
                    // Batch flushing is handled by the DatabaseManager's scheduler
                    // This task is for additional monitoring and logging
                    logger.fine("Batch flush task executed")
                } catch (e: Exception) {
                    logger.warning("Error in batch flush task: ${e.message}")
                }
            }
        }
        logger.info("Batch flush task started (period: ${batchFlushPeriod}s)")
    }
    
    private fun startHealthCheckTask() {
        healthCheckTask = launch {
            while (isActive) {
                try {
                    delay(healthCheckPeriod * 1000)
                    performHealthCheck()
                } catch (e: Exception) {
                    logger.warning("Error in health check task: ${e.message}")
                }
            }
        }
        logger.info("Health check task started (period: ${healthCheckPeriod}s)")
    }
    
    private fun startConnectionMaintenanceTask() {
        connectionMaintenanceTask = launch {
            while (isActive) {
                try {
                    delay(connectionMaintenancePeriod * 1000)
                    performConnectionMaintenance()
                } catch (e: Exception) {
                    logger.warning("Error in connection maintenance task: ${e.message}")
                }
            }
        }
        logger.info("Connection maintenance task started (period: ${connectionMaintenancePeriod}s)")
    }
    
    private suspend fun performHealthCheck() {
        try {
            val isHealthy = databaseManager.isHealthy()
            if (!isHealthy) {
                logger.warning("Database health check failed - database may be experiencing issues")
            } else {
                logger.fine("Database health check passed")
            }
        } catch (e: Exception) {
            logger.warning("Health check failed with exception: ${e.message}")
        }
    }
    
    private suspend fun performConnectionMaintenance() {
        try {
            // Log connection pool statistics
            logger.fine("Connection maintenance task executed")
            
            // Additional maintenance tasks can be added here:
            // - Connection pool statistics logging
            // - Dead connection cleanup
            // - Performance metrics collection
            // - Query performance analysis
        } catch (e: Exception) {
            logger.warning("Connection maintenance failed: ${e.message}")
        }
    }
    
    fun stopBatchFlushTask() {
        batchFlushTask?.cancel()
        batchFlushTask = null
        logger.info("Batch flush task stopped")
    }
    
    fun stopHealthCheckTask() {
        healthCheckTask?.cancel()
        healthCheckTask = null
        logger.info("Health check task stopped")
    }
    
    fun stopConnectionMaintenanceTask() {
        connectionMaintenanceTask?.cancel()
        connectionMaintenanceTask = null
        logger.info("Connection maintenance task stopped")
    }
    
    fun shutdown() {
        logger.info("Shutting down task scheduler...")
        stopBatchFlushTask()
        stopHealthCheckTask()
        stopConnectionMaintenanceTask()
        cancel() // Cancel the coroutine scope
        logger.info("Task scheduler shutdown complete")
    }
}