// ===== src/main/kotlin/com/binkibonk/soldbcore/DatabaseCorePlugin.kt =====
package com.binkibonk.soldbcore

import kotlinx.coroutines.*
import org.bukkit.plugin.ServicePriority
import org.bukkit.plugin.java.JavaPlugin
import kotlin.coroutines.CoroutineContext
import com.binkibonk.soldbcore.api.DatabaseAPI

class DatabaseCorePlugin : JavaPlugin(), CoroutineScope {
    private lateinit var job: Job
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + job

    private lateinit var dbManager: DatabaseManager

    override fun onEnable() {
        job = SupervisorJob()
        
        // Save default config
        saveDefaultConfig()
        
        // Initialize database manager (lazy initialization - connections deferred to first use)
        dbManager = DatabaseManager(this)
        
        // Register service immediately - connections will be established on first use
        server.servicesManager.register(
            DatabaseAPI::class.java,
            dbManager,
            this@DatabaseCorePlugin,
            ServicePriority.Normal
        )
        
        // Initialize database asynchronously in background without blocking onEnable
        launch {
            try {
                dbManager.initializeAsync().await()
                logger.info("DatabaseCore plugin initialized successfully!")
            } catch (throwable: Throwable) {
                logger.severe("Failed to initialize database: ${throwable.message}")
                throwable.printStackTrace()
                // Don't disable plugin - allow lazy initialization on first use
            }
        }
        
        logger.info("DatabaseCore plugin enabled! Database connections will be established on first use.")
    }

    override fun onDisable() {
        if (::dbManager.isInitialized) {
            dbManager.shutdown()
        }
        job.cancel()
        logger.info("DatabaseCore plugin disabled!")
    }
}