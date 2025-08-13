package com.binkibonk.soldbapi

import com.binkibonk.soldbapi.api.DatabaseAPI
import com.binkibonk.soldbapi.api.DatabaseTransaction
import com.binkibonk.soldbapi.api.QueryBuilder
import com.binkibonk.soldbapi.data.ColumnInfo
import com.binkibonk.soldbapi.data.TableInfo
import com.binkibonk.soldbapi.impl.DatabaseTransactionImpl
import com.binkibonk.soldbapi.impl.QueryBuilderImpl
import com.binkibonk.soldbapi.processing.BatchProcessor
import com.binkibonk.soldbapi.scheduling.TaskScheduler
import com.binkibonk.soldbapi.migration.MigrationManager
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.*
import java.sql.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import kotlin.coroutines.CoroutineContext
import org.bukkit.plugin.Plugin


sealed class DatabaseError : Exception() {
    class ConnectionError(message: String, cause: Throwable? = null) : DatabaseError()
    class QueryError(message: String, cause: Throwable? = null) : DatabaseError()
    class TransactionError(message: String, cause: Throwable? = null) : DatabaseError()
    class InitializationError(message: String, cause: Throwable? = null) : DatabaseError()
    class ValidationError(message: String, cause: Throwable? = null) : DatabaseError()
}

class DatabaseManager(
    private val plugin: DatabaseCorePlugin
) : DatabaseAPI, CoroutineScope {
    override val coroutineContext: CoroutineContext = Dispatchers.IO + SupervisorJob()
    
    private lateinit var dataSource: HikariDataSource
    private lateinit var batchProcessor: BatchProcessor
    private lateinit var sharedExecutor: ScheduledExecutorService
    private lateinit var taskScheduler: TaskScheduler
    private lateinit var migrationManager: MigrationManager
    
    // Table ownership tracking
    private val tableOwners = mutableMapOf<String, String>() // tableName -> pluginName
    private val ownershipLock = Any()
    
    // Database configuration
    private val dbHost: String = plugin.config.getString("database.host") ?: "localhost"
    private val dbPort: Int = plugin.config.getInt("database.port", 5432)
    private val dbName: String = plugin.config.getString("database.database") ?: "sol_database"
    private val dbUsername: String = plugin.config.getString("database.username") ?: "sol_username"
    private val dbPassword: String = plugin.config.getString("database.password") ?: "sol_password"
    
    // SQL injection protection regex for table and column names
    private val validIdentifierRegex = Regex("^[a-zA-Z_][a-zA-Z0-9_]*$")
    
    // Connection health monitoring
    private var lastConnectionTest = 0L
    private val healthCheckInterval = 30_000L // 30 seconds
    private var isConnected = false
    
    fun initialize(): CompletableFuture<Void> {
        // Create shared executor once for all tasks
        sharedExecutor = createOptimizedExecutor()
        
        return CompletableFuture.runAsync({
            try {
                // Try to register the PostgreSQL driver manually
                try {
                    Class.forName("com.binkibonk.soldbapi.libs.postgresql.Driver")
                } catch (e: ClassNotFoundException) {
                    plugin.logger.warning("Could not find relocated PostgreSQL driver, trying auto-detection...")
                }
                
                // Check and create database if it doesn't exist
                ensureDatabaseExists()
                
                // Setup HikariCP
                setupConnectionPool()
                
                // Test connection
                testConnection()
                
                // Initialize batch processor
                batchProcessor = BatchProcessor(dataSource, plugin.logger)
                
                // Initialize migration manager
                migrationManager = MigrationManager(dataSource, plugin.logger)
                runBlocking { 
                    migrationManager.initialize()
                    migrationManager.migrate()
                }
                
                // Setup scheduled tasks with shared executor
                setupScheduledTasks()
                
                // Initialize task scheduler
                taskScheduler = TaskScheduler(this, plugin.logger, plugin.config)
                taskScheduler.startScheduledTasks()
                
                plugin.logger.info("Database connection established successfully")
            } catch (e: Exception) {
                plugin.logger.severe("Failed to initialize database manager: ${e.message}")
                throw DatabaseError.InitializationError("Database initialization failed", e)
            }
        }, sharedExecutor)
    }
    
    /**
     * Async version that returns a Deferred for coroutine-based initialization
     */
    fun initializeAsync(): Deferred<Unit> {
        return async {
            initialize().get() // Convert CompletableFuture to Deferred
        }
    }
    
    private fun createOptimizedExecutor(): ScheduledExecutorService {
        val threadCount = if (Runtime.getRuntime().availableProcessors() * 2 > 2) {
            Runtime.getRuntime().availableProcessors() * 2
        } else {
            2
        }
        return Executors.newScheduledThreadPool(threadCount)
    }
    
    private fun ensureDatabaseExists() {
        try {
            // First, try to connect to the target database
            val testUrl = "jdbc:postgresql://$dbHost:$dbPort/$dbName"
            plugin.logger.info("Checking database connection at: $testUrl")
            
            try {
                DriverManager.getConnection(testUrl, dbUsername, dbPassword).use { conn ->
                    plugin.logger.info("Database '$dbName' already exists")
                    return
                }
            } catch (e: SQLException) {
                if (e.message?.contains("does not exist") == true) {
                    plugin.logger.info("Database '$dbName' does not exist. Creating it...")
                } else {
                    // If it's not a "database doesn't exist" error, rethrow
                    throw e
                }
            }
            
            // Connect to the default 'postgres' database to create the target database
            val postgresUrl = "jdbc:postgresql://$dbHost:$dbPort/postgres"
            plugin.logger.info("Connecting to default database at: $postgresUrl")
            
            DriverManager.getConnection(postgresUrl, dbUsername, dbPassword).use { conn ->
                // Check if database exists
                val checkDbSQL = "SELECT 1 FROM pg_database WHERE datname = ?"
                conn.prepareStatement(checkDbSQL).use { statement ->
                    statement.setString(1, dbName)
                    val resultSet = statement.executeQuery()
                    if (!resultSet.next()) {
                        plugin.logger.info("Database '$dbName' does not exist. Creating it...")
                        val createDbSQL = "CREATE DATABASE \"$dbName\""
                        conn.createStatement().executeUpdate(createDbSQL)
                        plugin.logger.info("Database '$dbName' created successfully!")
                    } else {
                        plugin.logger.info("Database '$dbName' already exists.")
                    }
                }
            }
        } catch (e: Exception) {
            plugin.logger.severe("Database connection failed!")
            plugin.logger.severe("Host: $dbHost, Port: $dbPort, Database: $dbName, Username: $dbUsername")
            plugin.logger.severe("Error: ${e.message}")
            e.printStackTrace()
            throw DatabaseError.ConnectionError("Failed to connect to database", e)
        }
    }
    
    private fun setupConnectionPool() {
        val config = plugin.config
        val hikariConfig = HikariConfig().apply {
            jdbcUrl = "jdbc:postgresql://$dbHost:$dbPort/$dbName"
            username = dbUsername
            password = dbPassword
            
            // Connection pool settings
            maximumPoolSize = config.getInt("database.pool.maximum-size", 10)
            minimumIdle = config.getInt("database.pool.minimum-idle", 2)
            connectionTimeout = config.getLong("database.pool.connection-timeout", 30000)
            idleTimeout = config.getLong("database.pool.idle-timeout", 600000)
            maxLifetime = config.getLong("database.pool.max-lifetime", 1800000)
            
            // Performance settings
            addDataSourceProperty("cachePrepStmts", "true")
            addDataSourceProperty("prepStmtCacheSize", "250")
            addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
            addDataSourceProperty("useServerPrepStmts", "true")
            addDataSourceProperty("rewriteBatchedStatements", "true")
        }
        
        dataSource = HikariDataSource(hikariConfig)
    }
    
    private fun testConnection() {
        dataSource.connection.use { conn ->
            conn.prepareStatement("SELECT 1").use { stmt ->
                stmt.executeQuery().use { rs ->
                    if (!rs.next() || rs.getInt(1) != 1) {
                        throw SQLException("Database connection test failed")
                    }
                }
            }
        }
    }
    
    private fun setupScheduledTasks() {
        // Use the shared executor for scheduled tasks
        sharedExecutor.scheduleAtFixedRate({
            // Run batch flush within this coroutine scope to prevent leaks
            runBlocking { batchProcessor.flush() }
        }, 5, 5, TimeUnit.SECONDS)
    }
    
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
    
    /**
     * Check and ensure database connection health with auto-reconnection and lazy initialization
     */
    private suspend fun ensureConnection(): HikariDataSource = withContext(Dispatchers.IO) {
        // Handle lazy initialization if database wasn't initialized yet
        if (!::dataSource.isInitialized) {
            plugin.logger.info("Lazy initializing database connection...")
            try {
                initializeDatabaseSync()
                isConnected = true
                lastConnectionTest = System.currentTimeMillis()
                return@withContext dataSource
            } catch (e: Exception) {
                plugin.logger.severe("Failed to lazy initialize database: ${e.message}")
                throw DatabaseError.InitializationError("Lazy database initialization failed", e)
            }
        }
        
        val currentTime = System.currentTimeMillis()
        
        // Skip frequent health checks
        if (isConnected && currentTime - lastConnectionTest < healthCheckInterval) {
            return@withContext dataSource
        }
        
        try {
            // Test current connection
            dataSource.connection.use { conn ->
                conn.prepareStatement("SELECT 1").use { stmt ->
                    stmt.executeQuery().use { rs ->
                        if (!rs.next() || rs.getInt(1) != 1) {
                            throw SQLException("Health check failed")
                        }
                    }
                }
            }
            isConnected = true
            lastConnectionTest = currentTime
            plugin.logger.fine("Database health check passed")
        } catch (e: Exception) {
            plugin.logger.warning("Database connection lost, attempting reconnection...")
            isConnected = false
            
            try {
                // Attempt to recreate connection pool
                if (::dataSource.isInitialized && !dataSource.isClosed) {
                    dataSource.close()
                }
                setupConnectionPool()
                testConnection()
                isConnected = true
                lastConnectionTest = currentTime
                plugin.logger.info("Database reconnection successful")
            } catch (reconnectException: Exception) {
                plugin.logger.severe("Database reconnection failed: ${reconnectException.message}")
                throw DatabaseError.ConnectionError("Database reconnection failed", reconnectException)
            }
        }
        
        dataSource
    }
    
    /**
     * Synchronous database initialization for lazy loading
     */
    private fun initializeDatabaseSync() {
        if (!::sharedExecutor.isInitialized) {
            sharedExecutor = createOptimizedExecutor()
        }
        
        // Try to register the PostgreSQL driver manually
        try {
            Class.forName("com.binkibonk.soldbapi.libs.postgresql.Driver")
        } catch (e: ClassNotFoundException) {
            plugin.logger.warning("Could not find relocated PostgreSQL driver, trying auto-detection...")
        }
        
        // Check and create database if it doesn't exist
        ensureDatabaseExists()
        
        // Setup HikariCP
        setupConnectionPool()
        
        // Test connection
        testConnection()
        
        // Initialize batch processor
        batchProcessor = BatchProcessor(dataSource, plugin.logger)
        
        // Initialize migration manager
        migrationManager = MigrationManager(dataSource, plugin.logger)
        runBlocking { 
            migrationManager.initialize()
            migrationManager.migrate()
        }
        
        // Setup scheduled tasks with shared executor
        setupScheduledTasks()
        
        // Initialize task scheduler
        if (!::taskScheduler.isInitialized) {
            taskScheduler = TaskScheduler(this, plugin.logger, plugin.config)
            taskScheduler.startScheduledTasks()
        }
        
        plugin.logger.info("Database lazy initialization completed successfully")
    }
    
    fun shutdown() {
        plugin.logger.info("Shutting down database manager...")
        try {
            if (::taskScheduler.isInitialized) {
                taskScheduler.shutdown()
            }
            if (::batchProcessor.isInitialized) {
                runBlocking { batchProcessor.shutdown() }
            }
            if (::sharedExecutor.isInitialized) {
                sharedExecutor.shutdown()
                if (!sharedExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    sharedExecutor.shutdownNow()
                }
            }
            if (::dataSource.isInitialized) {
                dataSource.close()
                plugin.logger.info("Database connection closed")
            }
        } catch (e: Exception) {
            plugin.logger.warning("Error during database shutdown: ${e.message}")
        }
    }
    
    fun getDatabaseName(): String = dbName
    
    fun getDatabaseStatus(): String {
        return if (::dataSource.isInitialized && !dataSource.isClosed) "Connected" else "Disconnected"
    }
    
    fun getMigrationManager(): MigrationManager = migrationManager
    
    override suspend fun createTable(name: String, schema: String): Boolean {
        if (!isHealthy()) {
            throw DatabaseError.ConnectionError("Database is not connected")
        }
        
        return withContext(Dispatchers.IO) {
            try {
                validateTableName(name)
                val connection = ensureConnection()
                connection.connection.use { conn ->
                    conn.prepareStatement(schema).use { stmt ->
                        stmt.executeUpdate()
                    }
                }
                
                // Track table ownership
                val callerPlugin = getCallingPluginName()
                setTableOwner(name, callerPlugin)
                
                plugin.logger.info("Created table '$name' (owned by $callerPlugin)")
                true
            } catch (e: SQLException) {
                plugin.logger.warning("Failed to create table '$name': ${e.message}")
                false
            }
        }
    }

    // Internal method that accepts the caller plugin name explicitly
    private suspend fun createTableWithCaller(name: String, schema: String, callerPlugin: String): Boolean {
        if (!isHealthy()) {
            throw DatabaseError.ConnectionError("Database is not connected")
        }

        return withContext(Dispatchers.IO) {
            try {
                validateTableName(name)
                val connection = ensureConnection()
                connection.connection.use { conn ->
                    conn.prepareStatement(schema).use { stmt ->
                        stmt.executeUpdate()
                    }
                }

                // Track table ownership using the provided caller plugin name
                setTableOwner(name, callerPlugin)

                plugin.logger.info("Created table '$name' (owned by $callerPlugin)")
                true
            } catch (e: SQLException) {
                plugin.logger.warning("Failed to create table '$name': ${e.message}")
                false
            }
        }
    }

    override suspend fun dropTable(name: String): Boolean {
        if (!isHealthy()) {
            throw DatabaseError.ConnectionError("Database is not connected")
        }
        
        // Check ownership
        if (!checkTableOwnership(name)) {
            val owner = getTableOwnerInternal(name)
            val caller = getCallingPluginName()
            plugin.logger.warning("Plugin '$caller' attempted to drop table '$name' owned by '$owner'")
            throw DatabaseError.QueryError("Access denied: Table '$name' is owned by '$owner'")
        }
        
        return withContext(Dispatchers.IO) {
            try {
                validateTableName(name)
                val connection = ensureConnection()
                val quotedTableName = quoteIdentifier(name)
                connection.connection.use { conn ->
                    conn.prepareStatement("DROP TABLE IF EXISTS $quotedTableName").use { stmt ->
                        stmt.executeUpdate()
                    }
                }
                
                // Remove ownership tracking
                synchronized(ownershipLock) {
                    tableOwners.remove(name.lowercase())
                }
                
                plugin.logger.info("Dropped table '$name'")
                true
            } catch (e: SQLException) {
                plugin.logger.warning("Failed to drop table '$name': ${e.message}")
                false
            }
        }
    }
    
    override suspend fun tableExists(name: String): Boolean = withContext(Dispatchers.IO) {
        try {
            validateTableName(name)
            val connection = ensureConnection()
            connection.connection.use { conn ->
                val meta = conn.metaData
                meta.getTables(null, null, name, arrayOf("TABLE")).use { rs ->
                    rs.next()
                }
            }
        } catch (e: SQLException) {
            plugin.logger.log(Level.WARNING, "Failed to check if table $name exists", e)
            throw DatabaseError.QueryError("Failed to check if table $name exists", e)
        }
    }
    
    override suspend fun getTableInfo(name: String): TableInfo? = withContext(Dispatchers.IO) {
        try {
            validateTableName(name)
            val connection = ensureConnection()
            connection.connection.use { conn ->
                val meta = conn.metaData
                
                // Get primary keys
                val primaryKeys = mutableSetOf<String>()
                meta.getPrimaryKeys(null, null, name).use { rs ->
                    while (rs.next()) {
                        primaryKeys.add(rs.getString("COLUMN_NAME"))
                    }
                }
                
                // Get columns
                val columns = mutableListOf<ColumnInfo>()
                meta.getColumns(null, null, name, null).use { rs ->
                    while (rs.next()) {
                        val columnName = rs.getString("COLUMN_NAME")
                        columns.add(ColumnInfo(
                            name = columnName,
                            type = rs.getString("TYPE_NAME"),
                            nullable = rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable,
                            primaryKey = columnName in primaryKeys
                        ))
                    }
                }
                
                if (columns.isNotEmpty()) TableInfo(name, columns) else null
            }
        } catch (e: SQLException) {
            plugin.logger.log(Level.WARNING, "Failed to get table info for $name", e)
            throw DatabaseError.QueryError("Failed to get table info for $name", e)
        }
    }
    
    override suspend fun insert(table: String, data: Map<String, Any>): Long {
        if (!isHealthy()) {
            throw DatabaseError.ConnectionError("Database is not connected")
        }
        
        // Check ownership for write operations
        if (!checkTableOwnership(table)) {
            val owner = getTableOwnerInternal(table)
            val caller = getCallingPluginName()
            throw DatabaseError.QueryError("Access denied: Table '$table' is owned by '$owner'. Write operations not allowed.")
        }
        
        validateTableName(table)
        data.keys.forEach { validateColumnName(it) }
        
        return withContext(Dispatchers.IO) {
            try {
                val columns = data.keys.joinToString(", ") { quoteIdentifier(it) }
                val placeholders = data.keys.joinToString(", ") { "?" }
                val sql = "INSERT INTO ${quoteIdentifier(table)} ($columns) VALUES ($placeholders)"
                
                val connection = ensureConnection()
                connection.connection.use { conn ->
                    conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS).use { stmt ->
                        data.values.forEachIndexed { index, value ->
                            stmt.setObject(index + 1, value)
                        }
                        val rowsAffected = stmt.executeUpdate()

                        // Try to get generated ID, but handle different primary key types
                        stmt.generatedKeys.use { rs ->
                            if (rs.next()) {
                                try {
                                    // Try to get as long first (for SERIAL/auto-increment columns)
                                    rs.getLong(1)
                                } catch (e: SQLException) {
                                    // If that fails, the primary key might be UUID or other type
                                    // Return the number of affected rows as a fallback
                                    rowsAffected.toLong()
                                }
                            } else {
                                // No generated keys, return affected rows count
                                rowsAffected.toLong()
                            }
                        }
                    }
                }
            } catch (e: SQLException) {
                plugin.logger.log(Level.SEVERE, "Failed to insert into table $table", e)
                throw DatabaseError.QueryError("Failed to insert into table $table", e)
            }
        }
    }

    // Internal method that accepts the caller plugin name explicitly
    private suspend fun insertWithCaller(table: String, data: Map<String, Any>, callerPlugin: String): Long {
        if (!isHealthy()) {
            throw DatabaseError.ConnectionError("Database is not connected")
        }

        // Check ownership for write operations using the provided caller plugin name
        if (!checkTableOwnershipWithCaller(table, callerPlugin)) {
            val owner = getTableOwnerInternal(table)
            throw DatabaseError.QueryError("Access denied: Table '$table' is owned by '$owner'. Write operations not allowed.")
        }

        validateTableName(table)
        data.keys.forEach { validateColumnName(it) }

        return withContext(Dispatchers.IO) {
            try {
                val columns = data.keys.joinToString(", ") { quoteIdentifier(it) }
                val placeholders = data.keys.joinToString(", ") { "?" }
                val sql = "INSERT INTO ${quoteIdentifier(table)} ($columns) VALUES ($placeholders)"

                val connection = ensureConnection()
                connection.connection.use { conn ->
                    conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS).use { stmt ->
                        data.values.forEachIndexed { index, value ->
                            stmt.setObject(index + 1, value)
                        }
                        val rowsAffected = stmt.executeUpdate()

                        // Try to get generated ID, but handle different primary key types
                        stmt.generatedKeys.use { rs ->
                            if (rs.next()) {
                                try {
                                    // Try to get as long first (for SERIAL/auto-increment columns)
                                    rs.getLong(1)
                                } catch (e: SQLException) {
                                    // If that fails, the primary key might be UUID or other type
                                    // Return the number of affected rows as a fallback
                                    rowsAffected.toLong()
                                }
                            } else {
                                // No generated keys, return affected rows count
                                rowsAffected.toLong()
                            }
                        }
                    }
                }
            } catch (e: SQLException) {
                plugin.logger.log(Level.SEVERE, "Failed to insert into table $table", e)
                throw DatabaseError.QueryError("Failed to insert into table $table", e)
            }
        }
    }

    override suspend fun insertBatch(table: String, data: List<Map<String, Any>>): List<Long> = withContext(Dispatchers.IO) {
        if (data.isEmpty()) return@withContext emptyList()
        
        try {
            validateTableName(table)
            data.first().keys.forEach { validateColumnName(it) }
            val connection = ensureConnection()
            val quotedTableName = quoteIdentifier(table)
            val quotedColumns = data.first().keys.joinToString(", ") { quoteIdentifier(it) }
            val placeholders = data.first().keys.joinToString(", ") { "?" }
            val sql = "INSERT INTO $quotedTableName ($quotedColumns) VALUES ($placeholders)"
            
            connection.connection.use { conn ->
                conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS).use { stmt ->
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
        } catch (e: SQLException) {
            plugin.logger.log(Level.SEVERE, "Failed to batch insert into table $table", e)
            throw DatabaseError.QueryError("Failed to batch insert into table $table", e)
        }
    }
    
    override suspend fun update(table: String, data: Map<String, Any>, where: String, params: List<Any>): Int {
        if (!isHealthy()) {
            throw DatabaseError.ConnectionError("Database is not connected")
        }
        
        // Check ownership for write operations
        if (!checkTableOwnership(table)) {
            val owner = getTableOwnerInternal(table)
            val caller = getCallingPluginName()
            throw DatabaseError.QueryError("Access denied: Table '$table' is owned by '$owner'. Write operations not allowed.")
        }
        
        validateTableName(table)
        data.keys.forEach { validateColumnName(it) }
        
        return withContext(Dispatchers.IO) {
            try {
                val setClause = data.keys.joinToString(", ") { "${quoteIdentifier(it)} = ?" }
                val sql = "UPDATE ${quoteIdentifier(table)} SET $setClause WHERE $where"
                
                val connection = ensureConnection()
                connection.connection.use { conn ->
                    conn.prepareStatement(sql).use { stmt ->
                        var paramIndex = 1
                        
                        // Set data values
                        data.values.forEach { value ->
                            stmt.setObject(paramIndex++, value)
                        }
                        
                        // Set where clause parameters
                        params.forEach { param ->
                            stmt.setObject(paramIndex++, param)
                        }
                        
                        stmt.executeUpdate()
                    }
                }
            } catch (e: SQLException) {
                plugin.logger.log(Level.SEVERE, "Failed to update table $table", e)
                throw DatabaseError.QueryError("Failed to update table $table", e)
            }
        }
    }
    
    override suspend fun delete(table: String, where: String, params: List<Any>): Int {
        if (!isHealthy()) {
            throw DatabaseError.ConnectionError("Database is not connected")
        }
        
        // Check ownership for write operations
        if (!checkTableOwnership(table)) {
            val owner = getTableOwnerInternal(table)
            val caller = getCallingPluginName()
            throw DatabaseError.QueryError("Access denied: Table '$table' is owned by '$owner'. Write operations not allowed.")
        }
        
        validateTableName(table)
        // validateWhereClause removed since method doesn't exist
        
        return withContext(Dispatchers.IO) {
            try {
                val sql = "DELETE FROM ${quoteIdentifier(table)} WHERE $where"
                
                val connection = ensureConnection()
                connection.connection.use { conn ->
                    conn.prepareStatement(sql).use { stmt ->
                        params.forEachIndexed { index, param ->
                            stmt.setObject(index + 1, param)
                        }
                        stmt.executeUpdate()
                    }
                }
            } catch (e: SQLException) {
                plugin.logger.log(Level.SEVERE, "Failed to delete from table $table", e)
                throw DatabaseError.QueryError("Failed to delete from table $table", e)
            }
        }
    }
    
    override suspend fun select(table: String, columns: List<String>, where: String?, params: List<Any>): List<Map<String, Any>> = withContext(Dispatchers.IO) {
        try {
            validateTableName(table)
            // Validate column names if not using wildcard
            if (!columns.contains("*")) {
                columns.forEach { validateColumnName(it) }
            }
            val quotedTableName = quoteIdentifier(table)
            val columnList = if (columns.contains("*")) "*" else columns.joinToString(", ") { quoteIdentifier(it) }
            val sql = "SELECT $columnList FROM $quotedTableName" + if (!where.isNullOrEmpty()) " WHERE $where" else ""
            
            executeQuery(sql, params)
        } catch (e: SQLException) {
            plugin.logger.log(Level.SEVERE, "Failed to select from table $table", e)
            throw DatabaseError.QueryError("Failed to select from table $table", e)
        }
    }
    
    override suspend fun executeQuery(sql: String, params: List<Any>): List<Map<String, Any>> = withContext(Dispatchers.IO) {
        try {
            val connection = ensureConnection()
            connection.connection.use { conn ->
                conn.prepareStatement(sql).use { stmt ->
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
        } catch (e: SQLException) {
            plugin.logger.log(Level.SEVERE, "Failed to execute query: $sql", e)
            throw DatabaseError.QueryError("Failed to execute query", e)
        }
    }
    
    override suspend fun executeUpdate(sql: String, params: List<Any>): Int {
        if (!isHealthy()) {
            throw DatabaseError.ConnectionError("Database is not connected")
        }
        
        // Check if this is a write operation and if so, check ownership
        val upperSql = sql.uppercase().trim()
        val isWriteOperation = upperSql.startsWith("INSERT") || 
                              upperSql.startsWith("UPDATE") || 
                              upperSql.startsWith("DELETE") ||
                              upperSql.startsWith("DROP") ||
                              upperSql.startsWith("CREATE") ||
                              upperSql.startsWith("ALTER")
        
        if (isWriteOperation) {
            // Try to extract table name from SQL for ownership check
            val tableName = extractTableNameFromSQL(sql)
            if (tableName != null) {
                if (!checkTableOwnership(tableName)) {
                    val owner = getTableOwnerInternal(tableName)
                    val caller = getCallingPluginName()
                    throw DatabaseError.QueryError("Access denied: Table '$tableName' is owned by '$owner'. Write operations not allowed.")
                }
            }
        }
        
        return withContext(Dispatchers.IO) {
            try {
                val connection = ensureConnection()
                connection.connection.use { conn ->
                    conn.prepareStatement(sql).use { stmt ->
                        params.forEachIndexed { index, param ->
                            stmt.setObject(index + 1, param)
                        }
                        stmt.executeUpdate()
                    }
                }
            } catch (e: SQLException) {
                plugin.logger.log(Level.SEVERE, "Failed to execute update: $sql", e)
                throw DatabaseError.QueryError("Failed to execute update", e)
            }
        }
    }
    
    override suspend fun <T> withTransaction(block: suspend (DatabaseTransaction) -> T): T = withContext(Dispatchers.IO) {
        val connection = ensureConnection()
        connection.connection.use { conn ->
            conn.autoCommit = false
            try {
                val transaction = DatabaseTransactionImpl(conn)
                val result = block(transaction)
                conn.commit()
                result
            } catch (e: Exception) {
                conn.rollback()
                plugin.logger.log(Level.SEVERE, "Transaction failed, rolled back", e)
                throw DatabaseError.TransactionError("Transaction failed", e)
            } finally {
                conn.autoCommit = true
            }
        }
    }
    
    override fun queryBuilder(): QueryBuilder = QueryBuilderImpl(this)
    
    override suspend fun isHealthy(): Boolean = withContext(Dispatchers.IO) {
        try {
            val connection = ensureConnection()
            connection.connection.use { conn ->
                conn.prepareStatement("SELECT 1").use { stmt ->
                    stmt.executeQuery().use { rs ->
                        rs.next() && rs.getInt(1) == 1
                    }
                }
            }
        } catch (e: Exception) {
            plugin.logger.warning("Health check failed: ${e.message}")
            false
        }
    }
    
    // Helper method to get the calling plugin from the stack trace
    private fun getCallingPlugin(): String? {
        val stackTrace = Thread.currentThread().stackTrace
        // Look for the plugin class in the stack trace
        for (element in stackTrace) {
            val className = element.className
            // Skip our own classes
            if (className.startsWith("com.binkibonk.soldbapi")) continue
            
            // Look for plugin classes (they extend JavaPlugin)
            // We can identify plugins by their package structure or known patterns
            // For now, we'll use a simple heuristic
            if (className.contains(".plugin.") || className.contains("com.binkibonk.soldbtest")) {
                // Extract plugin name from package (simplified approach)
                val parts = className.split(".")
                if (parts.size >= 3) {
                    return parts[parts.size - 3] // Assuming com.author.pluginname.classname structure
                }
            }
        }
        return null
    }
    
    // Better approach: Get plugin from Bukkit's plugin manager
    private fun getCallingPluginName(): String {
        // Fallback: try to get from stack trace first (more reliable)
        val stackTrace = Thread.currentThread().stackTrace
        val plugins = plugin.server.pluginManager.plugins

        for (element in stackTrace) {
            val className = element.className
            // Skip our own classes and Kotlin/Java internals
            if (className.startsWith("com.binkibonk.soldbapi") ||
                className.startsWith("kotlin") ||
                className.startsWith("java") ||
                className.startsWith("org.bukkit") ||
                className.startsWith("kotlinx.coroutines")) continue

            // Look for known plugin patterns by checking class name prefixes
            for (p in plugins) {
                val pluginPackage = p.javaClass.`package`?.name
                if (pluginPackage != null && className.startsWith(pluginPackage)) {
                    plugin.logger.fine("Identified calling plugin: ${p.name} from class: $className")
                    return p.name
                }
            }
        }

        // Secondary approach: try class loader matching
        val classLoader = Thread.currentThread().contextClassLoader
        for (p in plugins) {
            if (p.javaClass.classLoader === classLoader) {
                plugin.logger.fine("Identified calling plugin via classloader: ${p.name}")
                return p.name
            }
        }

        // Log stack trace for debugging when we can't identify the caller
        plugin.logger.warning("Could not identify calling plugin. Stack trace:")
        for (element in stackTrace.take(10)) {
            plugin.logger.warning("  ${element.className}.${element.methodName}(${element.fileName}:${element.lineNumber})")
        }

        return "Unknown"
    }
    
    // Set table owner (internal method)
    private fun setTableOwner(tableName: String, pluginName: String) {
        synchronized(ownershipLock) {
            tableOwners[tableName.lowercase()] = pluginName
        }
    }
    
    // Check if current caller owns the table
    private fun checkTableOwnership(tableName: String): Boolean {
        val owner = getTableOwnerInternal(tableName)
        val caller = getCallingPluginName()
        return owner == null || owner == caller // If no owner, allow (for existing tables)
    }

    // Check if specified caller owns the table
    private fun checkTableOwnershipWithCaller(tableName: String, callerPlugin: String): Boolean {
        val owner = getTableOwnerInternal(tableName)
        return owner == null || owner == callerPlugin // If no owner, allow (for existing tables)
    }

    // Get table owner (internal)
    private fun getTableOwnerInternal(tableName: String): String? {
        return synchronized(ownershipLock) {
            tableOwners[tableName.lowercase()]
        }
    }
    
    // Blocking versions of the API methods for plugins that can't use coroutines
    override fun createTableBlocking(name: String, schema: String): Boolean {
        // Capture the calling plugin name in the current thread context before runBlocking
        val callerPlugin = getCallingPluginName()
        return runBlocking {
            createTableWithCaller(name, schema, callerPlugin)
        }
    }
    
    override fun dropTableBlocking(name: String): Boolean {
        return runBlocking { dropTable(name) }
    }
    
    override fun tableExistsBlocking(name: String): Boolean {
        return runBlocking { tableExists(name) }
    }
    
    override fun getTableInfoBlocking(name: String): TableInfo? {
        return runBlocking { getTableInfo(name) }
    }
    
    override fun insertBlocking(table: String, data: Map<String, Any>): Long {
        // Capture the calling plugin name in the current thread context before runBlocking
        val callerPlugin = getCallingPluginName()
        return runBlocking {
            insertWithCaller(table, data, callerPlugin)
        }
    }
    
    override fun insertBatchBlocking(table: String, data: List<Map<String, Any>>): List<Long> {
        return runBlocking { insertBatch(table, data) }
    }
    
    override fun updateBlocking(table: String, data: Map<String, Any>, where: String, params: List<Any>): Int {
        return runBlocking { update(table, data, where, params) }
    }
    
    override fun deleteBlocking(table: String, where: String, params: List<Any>): Int {
        return runBlocking { delete(table, where, params) }
    }
    
    override fun selectBlocking(table: String, columns: List<String>, where: String?, params: List<Any>): List<Map<String, Any>> {
        return runBlocking { select(table, columns, where, params) }
    }
    
    override fun executeQueryBlocking(sql: String, params: List<Any>): List<Map<String, Any>> {
        return runBlocking { executeQuery(sql, params) }
    }
    
    override fun executeUpdateBlocking(sql: String, params: List<Any>): Int {
        return runBlocking { executeUpdate(sql, params) }
    }
    
    override fun isHealthyBlocking(): Boolean {
        return runBlocking { isHealthy() }
    }
    
    private fun setParameters(stmt: PreparedStatement, params: List<Any>) {
        params.forEachIndexed { index, param ->
            stmt.setObject(index + 1, param)
        }
    }

    // Blocking versions of the new API methods
    override suspend fun getTableOwner(tableName: String): String? {
        return getTableOwnerInternal(tableName)
    }
    
    override fun getTableOwnerBlocking(tableName: String): String? {
        return getTableOwnerInternal(tableName)
    }
    
    override suspend fun isTableOwner(tableName: String): Boolean {
        val owner = getTableOwnerInternal(tableName)
        val caller = getCallingPluginName()
        return owner == null || owner == caller
    }
    
    override fun isTableOwnerBlocking(tableName: String): Boolean {
        val owner = getTableOwnerInternal(tableName)
        val caller = getCallingPluginName()
        return owner == null || owner == caller
    }

    // Helper method to extract table name from SQL (simple implementation)
    private fun extractTableNameFromSQL(sql: String): String? {
        val upperSql = sql.uppercase()
        
        // Simple regex patterns for common operations
        val patterns = listOf(
            Regex("""INSERT\s+INTO\s+(\w+)""", RegexOption.IGNORE_CASE),
            Regex("""UPDATE\s+(\w+)""", RegexOption.IGNORE_CASE),
            Regex("""DELETE\s+FROM\s+(\w+)""", RegexOption.IGNORE_CASE),
            Regex("""DROP\s+TABLE\s+(\w+)""", RegexOption.IGNORE_CASE),
            Regex("""CREATE\s+TABLE\s+(\w+)""", RegexOption.IGNORE_CASE)
        )
        
        for (pattern in patterns) {
            val match = pattern.find(upperSql)
            if (match != null) {
                return match.groupValues[1]
            }
        }
        
        return null
    }
}