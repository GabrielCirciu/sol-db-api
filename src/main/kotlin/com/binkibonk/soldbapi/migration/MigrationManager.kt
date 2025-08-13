package com.binkibonk.soldbapi.migration

import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.sql.Connection
import java.sql.SQLException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.logging.Logger

/**
 * Manages database migrations - applying, tracking, and rolling back schema changes
 */
class MigrationManager(
    private val dataSource: HikariDataSource,
    private val logger: Logger
) {
    private val migrations = mutableListOf<Migration>()
    
    /**
     * Register a migration to be managed
     */
    fun addMigration(migration: Migration) {
        migrations.add(migration)
        migrations.sortBy { it.version }
    }
    
    /**
     * Initialize the migration tracking table
     */
    suspend fun initialize() = withContext(Dispatchers.IO) {
        dataSource.connection.use { conn ->
            val createMigrationTableSQL = """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    description VARCHAR(255) NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    execution_time_ms BIGINT DEFAULT 0
                )
            """.trimIndent()
            
            conn.createStatement().executeUpdate(createMigrationTableSQL)
            logger.info("Migration tracking table initialized")
        }
    }
    
    /**
     * Apply all pending migrations
     */
    suspend fun migrate() = withContext(Dispatchers.IO) {
        val appliedVersions = getAppliedVersions()
        val pendingMigrations = migrations.filter { it.version !in appliedVersions }
        
        if (pendingMigrations.isEmpty()) {
            logger.info("No pending migrations")
            return@withContext
        }
        
        logger.info("Applying ${pendingMigrations.size} pending migrations")
        
        for (migration in pendingMigrations) {
            applyMigration(migration)
        }
        
        logger.info("All migrations applied successfully")
    }
    
    /**
     * Get the current schema version (highest applied migration version)
     */
    suspend fun getCurrentVersion(): Int = withContext(Dispatchers.IO) {
        dataSource.connection.use { conn ->
            val sql = "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
            conn.createStatement().executeQuery(sql).use { rs ->
                if (rs.next()) rs.getInt(1) else 0
            }
        }
    }
    
    /**
     * Check if a specific migration version has been applied
     */
    suspend fun isMigrationApplied(version: Int): Boolean = withContext(Dispatchers.IO) {
        dataSource.connection.use { conn ->
            val sql = "SELECT COUNT(*) FROM schema_migrations WHERE version = ?"
            conn.prepareStatement(sql).use { stmt ->
                stmt.setInt(1, version)
                stmt.executeQuery().use { rs ->
                    rs.next() && rs.getInt(1) > 0
                }
            }
        }
    }
    
    /**
     * Get all applied migration versions
     */
    private fun getAppliedVersions(): Set<Int> {
        return dataSource.connection.use { conn ->
            val sql = "SELECT version FROM schema_migrations ORDER BY version"
            conn.createStatement().executeQuery(sql).use { rs ->
                val versions = mutableSetOf<Int>()
                while (rs.next()) {
                    versions.add(rs.getInt("version"))
                }
                versions
            }
        }
    }
    
    /**
     * Apply a single migration
     */
    private fun applyMigration(migration: Migration) {
        dataSource.connection.use { conn ->
            conn.autoCommit = false
            try {
                val startTime = System.currentTimeMillis()
                
                // Validate migration before applying
                if (!migration.validate()) {
                    throw SQLException("Migration validation failed for version ${migration.version}")
                }
                
                logger.info("Applying migration ${migration.version}: ${migration.description}")
                
                // Execute migration SQL statements
                for (sql in migration.up()) {
                    conn.createStatement().executeUpdate(sql)
                }
                
                val executionTime = System.currentTimeMillis() - startTime
                
                // Record migration as applied
                val recordSQL = """
                    INSERT INTO schema_migrations (version, description, execution_time_ms) 
                    VALUES (?, ?, ?)
                """.trimIndent()
                
                conn.prepareStatement(recordSQL).use { stmt ->
                    stmt.setInt(1, migration.version)
                    stmt.setString(2, migration.description)
                    stmt.setLong(3, executionTime)
                    stmt.executeUpdate()
                }
                
                conn.commit()
                logger.info("Migration ${migration.version} applied successfully in ${executionTime}ms")
                
            } catch (e: Exception) {
                conn.rollback()
                logger.severe("Failed to apply migration ${migration.version}: ${e.message}")
                throw e
            } finally {
                conn.autoCommit = true
            }
        }
    }
    
    /**
     * Rollback the last applied migration (if down() is implemented)
     */
    suspend fun rollback() = withContext(Dispatchers.IO) {
        val currentVersion = getCurrentVersion()
        if (currentVersion == 0) {
            logger.info("No migrations to rollback")
            return@withContext
        }
        
        val migration = migrations.find { it.version == currentVersion }
        if (migration == null) {
            logger.warning("Migration with version $currentVersion not found for rollback")
            return@withContext
        }
        
        if (migration.down().isEmpty()) {
            logger.warning("Migration ${migration.version} does not support rollback")
            return@withContext
        }
        
        dataSource.connection.use { conn ->
            conn.autoCommit = false
            try {
                logger.info("Rolling back migration ${migration.version}: ${migration.description}")
                
                // Execute rollback SQL statements
                for (sql in migration.down()) {
                    conn.createStatement().executeUpdate(sql)
                }
                
                // Remove migration record
                val removeSQL = "DELETE FROM schema_migrations WHERE version = ?"
                conn.prepareStatement(removeSQL).use { stmt ->
                    stmt.setInt(1, migration.version)
                    stmt.executeUpdate()
                }
                
                conn.commit()
                logger.info("Migration ${migration.version} rolled back successfully")
                
            } catch (e: Exception) {
                conn.rollback()
                logger.severe("Failed to rollback migration ${migration.version}: ${e.message}")
                throw e
            } finally {
                conn.autoCommit = true
            }
        }
    }
} 