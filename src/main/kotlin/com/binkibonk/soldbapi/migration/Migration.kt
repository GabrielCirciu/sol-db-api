package com.binkibonk.soldbapi.migration

/**
 * Represents a database migration with a version and SQL statements to execute
 */
abstract class Migration(val version: Int, val description: String) {
    /**
     * SQL statements to execute when applying this migration
     */
    abstract fun up(): List<String>
    
    /**
     * SQL statements to execute when rolling back this migration (optional)
     */
    open fun down(): List<String> = emptyList()
    
    /**
     * Validates that this migration can be safely applied
     */
    open fun validate(): Boolean = true
} 