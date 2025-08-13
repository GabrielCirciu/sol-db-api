// ===== src/main/kotlin/com/binkibonk/soldbcore/data/ColumnInfo.kt =====
package com.binkibonk.soldbcore.data

data class ColumnInfo(
    val name: String,
    val type: String,
    val nullable: Boolean,
    val primaryKey: Boolean
)