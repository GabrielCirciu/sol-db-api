// ===== src/main/kotlin/com/binkibonk/soldbcore/data/TableInfo.kt =====
package com.binkibonk.soldbcore.data

data class TableInfo(
    val name: String,
    val columns: List<ColumnInfo>
)