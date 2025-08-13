package com.binkibonk.soldbapi.data

data class ColumnInfo(
    val name: String,
    val type: String,
    val nullable: Boolean,
    val primaryKey: Boolean
)