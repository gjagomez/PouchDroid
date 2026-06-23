package com.genesis.rxdroid.sync

data class RxDroidConfig(
    val url: String,
    val username: String,
    val password: String,
    val database: String,
    val syncIntervalMinutes: Long = 15L,
    val batchSize: Int = 100,
    val liveSync: Boolean = true
)
