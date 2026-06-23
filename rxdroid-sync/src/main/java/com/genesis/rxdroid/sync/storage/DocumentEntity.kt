package com.genesis.rxdroid.sync.storage

import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey

@Entity(
    tableName = "rx_documents",
    indices = [
        Index(value = ["collection", "docId"], unique = true),
        Index(value = ["collection", "syncStatus"])
    ]
)
data class DocumentEntity(
    @PrimaryKey
    val compositeId: String,        // "{collection}|{docId}"
    val collection: String,
    val docId: String,
    val data: String,               // JSON completo del documento
    val rev: String? = null,        // _rev de CouchDB
    val deleted: Boolean = false,
    val updatedAt: Long = 0L,
    val syncStatus: String = SyncStatus.PENDING
)

object SyncStatus {
    const val SYNCED = "SYNCED"
    const val PENDING = "PENDING"
    const val CONFLICT = "CONFLICT"
}
