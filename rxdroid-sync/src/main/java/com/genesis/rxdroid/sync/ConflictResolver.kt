package com.genesis.rxdroid.sync

import com.genesis.rxdroid.sync.storage.DocumentEntity
import com.genesis.rxdroid.sync.storage.SyncStatus
import com.google.gson.Gson

class ConflictResolver(private val gson: Gson) {

    // Estrategia last-write-wins: gana el documento con updatedAt más reciente
    fun resolve(local: DocumentEntity, remoteData: Map<String, Any?>): DocumentEntity {
        val remoteUpdatedAt = (remoteData["updatedAt"] as? Double)?.toLong()
            ?: (remoteData["updated_at"] as? Double)?.toLong()
            ?: 0L

        return if (local.updatedAt >= remoteUpdatedAt) {
            // local gana: conservar data local pero actualizar _rev para poder hacer push
            local.copy(
                rev = remoteData["_rev"] as? String ?: local.rev,
                syncStatus = SyncStatus.PENDING
            )
        } else {
            // remoto gana: reemplazar data local con la versión del servidor
            local.copy(
                data = gson.toJson(remoteData),
                rev = remoteData["_rev"] as? String,
                updatedAt = remoteUpdatedAt,
                syncStatus = SyncStatus.SYNCED
            )
        }
    }
}
