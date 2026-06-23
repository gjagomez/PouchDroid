package com.genesis.rxdroid.sync

import android.util.Log
import com.genesis.rxdroid.sync.couch.AllDocsRequest
import com.genesis.rxdroid.sync.couch.BulkDocsRequest
import com.genesis.rxdroid.sync.couch.CheckpointDoc
import com.genesis.rxdroid.sync.couch.CouchDbClient
import com.genesis.rxdroid.sync.storage.DocumentDao
import com.genesis.rxdroid.sync.storage.DocumentEntity
import com.genesis.rxdroid.sync.storage.SyncStatus
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

private const val TAG = "RxDroid"
private const val MIN_SYNC_INTERVAL_MS = 60_000L   // mínimo 1 minuto entre syncs
private const val CHANGES_PAGE_SIZE = 50            // IDs por página (sin contenido)
private const val DOCS_CHUNK_SIZE = 10              // docs con contenido por request
private const val PUSH_CHUNK_SIZE = 10              // docs por push

class SyncManager(
    private val config: RxDroidConfig,
    private val dao: DocumentDao,
    private val client: CouchDbClient,
    private val conflictResolver: ConflictResolver,
    private val gson: Gson,
    private val deviceId: String
) {
    // Checkpoint único por dispositivo — evita colisiones en multi-device
    private val checkpointId = "rxdroid_${config.database}_$deviceId"
    private val mapType = object : TypeToken<Map<String, Any?>>() {}.type
    private var lastSyncAt: Long = 0L

    // Sync completo con cooldown (para WorkManager y onResume)
    suspend fun sync(collection: String) {
        val now = System.currentTimeMillis()
        if (now - lastSyncAt < MIN_SYNC_INTERVAL_MS) {
            Log.d(TAG, "Sync skipped — too soon since last sync")
            return
        }
        lastSyncAt = now
        try {
            pull(collection)
            push(collection)
        } catch (e: Exception) {
            Log.e(TAG, "Sync failed for '$collection': ${e.message}")
            throw e
        }
    }

    // Push inmediato sin cooldown (para cambios locales, igual que RxDB upstream)
    suspend fun pushImmediate(collection: String) {
        try {
            push(collection)
        } catch (e: Exception) {
            Log.e(TAG, "Immediate push failed for '$collection': ${e.message}")
            throw e
        }
    }

    private suspend fun pull(collection: String) {
        var since = getLastSeq()

        do {
            // Fase 1: traer solo IDs y seqs (sin contenido del doc)
            val changes = client.api.getChanges(
                db = config.database,
                since = since,
                limit = CHANGES_PAGE_SIZE,
                includeDocs = false
            )

            if (changes.results.isEmpty()) break

            val (deleted, toFetch) = changes.results.partition { it.deleted }

            // Guardar deletes directamente (no necesitan contenido)
            val deletedEntities = deleted.map { row ->
                DocumentEntity(
                    compositeId = "$collection|${row.id}",
                    collection = collection,
                    docId = row.id,
                    data = "{}",
                    deleted = true,
                    updatedAt = System.currentTimeMillis(),
                    syncStatus = SyncStatus.SYNCED
                )
            }
            if (deletedEntities.isNotEmpty()) dao.upsertAll(deletedEntities)

            // Fase 2: traer contenido en chunks de DOCS_CHUNK_SIZE
            toFetch.chunked(DOCS_CHUNK_SIZE).forEach { chunk ->
                val keys = chunk.map { it.id }
                val response = client.api.getDocs(
                    db = config.database,
                    keys = AllDocsRequest(keys)
                )

                val entities = response.rows.mapNotNull { row ->
                    val docData = row.doc ?: return@mapNotNull null
                    val updatedAt = (docData["updatedAt"] as? Double)?.toLong()
                        ?: System.currentTimeMillis()

                    DocumentEntity(
                        compositeId = "$collection|${row.id}",
                        collection = collection,
                        docId = row.id,
                        data = gson.toJson(docData),
                        rev = docData["_rev"] as? String,
                        deleted = false,
                        updatedAt = updatedAt,
                        syncStatus = SyncStatus.SYNCED
                    )
                }

                if (entities.isNotEmpty()) {
                    dao.upsertAll(entities)
                    Log.d(TAG, "Pull: ${entities.size} docs fetched for '$collection'")
                }
            }

            saveLastSeq(changes.lastSeq)
            since = changes.lastSeq

        } while (changes.results.size >= CHANGES_PAGE_SIZE)
    }

    private suspend fun push(collection: String) {
        val pending = dao.getByStatus(collection, SyncStatus.PENDING)
        if (pending.isEmpty()) return

        // Push en chunks para no enviar un body enorme
        pending.chunked(PUSH_CHUNK_SIZE).forEach { chunk ->
            val docs = chunk.map { entity ->
                val docMap = gson.fromJson<Map<String, Any?>>(entity.data, mapType).toMutableMap()
                docMap["_id"] = entity.docId
                if (entity.rev != null) docMap["_rev"] = entity.rev
                if (entity.deleted) docMap["_deleted"] = true
                docMap
            }

            val results = client.api.bulkDocs(config.database, BulkDocsRequest(docs))
            Log.d(TAG, "Push: ${chunk.size} docs sent for '$collection'")

            results.forEach { result ->
                val compositeId = "$collection|${result.id}"
                when {
                    result.error == null -> dao.markSynced(compositeId, result.rev)
                    result.error == "conflict" -> handleConflict(collection, result.id)
                    else -> Log.e(TAG, "Push error '${result.id}': ${result.error} - ${result.reason}")
                }
            }
        }
    }

    private suspend fun handleConflict(collection: String, docId: String) {
        val compositeId = "$collection|$docId"
        val local = dao.getById(compositeId) ?: return

        val response = client.api.getDoc(config.database, docId)
        if (response.isSuccessful && response.body() != null) {
            val resolved = conflictResolver.resolve(local, response.body()!!)
            dao.upsert(resolved)
            Log.d(TAG, "Conflict resolved for '$docId'")
        } else {
            dao.markConflict(compositeId)
            Log.w(TAG, "Could not fetch remote for conflict: '$docId'")
        }
    }

    private suspend fun getLastSeq(): String {
        val response = client.api.getCheckpoint(config.database, checkpointId)
        return if (response.isSuccessful) response.body()?.lastSeq ?: "0" else "0"
    }

    private suspend fun saveLastSeq(seq: String) {
        val existing = runCatching {
            client.api.getCheckpoint(config.database, checkpointId).body()
        }.getOrNull()

        val checkpoint = CheckpointDoc(
            id = "_local/$checkpointId",
            rev = existing?.rev,
            lastSeq = seq
        )
        client.api.saveCheckpoint(config.database, checkpointId, checkpoint)
    }
}
