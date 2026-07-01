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
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

private const val TAG = "RxDroid"
private const val CHANGES_PAGE_SIZE = 50
private const val DOCS_CHUNK_SIZE = 10
private const val PUSH_CHUNK_SIZE = 10

// Campo que identifica a qué colección pertenece un doc en CouchDB.
// Permite filtrar el _changes feed cuando hay múltiples colecciones en la misma DB.
internal const val FIELD_COLLECTION = "rxCollection"

class SyncManager(
    private val config: RxDroidConfig,
    private val dao: DocumentDao,
    private val client: CouchDbClient,
    private val conflictResolver: ConflictResolver,
    private val gson: Gson,
    private val deviceId: String
) {
    private val checkpointId = "rxdroid_${config.database}_$deviceId"
    private val mapType = object : TypeToken<Map<String, Any?>>() {}.type
    private var cachedCheckpointRev: String? = null

    // Un Mutex por colección: descarta syncs duplicados en lugar de encolarlos
    private val syncMutexes = ConcurrentHashMap<String, Mutex>()
    // Serializa acceso a cachedCheckpointRev entre corrutinas concurrentes
    private val checkpointMutex = Mutex()

    suspend fun sync(collection: String) {
        val mutex = syncMutexes.getOrPut(collection) { Mutex() }
        if (!mutex.tryLock()) {
            Log.d(TAG, "Sync already in progress for '$collection', skipping")
            return
        }
        try {
            try {
                pull(collection)
            } catch (e: Exception) {
                Log.e(TAG, "Pull failed for '$collection': ${e.javaClass.simpleName}: ${e.message}")
            }
            push(collection)
        } finally {
            mutex.unlock()
        }
    }

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

        val pendingIds = dao.getPendingNonDeleted(collection, SyncStatus.PENDING)
            .associate { it.compositeId to it.updatedAt }

        do {
            val changes = client.api.getChanges(
                db = config.database,
                since = since,
                limit = CHANGES_PAGE_SIZE,
                includeDocs = false
            )

            if (changes.results.isEmpty()) break

            val userDocs = changes.results.filter { !it.id.startsWith("_") }
            val (deleted, toFetch) = userDocs.partition { it.deleted }

            // Para deletes: solo propagar si el doc existe en esta colección localmente
            val deletedEntities = deleted.mapNotNull { row ->
                val compositeId = "$collection|${row.id}"
                if (dao.getById(compositeId) == null) return@mapNotNull null
                Log.d(TAG, "Pull: marking '${row.id}' as DELETED in '$collection'")
                DocumentEntity(
                    compositeId = compositeId,
                    collection = collection,
                    docId = row.id,
                    data = "{}",
                    deleted = true,
                    updatedAt = System.currentTimeMillis(),
                    syncStatus = SyncStatus.SYNCED
                )
            }
            if (deletedEntities.isNotEmpty()) dao.upsertAll(deletedEntities)

            toFetch.chunked(DOCS_CHUNK_SIZE).forEach { chunk ->
                val keys = chunk.map { it.id }
                val response = client.api.getDocs(
                    db = config.database,
                    keys = AllDocsRequest(keys)
                )

                val entities = response.rows.mapNotNull { row ->
                    val docData = row.doc ?: return@mapNotNull null

                    // Filtrar por colección: solo procesar docs que pertenezcan a esta colección.
                    // Docs sin rxCollection (datos previos a esta versión) se aceptan en todas.
                    val docCollection = docData[FIELD_COLLECTION] as? String
                    if (docCollection != null && docCollection != collection) {
                        return@mapNotNull null
                    }

                    val updatedAt = (docData["updatedAt"] as? Double)?.toLong()
                        ?: System.currentTimeMillis()
                    val compositeId = "$collection|${row.id}"
                    val localPendingAt = pendingIds[compositeId]
                    if (localPendingAt != null && localPendingAt >= updatedAt) {
                        Log.d(TAG, "Pull skipped '${row.id}' — local PENDING is newer")
                        return@mapNotNull null
                    }
                    DocumentEntity(
                        compositeId = compositeId,
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
                    Log.d(TAG, "Pull: ${entities.size} docs for '$collection'")
                }
            }

            saveLastSeq(changes.lastSeq)
            since = changes.lastSeq

        } while (changes.results.size >= CHANGES_PAGE_SIZE)
    }

    private suspend fun push(collection: String) {
        val pending = dao.getByStatus(collection, SyncStatus.PENDING)
        if (pending.isEmpty()) return

        pending.chunked(PUSH_CHUNK_SIZE).forEach { chunk ->
            val docs = chunk.map { entity ->
                val docMap = gson.fromJson<Map<String, Any?>>(entity.data, mapType).toMutableMap()

                docMap.remove("_id")
                docMap.remove("id")
                docMap.remove("_rev")
                docMap.remove("rev")
                docMap.remove("_deleted")

                val rawUpdatedAt = docMap["updatedAt"]
                if (rawUpdatedAt != null) {
                    when (rawUpdatedAt) {
                        is Double -> docMap["updatedAt"] = rawUpdatedAt.toLong()
                        is String -> docMap["updatedAt"] = rawUpdatedAt.toLongOrNull() ?: System.currentTimeMillis()
                    }
                }

                // Marca la colección de origen para filtrar en pull/SSE
                docMap[FIELD_COLLECTION] = collection

                docMap["_id"] = entity.docId
                if (entity.rev != null) docMap["_rev"] = entity.rev
                if (entity.deleted) docMap["_deleted"] = true
                docMap
            }

            try {
                val results = client.api.bulkDocs(config.database, BulkDocsRequest(docs))
                Log.d(TAG, "Push: ${chunk.size} docs for '$collection'")

                results.forEach { result ->
                    val compositeId = "$collection|${result.id}"
                    when {
                        result.error == null -> {
                            Log.d(TAG, "Push OK '${result.id}' rev=${result.rev}")
                            dao.markSynced(compositeId, result.rev)
                        }
                        result.error == "conflict" -> handleConflict(collection, result.id)
                        else -> Log.e(TAG, "Push error '${result.id}': ${result.error} - ${result.reason}")
                    }
                }
            } catch (e: Exception) {
                Log.e(TAG, "Failed to push chunk: ${e.message}")
                throw e
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

    private suspend fun getLastSeq(): String = checkpointMutex.withLock {
        val response = client.api.getCheckpoint(config.database, checkpointId)
        if (response.isSuccessful) {
            val body = response.body()
            cachedCheckpointRev = body?.rev
            return@withLock body?.lastSeq ?: "0"
        }
        // Migración: checkpoint anterior sin deviceId
        val legacyId = "rxdroid_${config.database}"
        val legacy = client.api.getCheckpoint(config.database, legacyId)
        if (legacy.isSuccessful) legacy.body()?.lastSeq ?: "0" else "0"
    }

    private suspend fun saveLastSeq(seq: String) = checkpointMutex.withLock {
        val checkpoint = CheckpointDoc(
            id = "_local/$checkpointId",
            rev = cachedCheckpointRev,
            lastSeq = seq
        )
        val response = client.api.saveCheckpoint(config.database, checkpointId, checkpoint)
        if (response.isSuccessful) {
            cachedCheckpointRev = response.body()?.rev
        } else {
            Log.e(TAG, "Failed to save checkpoint seq=$seq: HTTP ${response.code()}")
            // Limpiar rev para forzar un GET fresco en el próximo sync y evitar el bucle de 409
            cachedCheckpointRev = null
        }
    }
}
