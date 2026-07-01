package com.genesis.rxdroid.sync

import android.util.Log
import com.genesis.rxdroid.sync.couch.CouchDbClient
import com.genesis.rxdroid.sync.storage.DocumentDao
import com.genesis.rxdroid.sync.storage.DocumentEntity
import com.genesis.rxdroid.sync.storage.SyncStatus
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import kotlinx.coroutines.*
import okhttp3.Credentials
import okhttp3.Request
import java.util.concurrent.ConcurrentHashMap

private const val TAG = "RxDroid-SSE"
private const val HEARTBEAT_MS = 30_000
private const val RECONNECT_DELAY_BASE_MS = 5_000L
private const val RECONNECT_DELAY_MAX_MS = 120_000L

class CouchDbSSEListener(
    private val config: RxDroidConfig,
    private val dao: DocumentDao,
    private val client: CouchDbClient,
    private val gson: Gson
) {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val mapType = object : TypeToken<Map<String, Any?>>() {}.type

    // ConcurrentHashMap: acceso seguro desde main thread (start/stop) e IO dispatcher (seq updates)
    private val activeJobs = ConcurrentHashMap<String, Job>()
    private val lastSeqByCollection = ConcurrentHashMap<String, String>()

    fun start(collection: String, since: String? = null) {
        activeJobs[collection]?.cancel()
        val startSince = since ?: lastSeqByCollection[collection] ?: "now"

        activeJobs[collection] = scope.launch {
            var lastSeq = startSince
            var reconnectDelay = RECONNECT_DELAY_BASE_MS
            Log.d(TAG, "SSE started for '$collection' since='$lastSeq'")

            while (isActive) {
                try {
                    listenToChanges(collection, lastSeq) { newSeq ->
                        lastSeq = newSeq
                        lastSeqByCollection[collection] = newSeq
                        reconnectDelay = RECONNECT_DELAY_BASE_MS
                    }
                } catch (e: CancellationException) {
                    break
                } catch (e: Exception) {
                    Log.w(TAG, "SSE disconnected for '$collection': ${e.message}. Retrying in ${reconnectDelay}ms...")
                    delay(reconnectDelay)
                    reconnectDelay = minOf(reconnectDelay * 2, RECONNECT_DELAY_MAX_MS)
                }
            }
            Log.d(TAG, "SSE stopped for '$collection'")
        }
    }

    fun stop() {
        activeJobs.values.forEach { it.cancel() }
        activeJobs.clear()
    }

    fun destroy() {
        scope.cancel()
        activeJobs.clear()
        lastSeqByCollection.clear()
    }

    private suspend fun listenToChanges(
        collection: String,
        since: String,
        onSeqUpdate: (String) -> Unit
    ) = withContext(Dispatchers.IO) {
        val baseUrl = config.url.trimEnd('/')
        val url = "$baseUrl/${config.database}/_changes" +
            "?feed=eventsource" +
            "&since=$since" +
            "&include_docs=true" +
            "&heartbeat=$HEARTBEAT_MS"

        val request = Request.Builder()
            .url(url)
            .header("Authorization", Credentials.basic(config.username, config.password))
            .header("Accept", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .build()

        val streamClient = client.httpClient.newBuilder()
            .readTimeout(0, java.util.concurrent.TimeUnit.SECONDS)
            .build()

        streamClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                throw Exception("HTTP ${response.code}: ${response.message}")
            }
            Log.d(TAG, "SSE connected for '$collection'")
            val source = response.body!!.source()

            while (!source.exhausted() && isActive) {
                val line = source.readUtf8Line() ?: break
                when {
                    line.startsWith("data:") -> {
                        val json = line.removePrefix("data:").trim()
                        if (json.isNotEmpty()) processEvent(collection, json, onSeqUpdate)
                    }
                    line.isEmpty() -> Unit
                    line.startsWith(":") -> Unit
                }
            }
        }
    }

    private suspend fun processEvent(
        collection: String,
        json: String,
        onSeqUpdate: (String) -> Unit
    ) {
        val event = runCatching {
            gson.fromJson<Map<String, Any?>>(json, mapType)
        }.getOrNull() ?: return

        val seq    = event["seq"]?.toString() ?: return
        val docId  = event["id"] as? String ?: return
        val deleted = (event["deleted"] as? Boolean) ?: (event["_deleted"] as? Boolean) ?: false

        // Ignorar docs internos de CouchDB (_design/, _local/, etc.)
        // Avanzar siempre el seq para no repetir el evento en cada reconexión
        if (docId.startsWith("_")) {
            onSeqUpdate(seq)
            return
        }

        Log.d(TAG, "SSE event: id=$docId, deleted=$deleted, seq=$seq")
        val compositeId = "$collection|$docId"

        val entity = if (deleted) {
            // Solo procesar el delete si el doc existe en esta colección
            if (dao.getById(compositeId) == null) {
                onSeqUpdate(seq)
                return
            }
            DocumentEntity(
                compositeId = compositeId,
                collection = collection,
                docId = docId,
                data = "{}",
                deleted = true,
                updatedAt = System.currentTimeMillis(),
                syncStatus = SyncStatus.SYNCED
            )
        } else {
            @Suppress("UNCHECKED_CAST")
            val docData = (event["doc"] as? Map<String, Any?>) ?: mapOf("_id" to docId)

            // Filtrar por colección: solo procesar si rxCollection coincide (o no tiene campo)
            val docCollection = docData[FIELD_COLLECTION] as? String
            if (docCollection != null && docCollection != collection) {
                onSeqUpdate(seq)
                return
            }

            val updatedAt = (docData["updatedAt"] as? Double)?.toLong()
                ?: System.currentTimeMillis()

            val existing = dao.getById(compositeId)
            if (existing?.syncStatus == SyncStatus.PENDING && existing.updatedAt >= updatedAt) {
                Log.d(TAG, "SSE skipped '$docId' — local PENDING is newer")
                onSeqUpdate(seq)
                return
            }

            DocumentEntity(
                compositeId = compositeId,
                collection = collection,
                docId = docId,
                data = gson.toJson(docData),
                rev = docData["_rev"] as? String,
                deleted = false,
                updatedAt = updatedAt,
                syncStatus = SyncStatus.SYNCED
            )
        }

        dao.upsert(entity)
        onSeqUpdate(seq)
        Log.d(TAG, "SSE → Room: '$docId' (deleted=$deleted)")
    }
}
