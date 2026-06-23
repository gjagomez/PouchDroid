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

private const val TAG = "RxDroid-SSE"
private const val HEARTBEAT_MS = 10_000       // CouchDB envía heartbeat cada 10s
private const val RECONNECT_DELAY_MS = 5_000L // igual que RxDB retryTime

/**
 * Escucha cambios en tiempo real desde CouchDB usando Server-Sent Events.
 * Equivalente al pull.stream$ de RxDB con feed=eventsource.
 *
 * Flujo:
 *   CouchDB /_changes?feed=eventsource
 *       │  (streaming, conexión persistente)
 *       ▼
 *   CouchDbSSEListener
 *       │  parsea cada evento
 *       ▼
 *   Room (DocumentDao.upsert)
 *       │  Room emite nuevo valor al Flow
 *       ▼
 *   UI se actualiza automáticamente
 */
class CouchDbSSEListener(
    private val config: RxDroidConfig,
    private val dao: DocumentDao,
    private val client: CouchDbClient,
    private val gson: Gson
) {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val mapType = object : TypeToken<Map<String, Any?>>() {}.type

    // Inicia la escucha para una colección.
    // since = "now" para solo cambios nuevos (post-sync inicial)
    // since = "0"   para todos los cambios desde el principio
    fun start(collection: String, since: String = "now") {
        scope.launch {
            var lastSeq = since
            Log.d(TAG, "SSE started for '$collection' since='$lastSeq'")

            while (isActive) {
                try {
                    listenToChanges(collection, lastSeq) { newSeq ->
                        lastSeq = newSeq
                    }
                } catch (e: CancellationException) {
                    break
                } catch (e: Exception) {
                    Log.w(TAG, "SSE disconnected for '$collection': ${e.message}. Retrying in ${RECONNECT_DELAY_MS}ms...")
                    delay(RECONNECT_DELAY_MS)
                }
            }
            Log.d(TAG, "SSE stopped for '$collection'")
        }
    }

    fun stop() {
        scope.cancel()
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

        // OkHttp con timeout deshabilitado para streaming persistente
        val streamClient = client.httpClient.newBuilder()
            .readTimeout(0, java.util.concurrent.TimeUnit.SECONDS)  // sin timeout en streaming
            .build()

        streamClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                throw Exception("HTTP ${response.code}: ${response.message}")
            }

            Log.d(TAG, "SSE connected to CouchDB for '$collection'")
            val source = response.body!!.source()

            while (!source.exhausted() && isActive) {
                val line = source.readUtf8Line() ?: break

                when {
                    // Línea de datos: "data: {...}"
                    line.startsWith("data:") -> {
                        val json = line.removePrefix("data:").trim()
                        if (json.isNotEmpty()) {
                            processEvent(collection, json, onSeqUpdate)
                        }
                    }
                    // Línea vacía = heartbeat de CouchDB, ignorar
                    line.isEmpty() -> Unit
                    // Comentario SSE
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

        val seq    = event["seq"] as? String ?: return
        val docId  = event["id"]  as? String ?: return
        val deleted = event["deleted"] as? Boolean ?: false

        // Ignorar documentos de diseño y checkpoints internos
        if (docId.startsWith("_")) return

        val entity = if (deleted) {
            DocumentEntity(
                compositeId = "$collection|$docId",
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
            val updatedAt = (docData["updatedAt"] as? Double)?.toLong()
                ?: System.currentTimeMillis()

            DocumentEntity(
                compositeId = "$collection|$docId",
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
