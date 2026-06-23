package com.genesis.rxdroid.sync

import android.content.Context
import androidx.work.*
import com.genesis.rxdroid.sync.couch.CouchDbClient
import com.genesis.rxdroid.sync.storage.RxDroidDatabase
import com.google.gson.Gson
import kotlinx.coroutines.flow.map
import java.util.concurrent.TimeUnit

class RxDroid private constructor(
    private val context: Context,
    val config: RxDroidConfig,
    private val db: RxDroidDatabase,
    private val couchClient: CouchDbClient
) {
    private val collections = mutableSetOf<String>()

    private val syncManager = SyncManager(
        config = config,
        dao = db.documentDao(),
        client = couchClient,
        conflictResolver = ConflictResolver(gson),
        gson = gson
    )

    private val changeWatcher = ChangeWatcher(context, syncManager, collections)

    private val sseListener = CouchDbSSEListener(
        config = config,
        dao = db.documentDao(),
        client = couchClient,
        gson = gson
    )

    // Modo dinámico — sin data class, para formularios con estructura variable
    fun collection(name: String): RxCollection<RxDocument> {
        collections.add(name)
        return RxCollection.dynamic(name, db.documentDao(), gson)
    }

    // Modo tipado — con data class específica
    fun <T> collection(name: String, type: Class<T>): RxCollection<T> {
        collections.add(name)
        return RxCollection.typed(name, type, db.documentDao(), gson)
    }

    inline fun <reified T> collection(name: String, typed: Boolean): RxCollection<T> =
        collection(name, T::class.java)

    suspend fun syncAll() {
        collections.forEach { syncManager.sync(it) }
    }

    /**
     * Inicia sincronización completa:
     *
     * 1. SSE  — CouchDB → Room en tiempo real (como RxDB pull.stream$)
     * 2. Push — Room → CouchDB en ~500ms tras cambio local (como RxDB upstream)
     * 3. Red  — sync inmediato al reconectarse
     * 4. WorkManager — respaldo periódico en background
     */
    fun startSync() {
        // 1. SSE: escucha cambios remotos en tiempo real
        if (config.liveSync) {
            collections.forEach { collection ->
                sseListener.start(collection, since = "now")
            }
        }

        // 2. Push inmediato cuando hay cambios locales pendientes
        val pendingFlow = db.documentDao().observePendingCount().map { Unit }
        changeWatcher.watchLocalChanges(pendingFlow)

        // 3. Sync inmediato al recuperar red
        changeWatcher.watchNetwork()

        // 4. WorkManager como respaldo cada 15 min
        startPeriodicSync()
    }

    fun stopSync() {
        sseListener.stop()
        changeWatcher.stop()
        WorkManager.getInstance(context).cancelUniqueWork(WORK_NAME)
    }

    // Pausa SSE (cuando la app va a background)
    fun pauseLiveSync() {
        sseListener.stop()
    }

    // Reanuda SSE (cuando la app vuelve a primer plano)
    fun resumeLiveSync() {
        if (config.liveSync) {
            collections.forEach { sseListener.start(it, since = "now") }
        }
    }

    fun syncNow() {
        val request = OneTimeWorkRequestBuilder<SyncWorker>()
            .setConstraints(
                Constraints.Builder()
                    .setRequiredNetworkType(NetworkType.CONNECTED)
                    .build()
            ).build()
        WorkManager.getInstance(context).enqueue(request)
    }

    private fun startPeriodicSync() {
        val request = PeriodicWorkRequestBuilder<SyncWorker>(
            config.syncIntervalMinutes, TimeUnit.MINUTES
        ).setConstraints(
            Constraints.Builder()
                .setRequiredNetworkType(NetworkType.CONNECTED)
                .build()
        ).setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
            .build()

        WorkManager.getInstance(context).enqueueUniquePeriodicWork(
            WORK_NAME,
            ExistingPeriodicWorkPolicy.KEEP,
            request
        )
    }

    companion object {
        private const val WORK_NAME = "rxdroid_periodic_sync"
        val gson = Gson()

        @Volatile
        var instance: RxDroid? = null
            private set

        fun create(context: Context, config: RxDroidConfig): RxDroid {
            val db = RxDroidDatabase.getInstance(context)
            val client = CouchDbClient(config.url, config.username, config.password)
            return RxDroid(context.applicationContext, config, db, client)
                .also { instance = it }
        }
    }
}
