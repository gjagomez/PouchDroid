package com.genesis.rxdroid.sync

import android.content.Context
import androidx.work.*
import com.genesis.rxdroid.sync.couch.CouchDbClient
import com.genesis.rxdroid.sync.storage.RxDroidDatabase
import com.google.gson.Gson
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.map
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class RxDroid private constructor(
    private val context: Context,
    val config: RxDroidConfig,
    private val db: RxDroidDatabase,
    private val couchClient: CouchDbClient
) {
    private val collections: MutableSet<String> = Collections.synchronizedSet(LinkedHashSet())

    val registeredCollections: Set<String> get() = collections

    private val _lastSyncError = MutableStateFlow<String?>(null)
    val lastSyncError: StateFlow<String?> = _lastSyncError

    private val deviceId = DeviceId.get(context)

    private val syncManager = SyncManager(
        config = config,
        dao = db.documentDao(),
        client = couchClient,
        conflictResolver = ConflictResolver(gson),
        gson = gson,
        deviceId = deviceId
    )

    private val changeWatcher = ChangeWatcher(
        context = context,
        syncManager = syncManager,
        collections = collections,
        onError = { error -> _lastSyncError.value = error }
    )

    private val sseListener = CouchDbSSEListener(
        config = config,
        dao = db.documentDao(),
        client = couchClient,
        gson = gson
    )

    // Nombre único del trabajo periódico de WorkManager para esta base de datos
    private val workName = "${WORK_NAME_PREFIX}_${config.database}"
    // Nombre único del trabajo puntual (syncNow) — permite cancelarlo en stopSync()
    private val workNameOnce = "${WORK_NAME_PREFIX}_once_${config.database}"

    fun collection(name: String): RxCollection<RxDocument> {
        collections.add(name)
        return RxCollection.dynamic(name, db.documentDao(), gson)
    }

    fun <T> collection(name: String, type: Class<T>): RxCollection<T> {
        collections.add(name)
        return RxCollection.typed(name, type, db.documentDao(), gson)
    }

    suspend fun syncAll() {
        collections.forEach { syncManager.sync(it) }
    }

    fun startSync() {
        if (config.liveSync) {
            collections.forEach { sseListener.start(it, since = "now") }
        }
        val pendingFlow = db.documentDao().observePendingCount().map { Unit }
        changeWatcher.watchLocalChanges(pendingFlow)
        changeWatcher.watchNetwork()
        if (config.backgroundSync) startPeriodicSync()
    }

    fun stopSync() {
        sseListener.destroy()
        changeWatcher.stop()
        val wm = WorkManager.getInstance(context)
        wm.cancelUniqueWork(workName)
        wm.cancelUniqueWork(workNameOnce)
    }

    fun pauseLiveSync() {
        sseListener.stop()
    }

    fun resumeLiveSync() {
        if (!config.liveSync) return
        syncNow()
        collections.forEach { sseListener.start(it) }
    }

    fun syncNow() {
        val request = OneTimeWorkRequestBuilder<SyncWorker>()
            .setInputData(buildInputData())
            .setConstraints(buildConstraints())
            .build()
        WorkManager.getInstance(context).enqueueUniqueWork(
            workNameOnce,
            ExistingWorkPolicy.REPLACE,
            request
        )
    }

    fun enableBackgroundSync() {
        startPeriodicSync()
    }

    fun disableBackgroundSync() {
        WorkManager.getInstance(context).cancelUniqueWork(workName)
    }

    private fun startPeriodicSync() {
        val request = PeriodicWorkRequestBuilder<SyncWorker>(
            config.syncIntervalMinutes, TimeUnit.MINUTES
        ).setInputData(buildInputData())
            .setConstraints(buildConstraints())
            .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
            .build()

        WorkManager.getInstance(context).enqueueUniquePeriodicWork(
            workName,
            ExistingPeriodicWorkPolicy.UPDATE,
            request
        )
    }

    private fun buildConstraints() = Constraints.Builder()
        .setRequiredNetworkType(NetworkType.CONNECTED)
        .build()

    private fun buildInputData(): Data = Data.Builder()
        .putString(SyncWorker.KEY_URL, config.url)
        .putString(SyncWorker.KEY_USERNAME, config.username)
        .putString(SyncWorker.KEY_PASSWORD, config.password)
        .putString(SyncWorker.KEY_DATABASE, config.database)
        .putLong(SyncWorker.KEY_SYNC_INTERVAL, config.syncIntervalMinutes)
        .putBoolean(SyncWorker.KEY_LIVE_SYNC, config.liveSync)
        .putBoolean(SyncWorker.KEY_BACKGROUND_SYNC, config.backgroundSync)
        .putInt(SyncWorker.KEY_BATCH_SIZE, config.batchSize)
        .putBoolean(SyncWorker.KEY_DEBUG, config.debug)
        .build()

    companion object {
        private const val WORK_NAME_PREFIX = "rxdroid_sync"
        val gson = Gson()

        private val instances = ConcurrentHashMap<String, RxDroid>()

        fun getInstance(database: String): RxDroid? = instances[database]

        fun create(context: Context, config: RxDroidConfig): RxDroid {
            val key = config.database
            return instances[key] ?: synchronized(this) {
                instances[key] ?: run {
                    val db = RxDroidDatabase.getInstance(context, key)
                    val client = CouchDbClient(config.url, config.username, config.password, config.debug)
                    RxDroid(context.applicationContext, config, db, client)
                        .also { instances[key] = it }
                }
            }
        }
    }
}
