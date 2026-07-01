package com.genesis.rxdroid.sync

import android.content.Context
import android.util.Log
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters
import com.genesis.rxdroid.sync.storage.RxDroidDatabase

private const val TAG = "RxDroid-Worker"

class SyncWorker(
    context: Context,
    params: WorkerParameters
) : CoroutineWorker(context, params) {

    override suspend fun doWork(): Result {
        val rxDroid = resolveRxDroid() ?: run {
            Log.e(TAG, "No se pudo inicializar RxDroid — inputData incompleto y sin instancia activa")
            return Result.failure()
        }

        if (rxDroid.registeredCollections.isEmpty()) {
            val db = RxDroidDatabase.getInstance(applicationContext, rxDroid.config.database)
            db.documentDao().getDistinctCollections().forEach { rxDroid.collection(it) }
        }

        if (rxDroid.registeredCollections.isEmpty()) {
            Log.w(TAG, "Sin colecciones registradas, nada que sincronizar")
            return Result.success()
        }

        return try {
            Log.d(TAG, "Background sync iniciado (cols=${rxDroid.registeredCollections})")
            rxDroid.syncAll()
            Log.d(TAG, "Background sync completado")
            Result.success()
        } catch (e: Exception) {
            Log.e(TAG, "Background sync fallido: ${e.message}")
            if (runAttemptCount < 3) Result.retry() else Result.failure()
        }
    }

    private fun resolveRxDroid(): RxDroid? {
        // Campos mínimos requeridos — el resto tiene defaults seguros
        val database = inputData.getString(KEY_DATABASE) ?: return null
        val url      = inputData.getString(KEY_URL)      ?: return null
        val username = inputData.getString(KEY_USERNAME) ?: return null
        val password = inputData.getString(KEY_PASSWORD) ?: return null

        // Primero intentar la instancia viva en memoria para esta DB
        RxDroid.getInstance(database)?.let { return it }

        Log.d(TAG, "Proceso muerto — recreando RxDroid desde inputData (db=$database)")

        return RxDroid.create(
            applicationContext,
            RxDroidConfig(
                url                  = url,
                username             = username,
                password             = password,
                database             = database,
                syncIntervalMinutes  = inputData.getLong(KEY_SYNC_INTERVAL, 15L),
                liveSync             = inputData.getBoolean(KEY_LIVE_SYNC, false),
                backgroundSync       = inputData.getBoolean(KEY_BACKGROUND_SYNC, true),
                batchSize            = inputData.getInt(KEY_BATCH_SIZE, 100),
                debug                = inputData.getBoolean(KEY_DEBUG, false)
            )
        )
    }

    companion object {
        const val KEY_URL             = "rxdroid_url"
        const val KEY_USERNAME        = "rxdroid_username"
        const val KEY_PASSWORD        = "rxdroid_password"
        const val KEY_DATABASE        = "rxdroid_database"
        const val KEY_SYNC_INTERVAL   = "rxdroid_sync_interval"
        const val KEY_LIVE_SYNC       = "rxdroid_live_sync"
        const val KEY_BACKGROUND_SYNC = "rxdroid_background_sync"
        const val KEY_BATCH_SIZE      = "rxdroid_batch_size"
        const val KEY_DEBUG           = "rxdroid_debug"
    }
}
