package com.genesis.rxdroid.sync

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import android.util.Log
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.debounce

private const val TAG = "RxDroid"
private const val DEBOUNCE_MS = 500L  // agrupa cambios rápidos en 500ms (igual que RxDB batch)
private const val RETRY_DELAY_MS = 5_000L  // 5 seg igual que RxDB retryTime

class ChangeWatcher(
    private val context: Context,
    private val syncManager: SyncManager,
    private val collections: Set<String>,
    private val onError: (String) -> Unit = {}
) {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var networkCallback: ConnectivityManager.NetworkCallback? = null
    private var networkReconnectJob: Job? = null

    // Observa cambios locales pendientes y los sube inmediatamente
    // changeFlow: Flow que emite cuando hay escrituras locales nuevas
    fun watchLocalChanges(changeFlow: Flow<Unit>) {
        scope.launch {
            changeFlow
                .debounce(DEBOUNCE_MS)  // agrupa rafagas de cambios como RxDB
                .collect {
                    if (isOnline()) {
                        pushWithRetry()
                    }
                    // Si no hay red, WorkManager lo subirá cuando vuelva
                }
        }
    }

    // Push inmediato con retry de 5s (igual que RxDB retryTime)
    private suspend fun pushWithRetry() {
        var lastError: Exception? = null
        var attempts = 0
        while (attempts < 3) {
            try {
                collections.forEach { syncManager.pushImmediate(it) }
                return
            } catch (e: Exception) {
                lastError = e
                attempts++
                Log.w(TAG, "Push attempt $attempts failed: ${e.message}")
                if (attempts < 3) delay(RETRY_DELAY_MS)
            }
        }
        lastError?.let { onError("Sync error: ${it.message}") }
    }

    // Detecta reconexión y sincroniza inmediatamente (como navigator.onLine en RxDB)
    fun watchNetwork() {
        val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager

        networkCallback = object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) {
                Log.d(TAG, "Network available — scheduling sync")
                // Cancela y re-agenda: descarta reconexiones rápidas en red inestable
                networkReconnectJob?.cancel()
                networkReconnectJob = scope.launch {
                    delay(2_000) // debounce: espera 2s a que la red estabilice
                    collections.forEach {
                        runCatching { syncManager.sync(it) }
                    }
                }
            }
        }

        val request = NetworkRequest.Builder()
            .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
            .build()

        cm.registerNetworkCallback(request, networkCallback!!)
    }

    fun stop() {
        scope.cancel()
        networkCallback?.let {
            val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
            runCatching { cm.unregisterNetworkCallback(it) }
        }
    }

    private fun isOnline(): Boolean {
        val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        val network = cm.activeNetwork ?: return false
        val caps = cm.getNetworkCapabilities(network) ?: return false
        return caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
    }
}
