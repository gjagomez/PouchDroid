package com.genesis.rxdroid

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.lifecycle.lifecycleScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Delete
import androidx.compose.material.icons.filled.Edit
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.genesis.rxdroid.sync.RxDroid
import com.genesis.rxdroid.sync.RxDroidConfig
import com.genesis.rxdroid.sync.RxDocument
import com.genesis.rxdroid.ui.theme.RxDroidTheme

class MainActivity : ComponentActivity() {

    private val db by lazy {
        RxDroid.create(
            context = this,
            config = RxDroidConfig(
                url = "https://newdev.genesisempresarial.org:6984",
                username = "admin",
                password = "enx!hve_GXD8hkt7aku",
                database = "db_4004",
                backgroundSync = true,   // <-- parámetro a probar
                debug = BuildConfig.DEBUG
            )
        )
    }

    // Cambia "test" por el nombre de la colección real que tengas en CouchDB
    private val myCollection by lazy { db.collection("test") }

    @OptIn(ExperimentalMaterial3Api::class)
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        db.startSync()

        setContent {
            RxDroidTheme {
                val lista by myCollection.findAll().collectAsState(initial = emptyList())
                var isSyncing by remember { mutableStateOf(false) }
                var bgSyncEnabled by remember { mutableStateOf(db.config.backgroundSync) }
                val snackbarHostState = remember { androidx.compose.material3.SnackbarHostState() }

                val scope = rememberCoroutineScope()

                // Muestra el error de sync en un Snackbar para diagnóstico
                val syncError by db.lastSyncError.collectAsState()
                LaunchedEffect(syncError) {
                    syncError?.let { snackbarHostState.showSnackbar(it, duration = androidx.compose.material3.SnackbarDuration.Long) }
                }

                Scaffold(
                    snackbarHost = { androidx.compose.material3.SnackbarHost(snackbarHostState) },
                    modifier = Modifier.fillMaxSize(),
                    topBar = {
                        TopAppBar(
                            title = { Text("RxDroid Sync") },
                            actions = {
                                Row(
                                    verticalAlignment = androidx.compose.ui.Alignment.CenterVertically
                                ) {
                                    Text(
                                        text = if (bgSyncEnabled) "BG: ON" else "BG: OFF",
                                        style = MaterialTheme.typography.labelSmall,
                                        color = if (bgSyncEnabled)
                                            MaterialTheme.colorScheme.primary
                                        else
                                            MaterialTheme.colorScheme.error
                                    )
                                    Switch(
                                        checked = bgSyncEnabled,
                                        onCheckedChange = { enabled ->
                                            bgSyncEnabled = enabled
                                            if (enabled) db.enableBackgroundSync()
                                            else db.disableBackgroundSync()
                                            scope.launch {
                                                val msg = if (enabled)
                                                    "Background sync activado (cada ${db.config.syncIntervalMinutes} min)"
                                                else
                                                    "Background sync desactivado"
                                                snackbarHostState.showSnackbar(msg)
                                            }
                                        },
                                        modifier = Modifier.padding(horizontal = 4.dp)
                                    )
                                    Button(onClick = {
                                        isSyncing = true
                                        db.syncNow()
                                        isSyncing = false
                                    }) {
                                        Text("Sync")
                                    }
                                }
                            }
                        )
                    },
                    floatingActionButton = {
                        FloatingActionButton(
                            onClick = {
                                scope.launch {
                                    val nuevoDoc = RxDocument(mapOf(
                                        "nombre" to "Prueba ${System.currentTimeMillis() % 1000}",
                                        "fecha" to java.util.Date().toString(),
                                        "tipo" to "test_manual"
                                    ))
                                    myCollection.insert(nuevoDoc)
                                    db.syncNow()
                                }
                            }
                        ) {
                            Text("+", style = MaterialTheme.typography.headlineMedium)
                        }
                    }
                ) { innerPadding ->
                    if (lista.isEmpty()) {
                        Box(modifier = Modifier.fillMaxSize().padding(innerPadding), contentAlignment = androidx.compose.ui.Alignment.Center) {
                            Text("No hay documentos en 'db_4004|test'")
                        }
                    } else {
                        LazyColumn(
                            modifier = Modifier
                                .fillMaxSize()
                                .padding(innerPadding)
                                .padding(16.dp),
                            verticalArrangement = Arrangement.spacedBy(8.dp)
                        ) {
                            item {
                                Text(
                                    "Documentos (${lista.size})",
                                    style = MaterialTheme.typography.headlineSmall
                                )
                                Spacer(Modifier.height(8.dp))
                            }
                            items(lista) { doc ->
                                Card(
                                    modifier = Modifier.fillMaxWidth(),
                                    colors = CardDefaults.cardColors(
                                        containerColor = MaterialTheme.colorScheme.surfaceVariant
                                    )
                                ) {
                                    Row(
                                        modifier = Modifier.padding(16.dp),
                                        verticalAlignment = androidx.compose.ui.Alignment.CenterVertically
                                    ) {
                                        Column(modifier = Modifier.weight(1f)) {
                                            Text(
                                                text = "ID: ${doc.id}",
                                                style = MaterialTheme.typography.labelLarge,
                                                color = MaterialTheme.colorScheme.primary
                                            )
                                            Spacer(Modifier.height(4.dp))
                                            
                                            // Mostramos el JSON formateado
                                            Text(
                                                text = doc.toJson(),
                                                style = MaterialTheme.typography.bodySmall,
                                                maxLines = 10
                                            )
                                        }
                                        
                                        IconButton(onClick = {
                                            scope.launch {
                                                val currentMap = doc.toMap().toMutableMap()
                                                currentMap["nombre"] = "Editado Localmente"
                                                currentMap["updatedAt"] = System.currentTimeMillis().toDouble()
                                                myCollection.update(doc.id, RxDocument(currentMap))
                                                db.syncNow()
                                            }
                                        }) {
                                            Icon(
                                                imageVector = Icons.Default.Edit,
                                                contentDescription = "Editar",
                                                tint = MaterialTheme.colorScheme.primary
                                            )
                                        }

                                        IconButton(onClick = {
                                            scope.launch {
                                                myCollection.delete(doc.id)
                                                db.syncNow()
                                            }
                                        }) {
                                            Icon(
                                                imageVector = Icons.Default.Delete,
                                                contentDescription = "Eliminar",
                                                tint = MaterialTheme.colorScheme.error
                                            )
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private var lastResumeSync = 0L
    private var backgroundTestJob: Job? = null

    override fun onResume() {
        super.onResume()
        backgroundTestJob?.cancel()
        backgroundTestJob = null

        db.resumeLiveSync()

        val now = System.currentTimeMillis()
        if (now - lastResumeSync > 5 * 60 * 1000L) {
            lastResumeSync = now
            db.syncNow()
        }
    }

    override fun onPause() {
        super.onPause()
        db.pauseLiveSync()

        var count = 0
        backgroundTestJob = lifecycleScope.launch {
            while (true) {
                count++
                myCollection.insert(RxDocument(mapOf(
                    "nombre"    to "background_$count",
                    "updatedAt" to System.currentTimeMillis().toDouble()
                )))
                db.syncNow()
                delay(10_000)
            }
        }
    }
}
