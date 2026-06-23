package com.genesis.rxdroid

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.genesis.rxdroid.sync.RxDroid
import com.genesis.rxdroid.sync.RxDroidConfig
import com.genesis.rxdroid.ui.theme.RxDroidTheme

class MainActivity : ComponentActivity() {

    private val db by lazy {
        RxDroid.create(
            context = this,
            config = RxDroidConfig(
                url = "https://newdev.genesisempresarial.org:6984/",
                username = "admin",
                password = "tu_password",
                database = "formularios"
            )
        )
    }

    // Modo dinámico: sin data class, funciona con cualquier formId
    private val formularios by lazy { db.collection("formularios") }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        db.startSync()

        setContent {
            RxDroidTheme {
                val lista by formularios.findAll().collectAsState(initial = emptyList())

                Scaffold(modifier = Modifier.fillMaxSize()) { innerPadding ->
                    LazyColumn(
                        modifier = Modifier
                            .fillMaxSize()
                            .padding(innerPadding)
                            .padding(16.dp),
                        verticalArrangement = Arrangement.spacedBy(8.dp)
                    ) {
                        item {
                            Text(
                                "Formularios (${lista.size})",
                                style = MaterialTheme.typography.headlineSmall
                            )
                            Spacer(Modifier.height(8.dp))
                        }
                        items(lista) { doc ->
                            // Acceder a campos sin data class fija
                            val promotor = doc.getString("promotor") ?: "-"
                            val formId   = doc.getInt("formId") ?: 0
                            val status   = doc.getString("status") ?: "-"

                            // Acceder a campos anidados (doc.data)
                            val dataNested  = doc.getNested("data")
                            val statusLocal = dataNested?.getString("status_local") ?: "-"

                            // Acceder al JSON doble-encoded (data.data)
                            val dataInterna = dataNested?.getDataAsDocument("data")
                            val cliente     = dataInterna?.getString("PRIMER_NOMBRE") ?: "-"
                            val monto       = dataInterna?.getDouble("MONTO_SOLICITADO") ?: 0.0

                            Card(modifier = Modifier.fillMaxWidth()) {
                                Column(modifier = Modifier.padding(16.dp)) {
                                    Text("Promotor: $promotor | Form: $formId")
                                    Text("Cliente: $cliente")
                                    Text("Monto: Q$monto")
                                    Text(
                                        "Estado: $status / $statusLocal",
                                        color = when (status) {
                                            "Enviado" -> MaterialTheme.colorScheme.primary
                                            else -> MaterialTheme.colorScheme.error
                                        }
                                    )
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private var lastResumeSync = 0L

    override fun onResume() {
        super.onResume()
        // Reanuda SSE en tiempo real al volver a primer plano
        db.resumeLiveSync()

        // Sync completo si pasaron más de 5 minutos
        val now = System.currentTimeMillis()
        if (now - lastResumeSync > 5 * 60 * 1000L) {
            lastResumeSync = now
            db.syncNow()
        }
    }

    override fun onPause() {
        super.onPause()
        // Pausa SSE cuando la app va a background (ahorra batería)
        db.pauseLiveSync()
    }
}
