package com.genesis.rxdroid.sync

import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.genesis.rxdroid.sync.couch.CouchDbClient
import com.genesis.rxdroid.sync.storage.RxDroidDatabase
import com.google.gson.Gson
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import java.util.UUID

@RunWith(AndroidJUnit4::class)
class CouchDbIntegrationTest {

    private lateinit var db: RxDroidDatabase
    private lateinit var syncManager: SyncManager
    
    // --- CONFIGURA TUS CREDENCIALES AQUÍ ---
    private val CONFIG = RxDroidConfig(
        url = "https://newdev.genesisempresarial.org:6984", // 10.0.2.2 es localhost desde el emulador
        username = "admin",
        password = "enx!hve_GXD8hkt7aku",
        database = "db_4004"
    )
    // ---------------------------------------

    @Before
    fun setup() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        db = Room.inMemoryDatabaseBuilder(context, RxDroidDatabase::class.java).build()
        
        val client = CouchDbClient(CONFIG.url, CONFIG.username, CONFIG.password)
        val gson = Gson()
        val deviceId = "test_device_${UUID.randomUUID()}"
        val conflictResolver = ConflictResolver(gson)

        syncManager = SyncManager(CONFIG, db.documentDao(), client, conflictResolver, gson, deviceId)
    }

    @After
    fun tearDown() {
        db.close()
    }

    @Test
    fun testConnectionAndSync() = runBlocking {
        println("Iniciando prueba de integración con CouchDB en ${CONFIG.url}...")
        
        try {
            // Intentamos sincronizar una colección llamada "test" o cualquier que tengas
            syncManager.sync("test")
            
            val docs = db.documentDao().getAll("test")
            println("Sincronización finalizada. Documentos locales en Room: ${docs.size}")
            
            // Si no falló, pasamos el test
            assert(true)
        } catch (e: Exception) {
            println("FALLO EN INTEGRACIÓN: ${e.message}")
            e.printStackTrace()
            throw e
        }
    }
}
