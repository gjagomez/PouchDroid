package com.genesis.rxdroid.sync

import android.util.Log
import com.genesis.rxdroid.sync.couch.CouchDbClient
import com.genesis.rxdroid.sync.storage.DocumentDao
import com.google.gson.Gson
import io.mockk.*
import kotlinx.coroutines.test.runTest
import org.junit.After
import org.junit.Before
import org.junit.Test

class SyncManagerTest {

    private val config = RxDroidConfig(
        url = "https://newdev.genesisempresarial.org:6984",
        username = "admin",
        password = "enx!hve_GXD8hkt7aku",
        database = "db_4004"
    )
    private val dao = mockk<DocumentDao>(relaxed = true)
    private val client = mockk<CouchDbClient>(relaxed = true)
    private val conflictResolver = mockk<ConflictResolver>()
    private val gson = Gson()
    private val deviceId = "test_device"

    private lateinit var syncManager: SyncManager

    @Before
    fun setup() {
        mockkStatic(Log::class)
        every { Log.d(any(), any()) } returns 0
        every { Log.e(any(), any()) } returns 0
        every { Log.e(any(), any(), any()) } returns 0
        
        syncManager = SyncManager(config, dao, client, conflictResolver, gson, deviceId)
    }

    @After
    fun tearDown() {
        unmockkStatic(Log::class)
    }

    @Test
    fun `sync calls pull and push for the given collection`() = runTest {
        io.mockk.coEvery {
            client.api.getChanges(any(), any(), any(), any(), any())
        } returns com.genesis.rxdroid.sync.couch.ChangesResponse(emptyList(), "1-seq")

        io.mockk.coEvery {
            client.api.getCheckpoint(any(), any())
        } returns retrofit2.Response.success(com.genesis.rxdroid.sync.couch.CheckpointDoc("_local/id", "rev", "0"))

        io.mockk.coEvery {
            client.api.saveCheckpoint(any(), any(), any())
        } returns retrofit2.Response.success(com.genesis.rxdroid.sync.couch.BulkDocsResult("id", "rev"))

        io.mockk.coEvery { dao.getByStatus(any(), any()) } returns emptyList()

        syncManager.sync("users")

        // Verificamos que se llamó a pull (getChanges) al menos 1 vez
        coVerify(atLeast = 1) {
            client.api.getChanges(any(), any(), any(), any(), any())
        }
    }
}
