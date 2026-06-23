package com.genesis.rxdroid.sync

import com.genesis.rxdroid.sync.storage.DocumentDao
import com.genesis.rxdroid.sync.storage.DocumentEntity
import com.genesis.rxdroid.sync.storage.SyncStatus
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import java.util.UUID

class RxCollection<T> internal constructor(
    val name: String,
    private val deserialize: (String) -> T?,
    private val serialize: (T) -> String,
    private val extractId: (T) -> String?,
    private val dao: DocumentDao,
    private val gson: Gson
) {
    private val mapType = object : TypeToken<Map<String, Any?>>() {}.type

    // Observa todos los documentos como Flow reactivo
    fun findAll(): Flow<List<T>> {
        return dao.observeAll(name).map { entities ->
            entities.mapNotNull { deserialize(it.data) }
        }
    }

    // Observa un documento por ID
    fun findById(id: String): Flow<T?> {
        return dao.observeById("$name|$id").map { entity ->
            entity?.let { deserialize(it.data) }
        }
    }

    // Inserta un documento
    suspend fun insert(doc: T, id: String? = null): String {
        val json = serialize(doc)
        val docId = id
            ?: extractId(doc)
            ?: UUID.randomUUID().toString()

        dao.upsert(
            DocumentEntity(
                compositeId = "$name|$docId",
                collection = name,
                docId = docId,
                data = json,
                updatedAt = System.currentTimeMillis(),
                syncStatus = SyncStatus.PENDING
            )
        )
        return docId
    }

    // Actualiza un documento existente
    suspend fun update(id: String, doc: T) {
        val existing = dao.getById("$name|$id")
        dao.upsert(
            DocumentEntity(
                compositeId = "$name|$id",
                collection = name,
                docId = id,
                data = serialize(doc),
                rev = existing?.rev,
                updatedAt = System.currentTimeMillis(),
                syncStatus = SyncStatus.PENDING
            )
        )
    }

    // Elimina (soft delete) un documento
    suspend fun delete(id: String) {
        dao.softDelete("$name|$id", System.currentTimeMillis())
    }

    // Obtiene todos los documentos una sola vez
    suspend fun getAll(): List<T> {
        return dao.getAll(name).mapNotNull { deserialize(it.data) }
    }

    // Obtiene un documento por ID una sola vez
    suspend fun getById(id: String): T? {
        val entity = dao.getById("$name|$id") ?: return null
        return deserialize(entity.data)
    }

    companion object {
        private val mapType = object : TypeToken<Map<String, Any?>>() {}.type

        // Modo dinámico: sin data class, trabaja con RxDocument
        fun dynamic(name: String, dao: DocumentDao, gson: Gson): RxCollection<RxDocument> {
            return RxCollection(
                name = name,
                deserialize = { json ->
                    runCatching {
                        val map: Map<String, Any?> = gson.fromJson(json, mapType)
                        RxDocument(map)
                    }.getOrNull()
                },
                serialize = { doc -> doc.toJson() },
                extractId = { doc -> doc.id.ifBlank { null } },
                dao = dao,
                gson = gson
            )
        }

        // Modo tipado: con data class específica
        fun <T> typed(name: String, type: Class<T>, dao: DocumentDao, gson: Gson): RxCollection<T> {
            return RxCollection(
                name = name,
                deserialize = { json -> runCatching { gson.fromJson(json, type) }.getOrNull() },
                serialize = { doc -> gson.toJson(doc) },
                extractId = { doc ->
                    runCatching {
                        val map: Map<String, Any?> = gson.fromJson(gson.toJson(doc), mapType)
                        (map["id"] as? String) ?: (map["_id"] as? String)
                    }.getOrNull()
                },
                dao = dao,
                gson = gson
            )
        }
    }
}
