package com.genesis.rxdroid.sync.storage

import androidx.room.*
import kotlinx.coroutines.flow.Flow

@Dao
interface DocumentDao {

    @Query("SELECT * FROM rx_documents WHERE collection = :collection AND deleted = 0")
    fun observeAll(collection: String): Flow<List<DocumentEntity>>

    @Query("SELECT * FROM rx_documents WHERE compositeId = :compositeId AND deleted = 0")
    fun observeById(compositeId: String): Flow<DocumentEntity?>

    @Query("SELECT * FROM rx_documents WHERE collection = :collection AND deleted = 0")
    suspend fun getAll(collection: String): List<DocumentEntity>

    @Query("SELECT * FROM rx_documents WHERE compositeId = :compositeId")
    suspend fun getById(compositeId: String): DocumentEntity?

    // Para push: incluye eliminados pendientes (necesitamos propagar el delete a CouchDB)
    @Query("SELECT * FROM rx_documents WHERE collection = :collection AND syncStatus = :status")
    suspend fun getByStatus(collection: String, status: String): List<DocumentEntity>

    // Para pull: solo docs vivos con PENDING (evita saltar updates remotos por un delete local)
    @Query("SELECT * FROM rx_documents WHERE collection = :collection AND syncStatus = :status AND deleted = 0")
    suspend fun getPendingNonDeleted(collection: String, status: String): List<DocumentEntity>

    @Upsert
    suspend fun upsert(doc: DocumentEntity)

    @Upsert
    suspend fun upsertAll(docs: List<DocumentEntity>)

    @Query("UPDATE rx_documents SET syncStatus = 'SYNCED', rev = :rev WHERE compositeId = :compositeId")
    suspend fun markSynced(compositeId: String, rev: String?)

    @Query("UPDATE rx_documents SET syncStatus = 'CONFLICT' WHERE compositeId = :compositeId")
    suspend fun markConflict(compositeId: String)

    @Query("UPDATE rx_documents SET deleted = 1, syncStatus = 'PENDING', updatedAt = :updatedAt WHERE compositeId = :compositeId")
    suspend fun softDelete(compositeId: String, updatedAt: Long)

    // Observa cualquier cambio en la tabla (para disparar push inmediato)
    @Query("SELECT COUNT(*) FROM rx_documents WHERE syncStatus = 'PENDING'")
    fun observePendingCount(): Flow<Int>

    // Colecciones que tienen al menos un documento — usado por SyncWorker cuando la app no está en memoria
    @Query("SELECT DISTINCT collection FROM rx_documents")
    suspend fun getDistinctCollections(): List<String>
}
