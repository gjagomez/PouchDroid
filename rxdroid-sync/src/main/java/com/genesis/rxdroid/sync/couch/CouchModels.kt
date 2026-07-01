package com.genesis.rxdroid.sync.couch

import com.google.gson.annotations.SerializedName

// last_seq es String en CouchDB 2.x (cluster) y número en CouchDB 3.x (single-node)
class ChangesResponse(
    val results: List<ChangeRow>,
    @SerializedName("last_seq") private val lastSeqRaw: Any = "0"
) {
    val lastSeq: String get() = when (val v = lastSeqRaw) {
        is Double -> v.toLong().toString()
        is Long   -> v.toString()
        is Int    -> v.toString()
        else      -> v.toString()
    }
}

data class ChangeRow(
    val id: String,
    // seq puede ser String (CouchDB 2.x) o número (CouchDB 1.x) — no lo usamos, Gson lo ignora
    val deleted: Boolean = false,
    val doc: Map<String, Any?>? = null
)

data class BulkDocsRequest(
    val docs: List<Map<String, Any?>>
)

data class BulkDocsResult(
    val id: String,
    val rev: String? = null,
    val error: String? = null,
    val reason: String? = null
)

data class AllDocsRequest(val keys: List<String>)

data class AllDocsResponse(val rows: List<AllDocsRow>)

data class AllDocsRow(
    val id: String,
    val key: String,
    val error: String? = null,
    val doc: Map<String, Any?>? = null
)

data class CheckpointDoc(
    @SerializedName("_id") val id: String,
    @SerializedName("_rev") val rev: String? = null,
    @SerializedName("last_seq") val lastSeq: String = "0"
)
