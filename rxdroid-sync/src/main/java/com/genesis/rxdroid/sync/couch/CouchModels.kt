package com.genesis.rxdroid.sync.couch

import com.google.gson.annotations.SerializedName

data class ChangesResponse(
    val results: List<ChangeRow>,
    @SerializedName("last_seq") val lastSeq: String
)

data class ChangeRow(
    val id: String,
    val seq: String,
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
