package com.genesis.rxdroid.sync.couch

import android.util.Log
import okhttp3.Credentials
import okhttp3.OkHttpClient
import okhttp3.logging.HttpLoggingInterceptor
import retrofit2.Response
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.*
import java.util.concurrent.TimeUnit

interface CouchDbApi {

    // Sin include_docs para docs grandes: primero trae solo IDs y seqs
    @GET("{db}/_changes")
    suspend fun getChanges(
        @Path("db") db: String,
        @Query("since") since: String = "0",
        @Query("limit") limit: Int = 20,
        @Query("include_docs") includeDocs: Boolean = false,
        @Query("feed") feed: String = "normal"
    ): ChangesResponse

    // Trae docs individuales cuando se necesita el contenido
    @GET("{db}/_all_docs")
    suspend fun getDocs(
        @Path("db") db: String,
        @Body keys: AllDocsRequest,
        @Query("include_docs") includeDocs: Boolean = true
    ): AllDocsResponse

    @POST("{db}/_bulk_docs")
    suspend fun bulkDocs(
        @Path("db") db: String,
        @Body body: BulkDocsRequest
    ): List<BulkDocsResult>

    @GET("{db}/_local/{checkpointId}")
    suspend fun getCheckpoint(
        @Path("db") db: String,
        @Path("checkpointId") id: String
    ): Response<CheckpointDoc>

    @PUT("{db}/_local/{checkpointId}")
    suspend fun saveCheckpoint(
        @Path("db") db: String,
        @Path("checkpointId") id: String,
        @Body checkpoint: CheckpointDoc
    ): Response<BulkDocsResult>

    @GET("{db}/{docId}")
    suspend fun getDoc(
        @Path("db") db: String,
        @Path("docId") docId: String
    ): Response<Map<String, Any?>>
}

private fun isDebugBuild(): Boolean =
    android.os.Build.TYPE == "userdebug" || android.os.Build.TYPE == "eng"

class CouchDbClient(
    baseUrl: String,
    username: String,
    password: String
) {
    val api: CouchDbApi
    lateinit var httpClient: OkHttpClient  // expuesto para SSE listener

    init {
        val credentials = Credentials.basic(username, password)

        httpClient = OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(120, TimeUnit.SECONDS)   // docs grandes necesitan más tiempo
            .writeTimeout(120, TimeUnit.SECONDS)
            .addInterceptor { chain ->
                val request = chain.request().newBuilder()
                    .header("Authorization", credentials)
                    .header("Accept", "application/json")
                    .header("Accept-Encoding", "gzip")  // compresión automática
                    .build()
                chain.proceed(request)
            }
            .apply {
                if (isDebugBuild()) {
                    addInterceptor(HttpLoggingInterceptor().apply {
                        level = HttpLoggingInterceptor.Level.BASIC
                    })
                }
            }
            .build()
            .also { httpClient = it }

        api = Retrofit.Builder()
            .baseUrl(baseUrl)
            .client(httpClient)
            .addConverterFactory(GsonConverterFactory.create())
            .build()
            .create(CouchDbApi::class.java)
    }
}
