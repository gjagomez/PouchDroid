package com.genesis.rxdroid.sync

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

class RxDocument(private val raw: Map<String, Any?>) {

    val id: String get() = getString("_id") ?: ""
    val rev: String? get() = getString("_rev")

    fun getString(key: String): String? = raw[key] as? String

    fun getInt(key: String): Int? = when (val v = raw[key]) {
        is Double -> v.toInt()
        is Int -> v
        is String -> v.toIntOrNull()
        else -> null
    }

    fun getDouble(key: String): Double? = when (val v = raw[key]) {
        is Double -> v
        is Int -> v.toDouble()
        is String -> v.toDoubleOrNull()
        else -> null
    }

    fun getBoolean(key: String): Boolean? = raw[key] as? Boolean

    fun getMap(key: String): Map<String, Any?>? =
        @Suppress("UNCHECKED_CAST")
        raw[key] as? Map<String, Any?>

    fun getList(key: String): List<Any?>? =
        @Suppress("UNCHECKED_CAST")
        raw[key] as? List<Any?>

    // Devuelve el campo como otro RxDocument (para campos anidados)
    fun getNested(key: String): RxDocument? {
        val map = getMap(key) ?: return null
        return RxDocument(map)
    }

    // Devuelve el campo data.data (que en tu caso es JSON dentro de String)
    fun getDataAsDocument(key: String): RxDocument? {
        val str = getString(key) ?: return null
        return runCatching {
            val type = object : TypeToken<Map<String, Any?>>() {}.type
            val map: Map<String, Any?> = Gson().fromJson(str, type)
            RxDocument(map)
        }.getOrNull()
    }

    fun get(key: String): Any? = raw[key]

    fun has(key: String): Boolean = raw.containsKey(key)

    fun toMap(): Map<String, Any?> = raw

    fun toJson(): String = Gson().toJson(raw)

    override fun toString(): String = toJson()
}
