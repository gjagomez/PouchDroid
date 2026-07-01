package com.genesis.rxdroid.sync

import android.content.Context
import java.util.UUID

object DeviceId {
    private const val PREFS = "rxdroid_prefs"
    private const val KEY = "device_id"

    @Synchronized
    fun get(context: Context): String {
        val prefs = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
        return prefs.getString(KEY, null) ?: UUID.randomUUID().toString().also { id ->
            prefs.edit().putString(KEY, id).commit()
        }
    }
}
