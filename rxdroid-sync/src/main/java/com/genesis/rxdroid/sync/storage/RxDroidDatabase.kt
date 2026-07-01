package com.genesis.rxdroid.sync.storage

import android.content.Context
import androidx.room.Database
import androidx.room.Room
import androidx.room.RoomDatabase
import java.io.File
import java.util.concurrent.ConcurrentHashMap

@Database(entities = [DocumentEntity::class], version = 1, exportSchema = false)
abstract class RxDroidDatabase : RoomDatabase() {

    abstract fun documentDao(): DocumentDao

    companion object {
        private val instances = ConcurrentHashMap<String, RxDroidDatabase>()

        fun getInstance(context: Context, dbName: String): RxDroidDatabase {
            return instances[dbName] ?: synchronized(this) {
                instances[dbName] ?: run {
                    // Migración: renombrar rxdroid.db (formato anterior) → rxdroid_$dbName.db
                    val legacy = context.getDatabasePath("rxdroid.db")
                    val target = context.getDatabasePath("rxdroid_$dbName.db")
                    if (legacy.exists() && !target.exists()) {
                        legacy.renameTo(target)
                        File("${legacy.path}-wal").takeIf { it.exists() }?.renameTo(File("${target.path}-wal"))
                        File("${legacy.path}-shm").takeIf { it.exists() }?.renameTo(File("${target.path}-shm"))
                    }

                    Room.databaseBuilder(
                        context.applicationContext,
                        RxDroidDatabase::class.java,
                        "rxdroid_$dbName.db"
                    ).build().also { instances[dbName] = it }
                }
            }
        }
    }
}
