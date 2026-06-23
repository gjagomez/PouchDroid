package com.genesis.rxdroid.sync.storage

import android.content.Context
import androidx.room.Database
import androidx.room.Room
import androidx.room.RoomDatabase

@Database(entities = [DocumentEntity::class], version = 1, exportSchema = false)
abstract class RxDroidDatabase : RoomDatabase() {

    abstract fun documentDao(): DocumentDao

    companion object {
        @Volatile
        private var instance: RxDroidDatabase? = null

        fun getInstance(context: Context): RxDroidDatabase {
            return instance ?: synchronized(this) {
                instance ?: Room.databaseBuilder(
                    context.applicationContext,
                    RxDroidDatabase::class.java,
                    "rxdroid.db"
                ).build().also { instance = it }
            }
        }
    }
}
