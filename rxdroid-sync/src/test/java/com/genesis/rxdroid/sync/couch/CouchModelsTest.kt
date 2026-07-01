package com.genesis.rxdroid.sync.couch

import com.google.gson.Gson
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Test

class CouchModelsTest {
    private val gson = Gson()

    @Test
    fun `ChangesResponse deserialization works correctly`() {
        val json = """
            {
                "results": [
                    {
                        "id": "doc1",
                        "seq": "1-abc",
                        "deleted": false
                    }
                ],
                "last_seq": "1-abc"
            }
        """.trimIndent()

        val response = gson.fromJson(json, ChangesResponse::class.java)

        assertEquals("1-abc", response.lastSeq)
        assertEquals(1, response.results.size)
        assertEquals("doc1", response.results[0].id)
    }

    @Test
    fun `CheckpointDoc serialization uses underscore prefix for id and rev`() {
        val checkpoint = CheckpointDoc(id = "check1", rev = "1-rev", lastSeq = "100")
        val json = gson.toJson(checkpoint)

        // Check if @SerializedName works
        assert(json.contains("\"_id\":\"check1\""))
        assert(json.contains("\"_rev\":\"1-rev\""))
        assert(json.contains("\"last_seq\":\"100\""))
    }

    @Test
    fun `CheckpointDoc deserialization handles underscore prefix`() {
        val json = """
            {
                "_id": "check1",
                "_rev": "1-rev",
                "last_seq": "100"
            }
        """.trimIndent()

        val checkpoint = gson.fromJson(json, CheckpointDoc::class.java)

        assertEquals("check1", checkpoint.id)
        assertEquals("1-rev", checkpoint.rev)
        assertEquals("100", checkpoint.lastSeq)
    }

    @Test
    fun `ChangeRow doc field is parsed correctly`() {
        val json = """
            {
                "id": "doc1",
                "seq": "1",
                "doc": {
                    "name": "test",
                    "value": 42
                }
            }
        """.trimIndent()

        val row = gson.fromJson(json, ChangeRow::class.java)

        assertNotNull(row.doc)
        assertEquals("test", row.doc!!["name"])
        assertEquals(42.0, row.doc!!["value"])
    }
}
