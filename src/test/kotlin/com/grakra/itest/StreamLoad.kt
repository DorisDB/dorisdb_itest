package com.grakra.itest

import com.google.gson.Gson
import junit.framework.Assert
import org.apache.http.client.methods.HttpPut
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.HttpClients
import java.util.*

object StreamLoad {

    fun streamLoad(
            host: String,
            port: Int,
            db: String,
            table: String,
            columnSeparator:String,
            data: ByteArray
    ): Boolean {
        return streamLoad(
                host,
                port,
                "root",
                "",
                "${db}_${table}_label_${System.currentTimeMillis()}",
                columnSeparator,
                db,
                table,
                emptyList(),
                data)
    }

    fun streamLoad(
            host: String,
            port: Int,
            user: String,
            password: String,
            label: String,
            columnSeparator: String,
            db: String,
            table: String,
            headers: List<Pair<String, String>>,
            data: ByteArray
    ): Boolean {
        val principal = String(Base64.getEncoder().encode("$user:$password".toByteArray()))
        val path = "/api/$db/${table}/_stream_load"
        val httpClient = HttpClients.custom().build()
        val uri = URIBuilder().setScheme("http").setHost(host).setPort(port).setPath(path).build()
        val put = HttpPut(uri)
        put.addHeader("label", label)
        put.addHeader("Authorization", "Basic $principal")
        put.addHeader("column_separator", columnSeparator)
        headers.forEach { (k, v) ->
            put.addHeader(k, v)
        }
        put.entity = ByteArrayEntity(data, ContentType.TEXT_PLAIN)
        val response = httpClient.execute(put)!!
        val statusCode = response.statusLine.statusCode
        val responseContent = response.entity.content.bufferedReader().readText()
        val result = Gson().fromJson<Map<String, Object>>(responseContent, Map::class.java)!!
        println("=====STREAM_LOAD=====")
        val headerString = """
      -H "label:$label" -H "column_separator:$columnSeparator"  ${headers.map { (k, v) -> "-H \" $k:$v\"" }.joinToString(" ")}
    """.trimIndent()

        println(
                """
          cat <<'DONE' |curl --location-trusted -u $user:$password $headerString -T - $uri
          ${String(data).split("\n").take(10).joinToString("\n")}
          DONE
        """.split("\n").map { it.trim() }.joinToString("\n"))
        println("=====================")
        println("$path\nresponseContent=$responseContent")
        return statusCode in (200..299) && result.containsKey("Status") && result["Status"] as String == "Success"
    }
}