package com.grakra.dorisdb

import com.google.gson.Gson
import com.grakra.util.HouseKeeper
import com.grakra.util.MySQLUtil
import com.grakra.util.Result
import com.grakra.util.Util
import com.mysql.jdbc.Driver
import io.netty.util.concurrent.Promise
import junit.framework.Assert
import org.apache.http.client.methods.HttpPut
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.HttpClients
import org.springframework.jdbc.core.JdbcTemplate
import java.io.File
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import java.util.regex.Pattern
import kotlin.concurrent.thread
import kotlin.math.abs

class DorisDBCluster(private val dorisDockerDir: String, val shellCmdDir: String) {
    val dorisdbScript = dorisDockerDir + File.separator + "dorisdb.sh"

    init {
        Assert.assertTrue(File(dorisDockerDir).isDirectory)
        Assert.assertTrue(File(dorisdbScript).isFile)
    }

    companion object {
        val start_doris_cluster = "start_doris_cluster"
        val stop_doris_cluster = "stop_doris_cluster"
        val bootstrap_doris_cluster = "bootstrap_doris_cluster"
    }

    val feConf = Properties()
    val beConf = Properties()
    val feConfFile = dorisDockerDir + File.separator + "doris_fe_conf/fe.conf"
    val beConfFile = dorisDockerDir + File.separator + "doris_be_conf/be.conf"

    init {
        feConf.load(File(feConfFile).inputStream())
        beConf.load(File(beConfFile).inputStream())
    }


    val hostsFile = dorisDockerDir + File.separator + "hosts"
    val services = mutableMapOf<String, DorisService>()
    val fes = mutableMapOf<String, DorisService>()
    val feFollowers = mutableMapOf<String, DorisService>()
    val feObservers = mutableMapOf<String, DorisService>()
    val bes = mutableMapOf<String, DorisService>()

    init {
        val pat = Pattern.compile("^\\s*(\\d+(?:\\.\\d+){3})\\s*\\b((?:doris_fe_follower|doris_fe_observer|doris_be)\\d+)\\b")!!
        Util.readFile(File(hostsFile)).forEach { line ->
            pat.matcher(line).let { m ->
                if (m.matches()) {
                    val ip = m.group(1)!!
                    val name = m.group(2)!!
                    when {
                        name.startsWith("doris_fe_follower") -> {
                            val fe = DorisService(name, ip, DorisServiceType.FE)
                            fes[name] = fe
                            feFollowers[name] = fe
                            services[name] = fe
                        }
                        name.startsWith("doris_fe_observer") -> {
                            val fe = DorisService(name, ip, DorisServiceType.FE)
                            fes[name] = fe
                            feObservers[name] = fe
                            services[name] = fe
                        }
                        name.startsWith("doris_be") -> {
                            val be = DorisService(name, ip, DorisServiceType.BE)
                            bes[name] = be
                            services[name] = be
                        }
                    }
                }
            }
        }
    }


    fun start_doris_fe(vararg names: String) {
        names.forEach { name ->
            ShellCmd(shellCmdDir).run(dorisdbScript, "start_doris_fe", name)
        }
        names.forEach { name ->
            ensure_doris_fe_started(name)
        }
    }

    fun ensure_doris_fe_started(name: String) {
        val host = fes[name]!!.ip
        val port = feConf.getProperty("query_port", "9030").toInt()
        Util.ensurePortAlive(host, port, "DorisFe $name@$host:$port is NOT alive")
        println("DorisFe $name@$host:$port is alive")
    }

    fun ensure_doris_fe_stopped(name: String) {
        val host = fes[name]!!.ip
        val port = feConf.getProperty("query_port", "9030").toInt()
        Util.ensurePortDead(host, port, "DorisFe $name@$host:$port is NOT dead")
        println("DorisFe $name@$host:$port is dead")
    }

    fun stop_doris_fe(vararg names: String) {
        names.forEach { name ->
            ShellCmd(shellCmdDir).run(dorisdbScript, "stop_doris_fe", name)
        }
        names.forEach { name ->
            ensure_doris_fe_stopped(name)
        }
    }

    fun ensure_doris_be_started(name: String) {
        val host = bes[name]!!.ip
        val port = beConf.getProperty("heartbeat_service_port", "9050").toInt()
        Util.ensurePortAlive(host, port, "DorisBe $name@$host:$port is NOT alive")
        println("DorisBe $name@$host:$port is alive")
    }

    fun ensure_doris_be_stopped(name: String) {
        val host = bes[name]!!.ip
        val port = beConf.getProperty("heartbeat_service_port", "9050").toInt()
        Util.ensurePortDead(host, port, "DorisBe $name@$host:$port is NOT dead")
        println("DorisBe $name@$host:$port is dead")
    }

    fun start_doris_be(vararg names: String) {
        names.forEach { name ->
            ShellCmd(shellCmdDir).run(dorisdbScript, "start_doris_be", name)
        }
        names.forEach { name ->
            ensure_doris_be_started(name)
        }
    }

    fun stop_doris_be(vararg names: String) {
        names.forEach { name ->
            ShellCmd(shellCmdDir).run(dorisdbScript, "stop_doris_be", name)
        }
        names.forEach { name ->
            ensure_doris_be_stopped(name)
        }
    }

    fun bootstrap_doris_fe(vararg names: String) {
        names.forEach { name ->
            ShellCmd(shellCmdDir).run(dorisdbScript, "bootstrap_doris_fe", name)
        }
        names.forEach { name ->
            ensure_doris_fe_started(name)
        }
    }

    fun bootstrap_doris_be(vararg names: String) {
        names.forEach { name ->
            ShellCmd(shellCmdDir).run(dorisdbScript, "bootstrap_doris_be", name)
        }
        names.forEach { name ->
            ensure_doris_be_started(name)
        }
    }

    fun clean_doris_cluster() {
        stop_doris_cluster()
        fes.values.forEach { fe ->
            println("cleanup fe: ${fe.name}")
            val dataDir = File("$dorisDockerDir/${fe.dataDir}")
            val logsDir = File("$dorisDockerDir/${fe.logsDir}")
            val runDir = File("$dorisDockerDir/${fe.runDir}")
            arrayListOf(dataDir, logsDir, runDir).forEach { dir ->
                if (dir.isDirectory) {
                    dir.deleteRecursively()
                }
            }
        }

        bes.values.forEach { be ->
            println("cleanup be: ${be.name}")
            val dataDir = File("$dorisDockerDir/${be.dataDir}")
            val logsDir = File("$dorisDockerDir/${be.logsDir}")
            val runDir = File("$dorisDockerDir/${be.runDir}")
            arrayListOf(dataDir, logsDir, runDir).forEach { dir ->
                if (dir.isDirectory) {
                    dir.deleteRecursively()
                }
            }
        }
    }

    fun bootstrap_doris_cluster() {
        bootstrap_doris_fe("doris_fe_follower0")
        bootstrap_doris_fe("doris_fe_follower1")
        bootstrap_doris_fe("doris_fe_follower2")
        bootstrap_doris_be("doris_be0", "doris_be1", "doris_be2")
    }

    fun start_doris_cluster() {
        bootstrap_doris_fe("doris_fe_follower0", "doris_fe_follower1", "doris_fe_follower2")
        bootstrap_doris_be("doris_be0", "doris_be1", "doris_be2")
    }

    fun drop_outbound_packet(srcHost: String, dstHost: String, dstPort: Int): () -> Unit {
        val srcIP = services[srcHost]!!.ip
        val dstIP = services[dstHost]!!.ip
        val iptablesCmd = "iptables -A OUTPUT -p tcp -s $srcIP -d $dstIP --dport $dstPort -j DROP"
        ShellCmd(shellCmdDir).run("docker exec -u root $srcHost $iptablesCmd")
        return fun() {
            val iptablesCmd = "iptables -D OUTPUT -p tcp -s $srcIP -d $dstIP --dport $dstPort -j DROP"
            ShellCmd(shellCmdDir).run("docker exec -u root $srcHost $iptablesCmd")
        }
    }

    fun drop_inbound_packet(srcHost: String, dstHost: String, dstPort: Int): () -> Unit {
        val srcIP = services[srcHost]!!.ip
        val dstIP = services[dstHost]!!.ip
        val iptablesCmd = "iptables -A INPUT -p tcp -s $srcIP -d $dstIP --dport $dstPort -j DROP"
        ShellCmd(shellCmdDir).run("docker exec -u root $dstHost $iptablesCmd")
        return fun() {
            val iptablesCmd = "iptables -D INPUT -p tcp -s $srcIP -d $dstIP --dport $dstPort -j DROP"
            ShellCmd(shellCmdDir).run("docker exec -u root $dstHost $iptablesCmd")
        }
    }

    fun getBadTabletReplicas(db: String, table: String): List<Map<String, Any>>? {
        val sql = "ADMIN SHOW REPLICA STATUS from $db.$table WHERE STATUS != 'OK'"
        val masterFe = getMasterFe()
        Assert.assertNotNull(masterFe)
        val port = feConf.getProperty("query_port", "9030").toInt()
        val cxnString = "jdbc:mysql://$masterFe:$port/?user=root"
        val pool = MySQLUtil.newMySQLConnectionPool(cxnString, 1)
        return MySQLUtil.q(pool, sql)
    }

    fun getBadTabletReplicas(jdbc: JdbcTemplate, db: String, table: String): List<Map<String, Any>> {
        return runSql(jdbc) { jdbc ->
            jdbc.queryForList("ADMIN SHOW REPLICA STATUS from $db.$table WHERE STATUS != 'OK';")
        }
    }

    fun getBadTabletReplicas(masterFe: String, db: String, table: String): List<Map<String, Any>> {
        return runSql(10, masterFe, null) { jdbc ->
            jdbc.queryForList("ADMIN SHOW REPLICA STATUS from $db.$table WHERE STATUS != 'OK';")
        }
    }

    fun stop_doris_cluster() {
        val cmd = ShellCmd(shellCmdDir)
        cmd.run("docker ps -f status=running --format={{.Names}}")
        cmd.stdout.toString().split("\n").map { it.trim() }.forEach { name ->
            if (fes.containsKey(name) || bes.containsKey(name)) {
                cmd.run_nocheck("docker kill $name")
            }
        }
    }

    fun getAliveFeSet(): Set<String> {
        val cmd = ShellCmd(shellCmdDir)
        cmd.run("docker ps -f status=running --format={{.Names}}")
        return cmd.stdout.toString().split("\n")
                .map { it.trim() }
                .filter { fes.containsKey(it) }.toSet()
    }

    fun getAliveBeSet(): Set<String> {
        val cmd = ShellCmd(shellCmdDir)
        cmd.run("docker ps -f status=running --format={{.Names}}")
        return cmd.stdout.toString().split("\n")
                .map { it.trim() }
                .filter { bes.containsKey(it) }.toSet()
    }

    fun getMV(db: String, table: String, mv: String): Map<String, Any>? {
        return listMV(db).filter { m ->
            m["BaseIndexName"] == table && m["RollupIndexName"] == mv
        }.let { l ->
            if (l.isEmpty()) {
                null
            } else {
                l.first()
            }
        }
    }

    fun dropMV(db: String, table: String, mv: String) {
        getMV(db, table, mv)?.let { m ->
            val dropSql = "DROP MATERIALIZED VIEW IF EXISTS $mv on $db.$table"
            val cancelSql = "CANCEL ALTER TABLE ROLLUP FROM $db.$table (${m["JobId"] as String})"
            if (m["State"] as String == "FINISHED") {
                Result.wrap { runSql(db) { j -> j.execute(dropSql) } }
            } else {
                Result.wrap {
                    runSql(db) { j -> j.execute(cancelSql) }
                } onErr {
                    Result.wrap { runSql(db) { j -> j.execute(dropSql) } }
                }
            }
        }
        Util.ensure(60, "$mv on $db.$table drop fails") {
            getMV(db, table, mv) == null
        }
    }

    fun getMasterFe(host: String): String? {
        val port = feConf.getProperty("query_port", "9030")!!
        val cxnString = "jdbc:mysql://$host:$port/?user=root"
        println("getMasterFe from fe@$host:$port")
        val pool = MySQLUtil.newMySQLConnectionPool(cxnString, 1)
        val sql = "SHOW PROC '/frontends'"
        return MySQLUtil.q(pool, sql)?.filter { fe ->
            fe.let {
                it.containsKey("IsMaster") && it["IsMaster"]!! == "true"
            }
        }?.let {
            if (it.isEmpty()) {
                null
            } else {
                it.first()["HostName"] as String
            }
        } ?: null
    }

    fun getMasterFe(): String? {
        return Result.wrap {
            getMasterFe(getAliveFeSet().first())
        }.onErr {
            println("Error=${it.javaClass.simpleName}, ErrMsg=${it.message}")
            it.printStackTrace()
        }.unwrap_or_null()
    }

    fun <T> runSql(f: (JdbcTemplate) -> T): T {
        return runSql(null as String?, f)
    }

    fun dropTable(db: String, table: String) {
        runSql(db) { j ->
            j.execute("DROP TABLE IF EXISTS $table")
        }
        Util.ensure(60, "Fail to drop table $table") {
            runSql(db) { j -> j.queryForList("show tables") }.none {
                it?.get("Tables_in_$db")?.equals(table) ?: false
            }
        }
    }

    fun <T> runSql(db: String?, f: (JdbcTemplate) -> T): T {
        val masterFe = getMasterFe()
        Assert.assertNotNull(masterFe)
        return runSql(10, masterFe!!, db, f)
    }

    fun getSqlClient(db: String?): MySQLUtil.Sql {
        val masterFe = getMasterFe()
        Assert.assertNotNull(masterFe)
        val port = feConf.getProperty("query_port", "9030").toInt()
        return getSqlClient("$masterFe:$port", db)
    }

    fun getSqlClient(hostPort: String, db: String?): MySQLUtil.Sql {
        val cxnString = "jdbc:mysql://$hostPort/${db ?: ""}?user=root"
        val pool = MySQLUtil.newMySQLConnectionPool(cxnString, 1)
        return MySQLUtil.Sql(pool)
    }

    fun murmur_hash3_32(db: String, table: String): Long {
        return runSql(db) {
            val hash = "hash"
            val cols = it.queryForList("DESC $table").map { f -> f["Field"] as String }
            val sql = "SELECT SUM(${cols.joinToString("+") { f -> "murmur_hash3_32($f)" }}) as $hash from $db.$table"
            println("exec SQL for computing murmur_hash3_32: $sql")
            it.queryForList(sql).first()[hash] as Long
        }
    }

    fun jdbc(masterFe: String): JdbcTemplate {
        val port = feConf.getProperty("query_port", "9030").toInt()
        val cxnString = "jdbc:mysql://$masterFe:$port/?user=root"
        val driver = Class.forName(Driver::class.java.canonicalName).newInstance() as Driver
        val dataSource = MySQLUtil.getDataSource(driver, cxnString)
        return JdbcTemplate(dataSource)
    }

    fun <T> runSql(jdbc: JdbcTemplate, f: (JdbcTemplate) -> T): T {
        return f(jdbc)
    }

    fun <T> runSql(timeout: Int, f: (JdbcTemplate) -> T): T {
        val masterFe = getMasterFe()
        Assert.assertNotNull(masterFe)
        return runSql(timeout, masterFe!!, null, f)
    }

    fun <T> runSql(timeout: Int, masterFe: String, db: String?, f: (JdbcTemplate) -> T): T {
        val port = feConf.getProperty("query_port", "9030").toInt()
        println("masterFe fe@$masterFe:$port")
        return MySQLUtil.runSql(timeout, "jdbc:mysql://$masterFe:$port/${db ?: ""}?user=root", f)
    }

    fun streamLoad(
            db: String,
            table: String,
            data: ByteArray): Boolean {
        return streamLoad(db, table, emptyList(), data)
    }

    fun listMV(db: String): List<Map<String, Any>> {
        return runSql(db) { j ->
            j.queryForList("SHOW ALTER TABLE ROLLUP FROM $db")
        }
    }

    fun ensure_mv_finished(db: String, table: String, mv: String) {
        Util.ensure({
            listMV(db).any { m ->
                m["BaseIndexName"] == table && m["RollupIndexName"] == mv && m["State"] == "FINISHED"
            }
        },
                "MV $mv on $db.$table is not finished",
                600
        )
    }

    fun streamLoadIntoTableConcurrently(
            beList: List<String>,
            db: String,
            table: String,
            headers: List<Pair<String, String>>,
            batches: Int,
            dataList: List<ByteArray>
    ): () -> Boolean {

        Assert.assertTrue(beList.isNotEmpty())
        beList.forEach { be -> ensure_doris_be_started(be) }
        Assert.assertTrue(dataList.isNotEmpty())
        Assert.assertTrue(batches > 0)

        val nextBe = Util.roundRobin(beList)
        val nextData = Util.roundRobin(dataList)

        val houseKeeper = HouseKeeper()
        val killThread = AtomicBoolean(false)
        val totalBatch = AtomicInteger(0)
        val failBatch = AtomicInteger(0)
        val sucessBatch = AtomicInteger(0)
        val thd = thread {
            val promises = mutableListOf<Promise<Boolean?>>()
            while (!killThread.acquire) {
                totalBatch.incrementAndGet()
                houseKeeper.async {
                    streamLoad(
                            nextBe(),
                            8040,
                            "root",
                            "",
                            "label_${abs(Random().nextLong() / 2)}",
                            ",",
                            db,
                            table,
                            headers,
                            nextData())
                }.addListener { future ->
                    if (future != null && future.isSuccess) {
                        sucessBatch.incrementAndGet()
                    } else {
                        failBatch.incrementAndGet()
                        future?.cause()?.printStackTrace()
                    }
                }.let {
                    promises.add(it)
                }

                if (promises.size == batches) {
                    println("streamLoad for the ${totalBatch.getAcquire()} time")
                    promises.forEach {
                        it.get()
                    }
                    promises.clear()
                }
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1))
            }
            println("streamLoad for the ${totalBatch.getAcquire()} time")
            if (promises.isNotEmpty()) {
                promises.forEach { it.get() }
                promises.clear()
            }
        }

        return {
            killThread.setRelease(true)
            thd.join()
            houseKeeper.shutdown()
            totalBatch.getAcquire() == sucessBatch.getAcquire() + failBatch.getAcquire()
        }
    }

    fun streamLoad(
            db: String,
            table: String,
            headers: List<Pair<String, String>>,
            data: ByteArray
    ): Boolean {
        return getAliveBeSet().let { bes ->
            Assert.assertTrue(bes.isNotEmpty())
            bes.first()
        }.let { be ->
            ensure_doris_be_started(be)
            streamLoad(
                    be,
                    8040,
                    "root",
                    "",
                    "${db}_${table}_label_${System.currentTimeMillis()}",
                    ",",
                    db,
                    table,
                    headers,
                    data)
        }
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
          cat <<<'DONE' |curl --location-trusted -u $user:$password $headerString -T - $uri
          ${String(data).split("\n").take(10).joinToString("\n")}
          DONE
        """.split("\n").map { it.trim() }.joinToString("\n"))
        println("=====================")
        println("$path\nresponseContent=$responseContent")
        return statusCode in (200..299) && result.containsKey("Status") && result["Status"] as String == "Success"
    }

    fun must_start_doris_cluster() {
        if (getAliveFeSet().isNotEmpty() && getAliveBeSet().size >= 3) {
            return
        }
        Result.wrap {
            stop_doris_cluster()
            start_doris_cluster()
        }.onErr {
            clean_doris_cluster()
            bootstrap_doris_cluster()
        }
    }

    fun recreate_db(db: String) {
        runSql { j ->
            j.execute("DROP DATABASE IF EXISTS $db")
            j.execute("CREATE DATABASE IF NOT EXISTS $db")
        }
    }

    fun failover() {
        var prevMasterFe = getMasterFe()
        Assert.assertNotNull(prevMasterFe)
        println("before failover: masterFe=${prevMasterFe}")
        ensure_doris_fe_started(prevMasterFe!!)
        stop_doris_fe(prevMasterFe!!)

        var masterFe: String? = null
        var n = 0
        while (true) {
            println("Try to getMasterFe for the $n time")
            masterFe = getMasterFe()
            if (masterFe != null || n == 60) {
                break
            }
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2))
            n++
        }
        Assert.assertNotNull(masterFe)
        println("after failover: masterFe=${masterFe}")
        start_doris_fe(prevMasterFe)
        masterFe = getMasterFe(prevMasterFe)
        Assert.assertNotNull(masterFe)
        println("after $prevMasterFe recovered: masterFe=${masterFe}")
    }
}