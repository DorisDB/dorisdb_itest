package com.grakra.dorisdb

import com.google.gson.Gson
import com.grakra.util.MySQLUtil
import com.grakra.util.Result
import com.grakra.util.Util
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
import java.util.concurrent.locks.LockSupport
import java.util.regex.Pattern

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
    Util.ensurePortAlive(host, port, "DorisFe $name@$host:$port is alive")
  }

  fun ensure_doris_fe_stopped(name: String) {
    val host = fes[name]!!.ip
    val port = feConf.getProperty("query_port", "9030").toInt()
    Util.ensurePortDead(host, port, "DorisFe $name@$host:$port is dead")
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
    Util.ensurePortAlive(host, port, "DorisBe $name@$host:$port is alive")
  }

  fun ensure_doris_be_stopped(name: String) {
    val host = bes[name]!!.ip
    val port = beConf.getProperty("heartbeat_service_port", "9050").toInt()
    Util.ensurePortDead(host, port, "DorisBe $name@$host:$port is dead")
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

  fun drop_inbound_packet(srcHost:String, dstHost: String, dstPort:Int): () -> Unit {
    val srcIP = services[srcHost]!!.ip
    val dstIP = services[dstHost]!!.ip
    val iptablesCmd = "iptables -A INPUT -p tcp -s $srcIP -d $dstIP --dport $dstPort -j DROP"
    ShellCmd(shellCmdDir).run("docker exec -u root $dstHost $iptablesCmd")
    return fun() {
      val iptablesCmd = "iptables -D INPUT -p tcp -s $srcIP -d $dstIP --dport $dstPort -j DROP"
      ShellCmd(shellCmdDir).run("docker exec -u root $dstHost $iptablesCmd")
    }
  }

  fun getBadTabletReplicas(db:String, table:String):List<Map<String, Any>> {
    return runSql {jdbc->
      jdbc.queryForList("ADMIN SHOW REPLICA STATUS from db0.table3 WHERE STATUS != 'OK';")
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

  fun getMasterFe(host: String): String? {
    val port = feConf.getProperty("query_port", "9030")!!
    val cxnString = "jdbc:mysql://$host:$port/?user=root"
    println("cxnString=$cxnString")
    return MySQLUtil.runSql(cxnString) { jdbc ->
      jdbc.queryForList("SHOW PROC '/frontends';")?.filter { fe ->
        fe?.let {
          it.containsKey("IsMaster") && it["IsMaster"]!! == "true"
        } ?: false
      }?.let {
        if (it.isEmpty()) {
          null
        } else {
          it.first()["HostName"] as String
        }
      } ?: null
    }
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
    val masterFe = getMasterFe()
    Assert.assertNotNull(masterFe)
    val port = feConf.getProperty("query_port", "9030").toInt()
    return MySQLUtil.runSql("jdbc:mysql://$masterFe:$port/?user=root", f)
  }
  fun streamLoad(
      host: String,
      port: Int,
      user: String, password: String,
      label: String,
      columnSeparator: String,
      db: String,
      table: String,
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
    put.entity = ByteArrayEntity(data, ContentType.TEXT_PLAIN)
    val response = httpClient.execute(put)!!
    val statusCode = response.statusLine.statusCode
    val result = Gson().fromJson<Map<String, Object>>(
        response.entity.content.bufferedReader().readText(), Map::class.java)!!
    return statusCode in (200..299) && result.containsKey("Status") && result["Status"] as String == "Success"
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