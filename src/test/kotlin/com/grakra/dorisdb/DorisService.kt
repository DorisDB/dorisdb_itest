package com.grakra.dorisdb

class DorisService(val name: String, val ip: String, val type: DorisServiceType) {
  val logsDir = "${name}_logs"
  val dataDir = "${name}_data"
  val runDir = "${name}_run"
}
