#!/bin/bash
set -e -o pipefail
basedir=$(cd $(dirname $(readlink -f ${BASH_SOURCE:-$0}));pwd)

java -cp ${basedir}/target/dorisdb_itest-1.0-SNAPSHOT-jar-with-dependencies.jar com.grakra.tools.CatOrcKt $*
