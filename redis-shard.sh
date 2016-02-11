#!/bin/bash
#
# Moves/Shards Redis slots
#
# Jason V. Orona

shopt -s -o nounset

declare -rx SCRIPT=${0##*/}

declare -rx redis='/usr/local/bin/redis-cli'

declare -x SRC_IP=${1}
declare -x SRC_PORT=${2}
declare -x DEST_IP=${3}
declare -x DEST_PORT=${4}
declare -x HSLOT_START=${5}
declare -x HSLOT_END=${6}
# redis-shard.sh <src-ip> <src-port> <dest-ip> <dest-port> <hslot-start> <hslot-end>

if test ! -x "${redis}"; then
  printf "${SCRIPT}:${LINENO}: the command ${redis} is not available - aborting\n" >&2
  exit 197
fi

function getClusterNodes() {
  # Gathers node ip:port and node ID of all known cluster nodes
  echo "Discovering cluster nodes"
  sleep 5
  declare -a cluster_nodes=$(redis-cli -h ${SRC_IP} -p ${SRC_PORT} CLUSTER NODES | awk '{print $2"|"$1}');
  echo "Cluster Nodes:";
  echo "IP:PORT | NODE_ID"
  for node in ${cluster_nodes[@]}; do
    echo "${node}"
  done
  return 0
}

function clusterMeet() {
  # Make all the nodes aware of each other. It is not necessary to
  # execute a CLUSTER MEET on all of the other nodes, as all nodes exchange information
  # about other nodes.
  echo "Meeting with cluster ${DEST_IP}:${DEST_PORT}"
  echo "redis-cli -h ${SRC_IP} -p ${SRC_PORT} CLUSTER MEET ${DEST_IP} ${DEST_PORT}"
}

function clusterSlotImport() {
  # Changes the hash slot state to importing. It must be executed at
  # the node that is going to receive the hash slot, and the node ID of the current
  # slot owner must be passed in.
  # CLUSTER SETSLOT <hash-slot> IMPORTING <source-id>
  local hslot=${1}
  local src_id=${2}
  echo "redis-cli -h ${DEST_IP} -p ${DEST_PORT} CLUSTER SETSLOT ${hslot} IMPORTING ${src_id}"
  redis-cli -h ${DEST_IP} -p ${DEST_PORT} CLUSTER SETSLOT ${hslot} IMPORTING ${src_id}
  return 0
}

function clusterSlotMigrate() {
  # Changes the hash slot state to migrating. It is the opposite of the importing
  # subcommand. It must me executed at the node that owns the hash slot, and the node ID
  # of the new slot owner must be passed in.
  # CLUSTER SETSLOT <hash-slot> MIGRATING <destination-id>
  local hslot=${1}
  local dest_id=${2}
  echo "redis-cli -h ${SRC_IP} -p ${SRC_PORT} CLUSTER SETSLOT ${hslot} MIGRATING ${dest_id}"
  redis-cli -h ${SRC_IP} -p ${SRC_PORT} CLUSTER SETSLOT ${hslot} MIGRATING ${dest_id}
  return 0
}

function clusterSetSlotNode() {
  # Associates a hash slot with a node. It must be executed on the source and destination nodes.
  # Executing it on all master nodes is also recommended to avoid wrong redirects while the
  # propagation takes place.
  # When executed on destination node, the importing state is cleared and then the configuration
  # epoch is updated.
  # When executed on the source node, the migrating state is cleared as long as no keys exist
  # in that slot. Otherwise and error is thrown.
  # CLUSTER SETSLOT <hash-slot> MIGRATING <owner-id>
  local hslot=${1}
  local dest_id=${2}
  master_nodes=$(redis-cli -h ${SRC_IP} -p ${SRC_PORT} CLUSTER NODES | grep master | awk '{print$2}')
  if [ -z "${master_nodes}" ]; then
    echo "No other masters found"
  else
    for master in ${master_nodes[@]}; do
      ip=$(echo "${master}" | awk -F: '{print $1}')
      port=$(echo "${master}" | awk -F: '{print $2}')
      echo "redis-cli -h ${ip} -p ${port} CLUSTER SETSLOT ${hslot} NODE ${dest_id}"
      redis-cli -h ${ip} -p ${port} CLUSTER SETSLOT ${hslot} NODE ${dest_id}
    done
  fi
  #echo "redis-cli -h ${SRC_IP} -p ${SRC_PORT} CLUSTER SETSLOT ${hslot} NODE ${dest_id}"
  #echo "redis-cli -h ${DEST_IP} -p ${DEST_PORT} CLUSTER SETSLOT ${hslot} NODE ${dest_id}"
  return 0
}

function clusterSetSlotStable() {
  # Clears any state of a hash slot (importing or migrating). It is useful wen a rollback in a resharding
  # operation is needed.
  # CLUSTER SETSLOT <hash-slot> STABLE
  return 0
}

function migrateKey() {
  # CLUSTER COUNTKEYSINSLOT <slot>
  # CLUSTER GETKEYSINSLOT
  # MIGRATE <host> <port> <key> <db> <timeout>
  local hslot=${1}
  key_cnt=$(redis-cli -h ${SRC_IP} -p ${SRC_PORT} CLUSTER COUNTKEYSINSLOT ${hslot} | awk '{print$1}')
  if [[ ${key_cnt} -ne 0 ]]; then
    keys=$(redis-cli -h ${SRC_IP} -p ${SRC_PORT} CLUSTER GETKEYSINSLOT ${hslot} ${key_cnt} | awk '{print$1}')
    for key in ${keys[@]}; do
      echo "Migrating ${key} from ${hslot} to ${DEST_IP} ${DEST_PORT}"
      #echo "redis-cli -h ${SRC_IP} -p ${SRC_PORT} MIGRATE ${DEST_IP} ${DEST_PORT} ${key} 0 2000"
      redis-cli -h ${SRC_IP} -p ${SRC_PORT} MIGRATE ${DEST_IP} ${DEST_PORT} ${key} 0 2000
    done
  else
    echo "No keys found in ${hslot}"
  fi
  return 0
}

function slotPusher() {
  # Main executor of slot commands
  local src_id=$(redis-cli -h ${SRC_IP} -p ${SRC_PORT} CLUSTER NODES | grep "${SRC_IP}:${SRC_PORT}" | awk '{print$1}')
  local dest_id=$(redis-cli -h ${DEST_IP} -p ${DEST_PORT} CLUSTER NODES | grep "${DEST_IP}:${DEST_PORT}" | awk '{print$1}')
  for slot in $( seq $HSLOT_START $HSLOT_END); do
    echo ${slot}
    clusterSlotImport ${slot} ${src_id}
    clusterSlotMigrate ${slot} ${dest_id}
    migrateKey ${slot}
    clusterSetSlotNode ${slot} ${dest_id}
    #clusterSetSlotStable
  done
  return 0
}

function execute_main() {
  # Main program
  clusterMeet
  slotPusher
}

execute_main

exit 0
