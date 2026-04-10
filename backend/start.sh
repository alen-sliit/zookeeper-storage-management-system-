#!/usr/bin/env bash

set -u

# Always run relative to backend directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

start_in_window() {
  local name="$1"
  local command="$2"

  echo "Opening $name in new window..."
  # Windows Git Bash: use 'start' to open new window
  start bash -i -c "echo '=== $name ===' && $command; bash"
}

echo "Starting all services in separate windows..."
echo

start_in_window "server-1" "mvn exec:java -Dexec.args='server-1 localhost:2181,localhost:2182,localhost:2183 8081'"
sleep 1
start_in_window "server-2" "mvn exec:java -Dexec.args='server-2 localhost:2181,localhost:2182,localhost:2183 8082'"
sleep 1
start_in_window "server-3" "mvn exec:java -Dexec.args='server-3 localhost:2181,localhost:2182,localhost:2183 8083'"
sleep 1
start_in_window "server-4" "mvn exec:java -Dexec.args='server-4 localhost:2181,localhost:2182,localhost:2183 8084'"
sleep 1
start_in_window "server-5" "mvn exec:java -Dexec.args='server-5 localhost:2181,localhost:2182,localhost:2183 8085'"
sleep 1
start_in_window "web-server" "mvn -f pom.xml exec:java -Dexec.mainClass=com.example.zookeeper.client.WebClientServer -Dexec.args='localhost:2181,localhost:2182,localhost:2183 8080'"

echo
echo "All services launched in separate windows."
echo "Each window will close when you press Enter."
