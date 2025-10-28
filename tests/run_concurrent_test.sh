#!/bin/bash
# ===============================================
# Phase 2 concurrency test runner
# Usage:
#   ./run_concurrent_test.sh [threads] [mode]
#   Example: ./run_concurrent_test.sh 50 valgrind
# ===============================================

THREADS=${1:-50}
MODE=${2:-none}
SERVER_BIN="./server"
CLIENT_BIN="./client_multi"
SERVER_PID_FILE="server.pid"

set -e  # stop on any error
trap 'cleanup' EXIT INT TERM

cleanup() {
  if [ -f "$SERVER_PID_FILE" ]; then
    PID=$(cat "$SERVER_PID_FILE" 2>/dev/null || true)
    if [ -n "$PID" ]; then
      echo "Stopping server (PID $PID)..."
      kill "$PID" 2>/dev/null || true
    fi
    rm -f "$SERVER_PID_FILE"
  fi
}

# ===== Build section =====
if [ -f Makefile ]; then
  echo "Running make..."
  make || { echo "Build failed"; exit 1; }
else
  echo "No Makefile found; compiling manually..."
  gcc -pthread -Wall -Wextra server.c -o server
  gcc -pthread -Wall -Wextra client_multi.c -o client_multi
fi

# ===== Run modes =====
case "$MODE" in
  tsan)
    echo "[Mode: ThreadSanitizer]"
    make tsan || exit 1
    ./server_tsan & echo $! > "$SERVER_PID_FILE"
    sleep 1
    ./client_multi_tsan "$THREADS"
    ;;

  valgrind)
    echo "[Mode: Valgrind]"
    $SERVER_BIN & echo $! > "$SERVER_PID_FILE"
    sleep 1
    valgrind --leak-check=full --track-origins=yes $CLIENT_BIN "$THREADS"
    ;;

  none|*)
    echo "[Mode: Normal]"
    $SERVER_BIN & echo $! > "$SERVER_PID_FILE"
    sleep 1
    $CLIENT_BIN "$THREADS"
    ;;
esac

cleanup
echo "✅ Test completed successfully."
exit 0
