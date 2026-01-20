#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cleanup() {
    echo "=== Stopping test containers ==="
    cd "$SCRIPT_DIR"
    docker compose down --remove-orphans 2>/dev/null || true
}

# Always cleanup on exit (success or failure)
trap cleanup EXIT

cd "$PROJECT_DIR"

echo "=== Building blobber ==="
go build -o blobber .

echo "=== Starting test containers ==="
cd "$SCRIPT_DIR"
docker compose up -d

echo "=== Waiting for databases to be ready ==="

# Wait for MySQL
echo -n "Waiting for MySQL..."
for i in {1..30}; do
    if docker compose exec -T mysql mysqladmin ping -h localhost -u root -ptestpass &>/dev/null; then
        echo " ready"
        break
    fi
    echo -n "."
    sleep 1
done

# Wait for MariaDB
echo -n "Waiting for MariaDB..."
for i in {1..30}; do
    if docker compose exec -T mariadb mariadb-admin ping -h localhost -u root -ptestpass &>/dev/null; then
        echo " ready"
        break
    fi
    echo -n "."
    sleep 1
done

# Wait for PostgreSQL
echo -n "Waiting for PostgreSQL..."
for i in {1..30}; do
    if docker compose exec -T postgres pg_isready -U testuser -d testdb &>/dev/null; then
        echo " ready"
        break
    fi
    echo -n "."
    sleep 1
done

echo "=== Running integration tests ==="
cd "$SCRIPT_DIR"
go test -v -count=1 ./...

echo "=== All tests passed ==="
