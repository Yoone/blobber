.PHONY: all
all: fmt vendor build

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vendor
vendor:
	go mod tidy

.PHONY: build
build:
	go build -o blobber .

.PHONY: run
run: build
	./blobber

# Run unit tests only
.PHONY: test
test:
	go test $$(go list ./... | grep -v /integration)

# Run integration tests (starts/stops Docker containers automatically)
.PHONY: test-integration
test-integration:
	./integration/run-tests.sh

# Run all tests
.PHONY: test-all
test-all: test test-integration

.PHONY: clean
clean:
	$(RM) blobber
	go clean

# === Development / Manual Testing ===

# Start dev containers (MySQL, MariaDB, Postgres, MinIO, Azurite)
.PHONY: dev-up
dev-up:
	@echo "Starting dev containers..."
	cd integration && docker compose up -d
	@echo "Waiting for databases to be ready..."
	@until docker compose -f integration/docker-compose.yml exec -T mysql mysqladmin ping -h localhost -u root -ptestpass 2>/dev/null; do sleep 1; done
	@until docker compose -f integration/docker-compose.yml exec -T mariadb mariadb-admin ping -h localhost -u root -ptestpass 2>/dev/null; do sleep 1; done
	@until docker compose -f integration/docker-compose.yml exec -T postgres pg_isready -U testuser -d testdb 2>/dev/null; do sleep 1; done
	@echo "Waiting for storage init containers..."
	@docker compose -f integration/docker-compose.yml run --rm minio-init 2>/dev/null || true
	@docker compose -f integration/docker-compose.yml run --rm azurite-init 2>/dev/null || true
	@echo "Initializing SQLite test database..."
	@mkdir -p dev/backups/sqlite dev/backups/mysql dev/backups/mariadb dev/backups/postgres
	@rm -f dev/test.db
	@sqlite3 dev/test.db < dev/init-sqlite.sql
	@echo ""
	@echo "Dev environment ready!"
	@echo "  MySQL:    localhost:3306 (testuser/testpass/testdb)"
	@echo "  MariaDB:  localhost:3307 (testuser/testpass/testdb)"
	@echo "  Postgres: localhost:5432 (testuser/testpass/testdb)"
	@echo "  SQLite:   dev/test.db"
	@echo "  MinIO:    http://localhost:9001 (minioadmin/minioadmin)"
	@echo "  Azurite:  localhost:10000 (devstoreaccount1)"
	@echo ""
	@echo "Run 'make dev' to start the TUI (uses dev/rclone.conf automatically)"

# Stop dev containers
.PHONY: dev-down
dev-down:
	@echo "Stopping dev containers..."
	cd integration && docker compose down --remove-orphans

# Run blobber TUI with dev config
.PHONY: dev
dev: build
	./blobber -c dev/blobber-dev.yaml --rclone-config dev/rclone.conf

# Clean dev data (backups and SQLite)
.PHONY: dev-clean
dev-clean:
	rm -rf dev/backups dev/test.db
