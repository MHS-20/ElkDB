GO       = go
GOFLAGS  =
DBGFLAGS = -gcflags="all=-N -l"
LDFLAGS  =

CLI     = elkdb
SERVER  = elkdb-server
TARGETS = $(CLI) $(SERVER)
PKG     = ./...

DB      ?= elkdb.db
ADDR    ?= :5433

.PHONY: all
all: $(TARGETS)

$(CLI):
	@echo "  GO BUILD  $(CLI)"
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(CLI) ./cmd/cli

$(SERVER):
	@echo "  GO BUILD  $(SERVER)"
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(SERVER) ./cmd/server

.PHONY: debug
debug: GOFLAGS += $(DBGFLAGS)
debug: clean $(TARGETS)

# run: start the server in the background, then open the remote REPL.
# The server is stopped automatically when the REPL exits (trap on EXIT).
.PHONY: run
run: all
	@echo "  START     $(SERVER) -db $(DB) -addr $(ADDR)"
	@./$(SERVER) -db $(DB) -addr $(ADDR) & \
	SERVER_PID=$$!; \
	trap "kill $$SERVER_PID 2>/dev/null" EXIT; \
	sleep 0.2; \
	./$(CLI) -remote $(ADDR) $(ARGS); \
	wait $$SERVER_PID 2>/dev/null || true

.PHONY: test
test:
	@echo "  GO TEST"
	$(GO) test $(PKG)

.PHONY: lint
lint:
	@echo "  LINT"
	$(GO) vet $(PKG)
	@command -v staticcheck >/dev/null 2>&1 && staticcheck $(PKG) || \
		echo "  (staticcheck not found; run: go install honnef.co/go/tools/cmd/staticcheck@latest)"

.PHONY: fmt
fmt:
	@echo "  FMT"
	$(GO) fmt $(PKG)

.PHONY: tidy
tidy:
	@echo "  TIDY"
	$(GO) mod tidy

.PHONY: clean
clean:
	rm -f $(TARGETS)
	@echo "  Cleaned."

PREFIX  = /usr/local
.PHONY: install
install: all
	install -m 755 $(CLI)    $(PREFIX)/bin/$(CLI)
	install -m 755 $(SERVER) $(PREFIX)/bin/$(SERVER)
	@echo "  Installed to $(PREFIX)/bin/"

.PHONY: uninstall
uninstall:
	rm -f $(PREFIX)/bin/$(CLI) $(PREFIX)/bin/$(SERVER)
	@echo "  Uninstalled $(CLI) and $(SERVER)"

.PHONY: help
help:
	@echo "Targets:"
	@echo "  all       — build both binaries (default)"
	@echo "  debug     — build without optimisations (-N -l)"
	@echo "  run       — build, start server, open remote REPL (stops server on exit)"
	@echo "  test      — run all tests"
	@echo "  lint      — go vet + staticcheck"
	@echo "  fmt       — gofmt all packages"
	@echo "  tidy      — go mod tidy"
	@echo "  clean     — remove binaries"
	@echo "  install   — install both binaries to $(PREFIX)/bin"
	@echo "  uninstall — remove both binaries from $(PREFIX)/bin"
	@echo ""
	@echo "Variables:"
	@echo "  DB=$(DB)     path to the database file"
	@echo "  ADDR=$(ADDR)    server listen address"
	@echo "  ARGS=       extra flags passed to the CLI"
