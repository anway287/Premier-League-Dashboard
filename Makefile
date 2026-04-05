.PHONY: install test test-fast test-epl test-nba test-e2e test-slow \
        metrics-server stack-up stack-down tf-init tf-apply tf-destroy clean

PYTHON   = /Library/Frameworks/Python.framework/Versions/3.12/bin/python3
PYTEST   = $(PYTHON) -m pytest
PIP      = $(PYTHON) -m pip
DOCKER   = docker compose

# ── Setup ──────────────────────────────────────────────────────────────────

install:
	$(PIP) install -r requirements.txt

# ── Tests ──────────────────────────────────────────────────────────────────

test:
	$(PYTEST)

test-fast:
	$(PYTEST) -m "not slow and not integration"

test-epl:
	$(PYTEST) -m epl -v

test-nba:
	$(PYTEST) -m nba -v

test-e2e:
	$(PYTEST) -m e2e -v

test-slow:
	$(PYTEST) -m slow -v

test-parallel:
	$(PYTEST) -n auto

test-integration:
	HERMETIC_USE_LOCALSTACK=1 $(PYTEST) -m integration -v

# ── Dashboards ─────────────────────────────────────────────────────────────

# Visual sports dashboard — opens in browser automatically at http://localhost:8080
dashboard:
	$(PYTHON) scripts/sports_dashboard.py

# Run tests then serve metrics for Grafana/Prometheus
metrics-server:
	$(PYTEST) && $(PYTHON) -m metrics.exporter

# ── Docker stack (requires Docker Desktop) ─────────────────────────────────

stack-up:
	@command -v docker >/dev/null 2>&1 || { echo "Docker not found. Install Docker Desktop: https://www.docker.com/products/docker-desktop/"; exit 1; }
	$(DOCKER) up -d
	@echo "Grafana    → http://localhost:3000  (admin / admin)"
	@echo "Prometheus → http://localhost:9091"
	@echo "LocalStack → http://localhost:4566"

stack-down:
	@command -v docker >/dev/null 2>&1 || { echo "Docker not installed"; exit 1; }
	$(DOCKER) down -v

# ── Terraform ──────────────────────────────────────────────────────────────

tf-init:
	cd terraform && terraform init

tf-apply:
	cd terraform && terraform apply -auto-approve \
	  -var="run_prefix=manual-$(shell date +%Y%m%d)"

tf-destroy:
	cd terraform && terraform destroy -auto-approve \
	  -var="run_prefix=manual-$(shell date +%Y%m%d)"

# ── Clean ──────────────────────────────────────────────────────────────────

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache  -exec rm -rf {} + 2>/dev/null || true
	rm -f metrics/results.json metrics/metrics.prom
