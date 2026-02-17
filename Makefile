# Variables
ICEBERG_VERSION := 1.10.1
SPARK_VERSION := 3.5
SCALA_VERSION := 2.12
JAR_NAME := iceberg-spark-runtime-$(SPARK_VERSION)_$(SCALA_VERSION)-$(ICEBERG_VERSION).jar
JAR_URL := https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-$(SPARK_VERSION)_$(SCALA_VERSION)/$(ICEBERG_VERSION)/$(JAR_NAME)
JARS_DIR := ./jars


.PHONY: install-uv install-dependencies install \
        ruff mypy lint \
        format format-check \
        precommit \
        all-test unit-test integration-test

install-uv:  ## Install uv if the user has not done that yet.
	 @if ! which uv >/dev/null; then \
        echo "uv could not be found. Installing in ${HOME}/.uv"; \
		python3 -m venv ${HOME}/.uv && \
		. ${HOME}/.uv/bin/activate && \
        pip install --retries 20 --timeout 60 uv && \
		if [ ! -f "${HOME}/.local/bin/uv" ]; then \
			echo "Creating a symlink for uv in ~/.local/bin"; \
			mkdir -p ${HOME}/.local/bin; \
			ln -s ${HOME}/.uv/bin/uv ${HOME}/.local/bin/uv; \
			ln -s ${HOME}/.uv/bin/uvx ${HOME}/.local/bin/uvx; \
		fi; \
     else \
        echo "`uv --version` is already installed."; \
     fi

install-iceberg-jar:
	uv sync
	mkdir -p $(JARS_DIR)
	@if [ ! -f $(JARS_DIR)/$(JAR_NAME) ]; then \
		echo "Downloading Iceberg JAR..."; \
		curl -L $(JAR_URL) -o $(JARS_DIR)/$(JAR_NAME); \
	else \
		echo "Iceberg JAR already exists."; \
	fi

install-dependencies:
	uv venv --clear --system-site-packages && \
	uv sync


install: | install-uv install-dependencies install-iceberg-jar

ruff:
	uv run ruff check

mypy:
	uv run mypy --install-types --non-interactive --config=pyproject.toml

lint: | ruff mypy

format: 
	uv run ruff format

format-check: 
	uv run ruff format --check

precommit: | format-check lint all-test

all-test:
	uv run pytest tests/ ${PYTEST_ARGS}

unit-test:
	uv run pytest tests/ -s -m "not integration" ${PYTEST_ARGS}

integration-test:
	uv run pytest tests/ -m "integration" ${PYTEST_ARGS}
