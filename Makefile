PROJECT_NAME = de-orpiment-group-project
PYTHON_INTERPRETER = python
WD=$(shell pwd)
SHELL := /bin/bash
PYTHONPATH=${WD}
PIP:=pip

# Build environment
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up venv."
	( \
	    $(PYTHON_INTERPRETER) -m venv venv; \
	)

ACTIVATE_ENV := source venv/bin/activate

define execute_in_env
	$(ACTIVATE_ENV) && $(1)
endef

# Build environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)

# Set Up
bandit:
	$(call execute_in_env, $(PIP) install bandit)
coverage:
	$(call execute_in_env, $(PIP) install pytest-cov)
dev-setup: bandit coverage

# Run security test
security-test:
	$(call execute_in_env, bandit -lll -r ./clean_layer ./extract_layer)
check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=./clean_layer ./extract_layer tests/)

# Run all
all: create-environment requirements dev-setup security-test check-coverage

# Run script for pytest
# test-db:
# 	chmod +x test_db_script.sh
# 	.\test_db_script.sh
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -v)
run-pytest: unit-test

# Run bash scripts
create-bucket:
	chmod +x create_bucket.sh
	./create_bucket.sh
zip-layer:
	chmod +x upload_layer_func.sh
	./upload_layer_func.sh
run-script: create-bucket zip-layer