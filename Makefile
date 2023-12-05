GLOBAL_PYTHON = $(shell which python3)
LOCAL_PYTHON = ./.venv/bin/python
LOCAL_PRE_COMMIT = ./.venv/lib/python*/site-packages/pre_commit

setup: venv install make_local pre-commit

venv: $(GLOBAL_PYTHON)
	@echo "Creating .venv..."
	python3 -m venv .venv
	@echo "Activating .venv..."
	@echo "Run 'deactivate' command to exit virtual environment."
	@echo "For more info, see https://docs.python.org/3/library/venv.html."
	. ./.venv/bin/activate

install: ${LOCAL_PYTHON}
	@echo "Installing dependencies..."
	poetry install --no-root --sync

make_local:
	if [ ! -d local/ ]; then mkdir -p local/; fi

pre-commit: ${LOCAL_PYTHON} ${LOCAL_PRE_COMMIT}
	@echo "Setting up pre-commit..."
	@if [ -f ${LOCAL_PRE_COMMIT} ]; then \
		./.venv/bin/pre-commit install; \
	else \
		echo "pre-commit not found in ${LOCAL_PRE_COMMIT}"; \
		echo "Trying to reinstall pre-commit..."; \
		./.venv/bin/pip install pre-commit; \
		./.venv/bin/pre-commit install; \
	fi
	./.venv/bin/pre-commit autoupdate



clean:
	rm -rf .git/hooks
	rm -rf .venv
	rm -f poetry.lock
