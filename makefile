.PHONY: install test lint format clean run

install:
	uv sync --all-extras

test:
	uv run pytest tests/ -v

lint:
	uv run ruff check src/

format:
	uv run black src/
	uv run ruff check --fix src/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf dist/
	rm -rf build/

run:
	python -m oracle_to_excel.main --session 12345
