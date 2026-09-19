.PHONY: install demo serve test lint clean poll

install:          ## create a venv and install dependencies
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt
	.venv/bin/pip install pytest pytest-asyncio pyflakes

demo:             ## seed the offline corpus and print the ranked feed
	.venv/bin/python main.py demo

serve:            ## run the API with the live pipeline
	.venv/bin/python main.py serve

poll:             ## fetch all due sources once, then exit
	.venv/bin/python main.py poll-once

test:             ## run the test suite
	.venv/bin/python -m pytest tests/ -q

lint:             ## static check for undefined and unused names
	.venv/bin/python -m pyflakes newsagg/ tests/

clean:            ## remove the database and caches
	rm -f newsagg.db newsagg.db-wal newsagg.db-shm
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache
