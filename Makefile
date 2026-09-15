.PHONY: venv ingest clean aggregate bench notebook

VENV := venv
PYTHON := $(VENV)/bin/python

venv:
	python3.12 -m venv $(VENV)
	$(PYTHON) -m pip install -r requirements.txt

ingest:
	$(PYTHON) src/ingest.py

clean: ingest
	$(PYTHON) src/clean.py

aggregate: clean
	$(PYTHON) src/transform.py

bench:
	$(PYTHON) src/benchmark.py

notebook:
	$(PYTHON) -m jupyter notebook notebooks/exploration.ipynb
