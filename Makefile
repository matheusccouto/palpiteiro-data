build: pip
	pip install -r requirements.txt -r requirements-dev.txt
	dbt deps
pip:
	pip install --upgrade pip wheel
