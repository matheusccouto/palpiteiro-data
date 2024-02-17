build: pip gcloud
	pip install -r requirements.txt -r requirements-dev.txt
	dbt deps
pip:
	pip install --upgrade pip wheel
gcloud:
	gcloud auth login