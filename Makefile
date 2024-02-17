build: pip gcloud
	pip install -r requirements.txt -r requirements-dev.txt
	dbt deps
pip:
	pip install --upgrade pip wheel
gcloud:
	gcloud auth application-default login --impersonate-service-account github-palpiteiro-data@palpiteiro-dev.iam.gserviceaccount.com
	gcloud config set project palpiteiro-dev