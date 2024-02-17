build: pip gcloud
	pip install -r requirements.txt -r requirements-dev.txt
	dbt deps
pip:
	pip install --upgrade pip wheel
gcloud:
	gcloud auth application-default login palpiteiro-dev --impersonate-service-account github-palpiteiro-data@palpiteiro-dev.iam.gserviceaccount.com
	gcloud auth application-default login [ACCOUNT] [--no-browser] [--client-id-file=CLIENT_ID_FILE] [--disable-quota-project] [--no-launch-browser] [--login-config=LOGIN_CONFIG] [--scopes=SCOPE,[SCOPE,…]] [GCLOUD_WIDE_FLAG …]