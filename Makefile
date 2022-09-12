build:
	echo ${GCP_KEYFILE} > ./keyfile.json
	pip install --upgrade pip wheel
	pip install -r requirements.txt -r requirements-dev.txt