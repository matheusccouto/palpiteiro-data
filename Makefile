codespaces:
	echo -e "$GCP_KEYFILE" > ~/keyfile.json
	pip install --upgrade pip wheel
	pip intall -r requirements.txt -r requirements-dev.txt