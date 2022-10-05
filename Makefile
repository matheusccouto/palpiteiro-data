build: pip
	printenv GCP_KEYFILE > /home/vscode/keyfile.json
	pip install --upgrade pip wheel
	pip install -r requirements.txt -r requirements-dev.txt
pip:
	pip install --upgrade pip wheel
diagrams: pip
	sudo apt install graphviz -y
	pip install diagrams