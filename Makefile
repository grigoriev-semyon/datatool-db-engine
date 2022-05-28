run-dev: install-dev
	docker-compose --file ./docker-compose.dev.yml up -d --build --force-recreate --remove-orphans


stop-dev:
	docker-compose --file ./docker-compose.dev.yml down -v


install:
	pip install -r requirements.txt


install-dev: install
	pip install -r requirements.dev.txt
