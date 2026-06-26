.PHONY: up down logs lint test

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

lint:
	ruff check .

test:
	pytest tests/unit/ -v
