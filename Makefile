GRADLE ?= ./gradlew

.PHONY: test build run

test:
	$(GRADLE) test

build:
	$(GRADLE) build

run:
	$(GRADLE) shadowJar
	docker compose up
