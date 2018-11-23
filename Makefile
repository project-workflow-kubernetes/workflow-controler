REMOTE_REPO=liabifano
DOCKER_NAME=workflow
DOCKER_LABEL=latest
GIT_MASTER_HEAD_SHA:=$(shell git rev-parse --short=7 --verify HEAD)
TEST_PATH=./


help:
	@echo "build"
	@echo "push"


build:
	@docker build -t ${REMOTE_REPO}/${DOCKER_NAME}:${DOCKER_LABEL} .
	@docker run ${REMOTE_REPO}/${DOCKER_NAME}:${DOCKER_LABEL} /bin/bash -c "cd workflow; py.test --verbose --color=yes"


push:
	@docker tag ${REMOTE_REPO}/${DOCKER_NAME}:${DOCKER_LABEL} ${REMOTE_REPO}/${DOCKER_NAME}:${GIT_MASTER_HEAD_SHA}
	@echo "${DOCKER_PASSWORD}" | docker login -u="${DOCKER_USERNAME}" --password-stdin
	@docker push ${REMOTE_REPO}/${DOCKER_NAME}:${GIT_MASTER_HEAD_SHA}
