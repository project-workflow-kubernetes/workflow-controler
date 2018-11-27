REMOTE_REPO=liabifano
DOCKER_NAME=workflow
DOCKER_LABEL=latest
GIT_MASTER_HEAD_SHA:=$(shell git rev-parse --short=7 --verify HEAD)
TEST_PATH=./
PROJECT_NAME=workflow


help:
	@echo "build"
	@echo "push"


build:
	@docker build -t ${REMOTE_REPO}/${DOCKER_NAME}:${DOCKER_LABEL} .
	@docker run ${REMOTE_REPO}/${DOCKER_NAME}:${DOCKER_LABEL} /bin/bash -c "cd /workflow; py.test --verbose --color=yes"


push:
	@docker tag ${REMOTE_REPO}/${DOCKER_NAME}:${DOCKER_LABEL} ${REMOTE_REPO}/${DOCKER_NAME}:${GIT_MASTER_HEAD_SHA}
	@echo "${DOCKER_PASSWORD}" | docker login -u="${DOCKER_USERNAME}" --password-stdin
	@docker push ${REMOTE_REPO}/${DOCKER_NAME}:${GIT_MASTER_HEAD_SHA}


clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +


clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +


clean: clean-build clean-pyc


test: clean-pyc
	@echo "\n--- If the env $(PROJECT_NAME) doesn't exist, run 'make install' before ---\n"n
	@echo "\n--- Running tests at $(PROJECT_NAME) ---\n"
	bash -c "source activate $(PROJECT_NAME) &&  py.test --verbose --color=yes $(TEST_PATH)"


install: clean
	-@conda env remove -yq -n $(PROJECT_NAME) # ignore if fails
	@conda create -y --name $(PROJECT_NAME) --file conda.txt
	@echo "\n --- Creating env: $(PROJECT_NAME) in $(shell which conda) ---\n"
	@echo "\n--- Installing dependencies ---\n"
	bash -c "source activate $(PROJECT_NAME) && pip install -e . && pip install -U -r requirements.txt && source deactivate"
