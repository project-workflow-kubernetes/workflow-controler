REMOTE_REPO=liabifano

help:
	@echo "build-local"
	@echo "build-and-push"
	@echo "test"


build-local:
	@docker build -t  ${REMOTE_REPO}/workflow .


build-and-push:
	@docker login --username=${DOCKER_USER} --password=${DOCKER_PASS} 2> /dev/null
	@docker build -t ${REMOTE_REPO}/workflow .
	@docker push ${REMOTE_REPO}/workflow


test: build-local
	-@docker run ${REMOTE_REPO}/workflow /bin/bash -c "cd executor; py.test --verbose --color=yes"
	@docker rmi -f ${REMOTE_REPO}/workflow
