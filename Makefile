PROJECT_NAME=workflow
TEST_PATH=./


help:
	@echo "install - install project in dev mode using conda"
	@echo "run - run main.py file"
	@echo "test -  run tests quickly within env: $(PROJECT_NAME)"
	@echo "lint - check code style"
	@echo "clean - remove build and python artifacts"
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove python artifacts"


test: clean-pyc
	@echo "\n--- If the env $(PROJECT_NAME) doesn't exist, run 'make install' before ---\n"n
	@echo "\n--- Running tests at $(PROJECT_NAME) ---\n"
	bash -c "source activate $(PROJECT_NAME) &&  py.test --verbose --color=yes $(TEST_PATH)"


test-in-docker:
	@echo "\n--- Make sure that your docker-machine is up ---\n"
	@echo "\n--- Building docker ---\n"
	@docker build -t test-$(PROJECT_NAME) .
	@echo "\n--- Running tests inside docker ---\n"
	-@docker run test-$(PROJECT_NAME) py.test --verbose --color=yes
	@docker rmi -f test-$(PROJECT_NAME)


install: clean
	-@conda env remove -yq -n $(PROJECT_NAME) # ignore if fails
	@conda create -y --name $(PROJECT_NAME) --file conda.txt
	@echo "\n --- Creating env: $(PROJECT_NAME) in $(shell which conda) ---\n"
	@echo "\n--- Installing dependencies ---\n"
	bash -c "source activate $(PROJECT_NAME) && pip install -e . && pip install -U -r requirements.txt && source deactivate"

run:
	@echo "\n--- If the env $(PROJECT_NAME) doesn't exist, run 'make install' before ---\n"
	bash -c "source activate $(PROJECT_NAME)"
	@echo "\n--- Running main.py file ---\n"
	python src/$(PROJECT_NAME)/main.py

lint:
	-@pylint src/**/*.py --output-format text --reports no --msg-template "{path}:{line:04d}:{obj} {msg} ({msg_id})" | sort


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
