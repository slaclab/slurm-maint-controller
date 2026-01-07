ENVIRONMENT := venv
# CONTAINER_NAME := slaclab/sshkey-service
# CONTAINER_TAG := latest
# CONTAINER_RT := sudo podman
DOCKERHUB_USERNAME := slaclab
# CONTAINER_PREFIX := docker.io

dev:
    #hmm... need libffi-dev
	python3.9 -m venv $(ENVIRONMENT)
	cd $(ENVIRONMENT) && ./bin/pip install  -r ../requirements.txt


clean-dev:
	rm -rf $(ENVIRONMENT)

start-app:
	$(ENVIRONMENT)/bin/python3 app.py

test:
	$(ENVIRONMENT)/bin/python3 controller.py --dry-run --log-level=DEBUG

tests:
	@echo "Running test suite..."
	$(ENVIRONMENT)/bin/python3 tests/test_node_states.py
	$(ENVIRONMENT)/bin/python3 tests/test_node_counting.py
	@echo "All tests completed!"

# dockerhub-login:
# 	$(CONTAINER_RT) login --username $(DOCKERHUB_USERNAME) $(CONTAINER_PREFIX)/$(CONTAINER_NAME)

# build:
# 	$(CONTAINER_RT) build -t $(CONTAINER_PREFIX)/$(CONTAINER_NAME):$(CONTAINER_TAG) .

# push:
# 	$(CONTAINER_RT) push $(CONTAINER_PREFIX)/$(CONTAINER_NAME):$(CONTAINER_TAG)
