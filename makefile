PYTHON_VER			:= 3.11.6
VENV				:= $(PYTHON_VER)-pyawswrapper
S3_TEST_BUCKET		:= localstack-bucket

test_all:
	@if [ -e htmlcov ]; then\
		(rm -r htmlcov) \
	fi
	( \
		export SQLALCHEMY_WARN_20=1 && \
		pytest -n 8 -c tests/pytest.ini -v --dist=loadgroup --cov --cov-report=xml --cov-report=html --junitxml=xunit-result.xml tests \
	)

test: __require_target__
	( \
		export SQLALCHEMY_WARN_20=1 && \
		pytest -v $(TARGET) --cov --cov-report=xml --cov-report=html --junitxml=xunit-result.xml \
	)

env/localstack: env/localstack/start sleep env/localstack/init

env/localstack/install:
	( \
		pip install localstack \
	)

env/localstack/start:
	( \
		SERVICES=s3 DEBUG=1 localstack start -d && \
		localstack status services \
	)

env/localstack/init:
	( \
		awslocal s3 mb s3://$(S3_TEST_BUCKET) \
	)

env/init: virtualenv/install python/requirements

env/destroy: virtualenv/remove

env/upgrade:
	( \
		export PIP_CONSTRAINT='constraints.txt' && \
		. ~/.pyenv/versions/$(VENV)/bin/activate && \
		python -m pip install --upgrade pip && \
		pip install pip-review && \
		pip-review -i && \
		pip uninstall -y pip-review \
	)

env/freeze:
	( \
		. ~/.pyenv/versions/$(VENV)/bin/activate && \
		pip freeze \
	)

virtualenv/install:
	$(eval ret := $(shell pyenv versions | grep $(PYTHON_VER)))
	@if [ -n "$(ret)" ]; then \
		echo '$(PYTHON_VER) exists'; \
	else \
		(pyenv install $(PYTHON_VER)); \
	fi
	$(eval ret := $(shell pyenv versions | grep -P "\s$(VENV)(?=\s|$$)"))
	@if [ -n "$(ret)" ]; then \
		echo '$(VENV) exists'; \
	else \
		(pyenv virtualenv -f $(PYTHON_VER) $(VENV)); \
	fi
	pyenv versions

virtualenv/remove:
	(pyenv uninstall -f $(VENV))

python/requirements:
	$(eval TMP := $(shell mktemp -d))
	( \
		. ~/.pyenv/versions/$(VENV)/bin/activate && \
		python -m pip install --upgrade pip && \
		pip install -r dev_requirements.txt && \
		pip install -r requirements.txt \
	)
	( \
		. ~/.pyenv/versions/$(VENV)/bin/activate && \
		pip freeze \
	)

sleep:
	sleep 20

__require_target__:
	@[ -n "$(TARGET)" ] || (echo "[ERROR] Parameter [TARGET] is requierd" 1>&2 && echo "(e.g) make xxx TARGET=..." 1>&2 && exit 1)

