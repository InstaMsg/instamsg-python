.PHONY: all test build clean-pyc clean-build
	
all: setup

venv/bin/activate:
	if which virtualenv-2.7 >/dev/null; then virtualenv-2.7 -p python3.7 venv -v --no-wheel; else virtualenv -p python3.7 venv -v --no-wheel; fi

run: venv/bin/activate
	. venv/bin/activate; python examples/instamsg-example.py

setup: venv/bin/activate
	. venv/bin/activate; pip install wheel --no-cache; pip install -Ur requirements/development.txt; pip install -Ur requirements/production.txt

build: clean-build clean-pyc; python setup.py build

install: clean-build clean-pyc; python setup.py install

clean-build:
	rm --force --recursive build
	rm --force --recursive dist
	rm --force --recursive *.egg-info

clean-pyc:
	find . -name '*.pyc' -exec rm --force {} \;
	find . -name '*.pyo' -exec rm --force {} \;
	find . -name '*~' -exec rm --force  {} \;

test: 
	tox tests
