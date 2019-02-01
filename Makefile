.PHONY: all test build clean-pyc clean-build
	
all: setup

venv/bin/activate:
	if which virtualenv-2.7 >/dev/null; then virtualenv-2.7 -p /usr/bin/python3.6 venv; else virtualenv -p /usr/bin/python3.6 venv; fi

run: venv/bin/activate requirements/development.txt
	. venv/bin/activate; python examples/instamsg-example.py

setup: venv/bin/activate requirements/development.txt
	. venv/bin/activate; pip install -Ur requirements/development.txt

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
