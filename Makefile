PYTHON ?= python
PYDOCTOR ?= pydoctor

TEST_COMMAND = $(PYTHON) test

STORM_POSTGRES_URI = postgres:storm_test
STORM_POSTGRES_HOST_URI = postgres://localhost/storm_test
STORM_MYSQL_URI = mysql:storm_test
STORM_MYSQL_HOST_URI = mysql://localhost/storm_test

export STORM_POSTGRES_URI
export STORM_POSTGRES_HOST_URI
export STORM_MYSQL_URI
export STORM_MYSQL_HOST_URI

all: build

build:
	$(PYTHON) setup.py build_ext -i

check: build
	# Run the tests once with C extensions and once without them.
	$(TEST_COMMAND) && STORM_CEXTENSIONS=0 $(TEST_COMMAND)

doc:
	$(PYDOCTOR) --make-html --html-output apidoc --add-package storm

release:
	$(PYTHON) setup.py sdist

clean:
	rm -rf build
	rm -rf build-stamp
	rm -rf dist
	rm -rf storm.egg-info
	rm -rf debian/files
	rm -rf debian/python-storm
	rm -rf debian/python-storm.*
	find . -name "*.so" -type f -exec rm -f {} \;
	find . -name "*.pyc" -type f -exec rm -f {} \;
	find . -name "*~" -type f -exec rm -f {} \;

.PHONY: all build test
