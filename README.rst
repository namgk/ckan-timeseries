
=============
ckanext-datastore_ts
=============

Timeseries data is streams of data that is indexed by timestamp, such as sensing data. Current default CKAN Datastore plugin only index data based on an auto-incremental integer with no support for timeseries data. Perceived that timeseries data is an important capability when working with sensor network, a new plugin that supports operations based on timestamp natively would be necessary. Thus, the purpose of this repository.

------------
Changes
------------
* If you are upgrading from version < 0.1.0, the schema has changed. A command has been created to upgrade the schema. Please run the following command:
```
paster --plugin=ckan datastore_ts -c <path to ini configuration file> upgrade-schema autogen_timestamp _autogen_timestamp
```

------------
Requirements
------------

Being developed under CKAN 2.5.2

Requires iso8601:

``` pip install iso8601 ```

------------
Installation
------------

.. Add any additional install steps to the list below.
   For example installing any non-Python dependencies or adding any required
   config settings.

To install ckanext-datastore_ts:

1. Activate your CKAN virtual environment, for example::

     . /usr/lib/ckan/default/bin/activate

2. Install the ckanext-datastore_ts Python package into your virtual environment::

     pip install ckanext-timeseries

3. Add ``timeseries`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu::

     sudo service apache2 reload


---------------
Config Settings
---------------
CKAN Timeseries uses configurations from Datastore plugin so make sure those are set. In brief:

sqlalchemy.url = postgresql://ckan_default:pass@localhost/ckan_default

ckan.datastore.write_url = postgresql://ckan_default:password@localhost/datastore_default

ckan.datastore.read_url = postgresql://datastore_default:password@localhost/datastore_default

CKAN Timeseries introduce a new configuration to set the maximum size of a resource table (as we are dealing with real time data). When a resource reaches this limit, it's table will be cleaned, the default 30% of the oldest data will be deleted. This percentage can be customized by user when creating a CKAN Timeseries resource. Please look at the wiki page for more detail.

ckan.timeseries.max_resource_size = 9000

------------------------
Development Installation
------------------------

To install ckanext-timeseries for development, activate your CKAN virtualenv and
do::

    git clone https://github.com/namgk/ckan-timeseries.git
    cd ckanext-timeseries
    python setup.py develop

-----------------
Running the Tests
-----------------

To run the tests, do::

    nosetests --nologcapture --ckan --with-pylons=test-core.ini ckanext/timeseries/tests/test.. .py

To run the tests and produce a coverage report, first make sure you have
coverage installed in your virtualenv (``pip install coverage``) then run::

    nosetests --nologcapture --with-pylons=test.ini --with-coverage --cover-package=ckanext.timeseries --cover-inclusive --cover-erase --cover-tests


---------------------------------
Registering ckanext-timeseries on PyPI
---------------------------------

ckanext-timeseries should be availabe on PyPI as
https://pypi.python.org/pypi/ckanext-timeseries. If that link doesn't work, then
you can register the project on PyPI for the first time by following these
steps:

1. Create a source distribution of the project::

     python setup.py sdist

2. Register the project::

     python setup.py register

3. Upload the source distribution to PyPI::

     python setup.py sdist upload

4. Tag the first release of the project on GitHub with the version number from
   the ``setup.py`` file. For example if the version number in ``setup.py`` is
   0.0.1 then do::

       git tag 0.0.1
       git push --tags


----------------------------------------
Releasing a New Version of ckanext-timeseries
----------------------------------------

ckanext-timeseries is availabe on PyPI as https://pypi.python.org/pypi/ckanext-timeseries.
To publish a new version to PyPI follow these steps:

1. Update the version number in the ``setup.py`` file.
   See `PEP 440 <http://legacy.python.org/dev/peps/pep-0440/#public-version-identifiers>`_
   for how to choose version numbers.

2. Create a source distribution of the new version::

     python setup.py sdist

3. Upload the source distribution to PyPI::

     python setup.py sdist upload

4. Tag the new release of the project on GitHub with the version number from
   the ``setup.py`` file. For example if the version number in ``setup.py`` is
   0.0.2 then do::

       git tag 0.0.2
       git push --tags
