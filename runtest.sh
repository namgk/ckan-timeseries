#!/bin/bash

nosetests --ckan --with-pylons=test-core-2.8.ini ckanext/timeseries/tests/test_create_short.py
