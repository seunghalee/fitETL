#!/bin/bash

# migrate tables from Postgres to Redshift
python3 ./src/postgres_to_redshift.py

# validate migration process
python3 ./src/validation.py
