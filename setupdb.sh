#!/bin/sh

# use 'dropdb disk-catalogue' to start from scratch

set -e
createdb -E UTF8 disk-catalogue
psql disk-catalogue < disk-catalogue.sql
