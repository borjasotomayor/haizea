#!/bin/bash

rm reservations.db
sqlite3 reservations.db '.read create_db.sql'
sqlite3 reservations.db '.read insert_types.sql'
sqlite3 reservations.db '.read insert_testdata.sql'