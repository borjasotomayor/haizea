#!/bin/bash

rm reservations_template.db
sqlite3 reservations_template.db '.read create_db.sql'
sqlite3 reservations_template.db '.read insert_types.sql'