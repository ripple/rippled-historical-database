Ripple Historical Database
==========================

![Travis Build Status](https://travis-ci.org/ripple/rippled-historical-database.svg?branch=develop)

SQL database as a canonical source of historical data in Ripple

#### Create the postgresql database schema found in `schema.sql`.

    ./init_db.sh


export DATABASE_URL=postgres://postgres:password@127.0.0.1:5432/test_db
