Nexmatix Cloud SQL
==================

Overview
--------

There are three tables:

* `valve` - Contains the configuration for each valve.
* `valve_status` - Contains the current status of each valve.
* `valve_alert` - Contains the alerts for each valve.

The `valve` and `valve_status` tables are updated by Cloud Functions.
The `valve_alert` table is maintained by triggers that implement the alerting logic.
See `create_tables.sql` and `install_triggers.sql` for more details.

Deployment
----------

1. Create the instance

    The remaining instructions assume the instance is named `nexmatixmvd`.
    
    The database can be accessed with a mysql client via

        gcloud beta sql connect nexmatixmvd --user=root

2. Create a Cloud Function user

        gcloud sql users create cfuser 'cloudsqlproxy~%' --instance=nexmatixmvd --password=

3. Enable the installation of triggers

        gcloud sql instances patch nexmatixmvd --database-flags log_bin_trust_function_creators=on

4. Create the database

    Via the mysql client:

        create database smartdevice_repo;

5. Create the tables

    Via the mysql client:

        source create_tables.sql;


6. Install triggers

    Via the mysql client:

        source install_triggers.sql;
