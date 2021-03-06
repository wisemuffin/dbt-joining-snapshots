USE ROLE ACCOUNTADMIN; -- you need accountadmin (or security admin) for user creation, future grants


DROP USER IF EXISTS DBT_JOINING_SNAPSHOTS_DBT_CLOUD;
DROP ROLE IF EXISTS DBT_JOINING_SNAPSHOTS_TRANSFORMER;
DROP DATABASE IF EXISTS DBT_JOINING_SNAPSHOTS_DATABASE CASCADE;
DROP WAREHOUSE IF EXISTS DBT_JOINING_SNAPSHOTS_TRANSFORMING;

-- creating a warehouse
CREATE WAREHOUSE DBT_JOINING_SNAPSHOTS_TRANSFORMING WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE COMMENT = 'Warehouse to transform data';

-- creating database
CREATE DATABASE DBT_JOINING_SNAPSHOTS_DATABASE COMMENT = 'exploring dbt snapshots';

-- creating an access role
CREATE ROLE DBT_JOINING_SNAPSHOTS_TRANSFORMER COMMENT = 'Role for dbt';

-- granting role permissions
GRANT USAGE,OPERATE ON WAREHOUSE DBT_JOINING_SNAPSHOTS_TRANSFORMING TO ROLE DBT_JOINING_SNAPSHOTS_TRANSFORMER;
GRANT USAGE,CREATE SCHEMA ON DATABASE DBT_JOINING_SNAPSHOTS_DATABASE TO ROLE DBT_JOINING_SNAPSHOTS_TRANSFORMER;

GRANT USAGE ON DATABASE "FIVETRAN_DATABASE" TO ROLE DBT_JOINING_SNAPSHOTS_TRANSFORMER;
GRANT USAGE ON SCHEMA "FIVETRAN_DATABASE"."HUBSPOT" TO ROLE DBT_JOINING_SNAPSHOTS_TRANSFORMER;
GRANT SELECT ON ALL TABLES IN SCHEMA "FIVETRAN_DATABASE"."HUBSPOT" TO ROLE DBT_JOINING_SNAPSHOTS_TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA "FIVETRAN_DATABASE"."HUBSPOT" TO ROLE DBT_JOINING_SNAPSHOTS_TRANSFORMER;

GRANT USAGE ON DATABASE DBT_JOINING_SNAPSHOTS_DATABASE TO ROLE DBT_JOINING_SNAPSHOTS_TRANSFORMER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE DBT_JOINING_SNAPSHOTS_DATABASE TO ROLE DBT_JOINING_SNAPSHOTS_TRANSFORMER;
GRANT SELECT ON ALL TABLES IN DATABASE DBT_JOINING_SNAPSHOTS_DATABASE TO ROLE DBT_JOINING_SNAPSHOTS_TRANSFORMER;

GRANT USAGE ON FUTURE SCHEMAS IN DATABASE DBT_JOINING_SNAPSHOTS_DATABASE TO ROLE DBT_JOINING_SNAPSHOTS_TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN DATABASE DBT_JOINING_SNAPSHOTS_DATABASE TO ROLE DBT_JOINING_SNAPSHOTS_TRANSFORMER;


-- creating user and associating with role
CREATE USER DBT_JOINING_SNAPSHOTS_DBT_CLOUD PASSWORD='CHANGEME_PLEASE' DEFAULT_ROLE = DBT_JOINING_SNAPSHOTS_TRANSFORMER;
-- Make sure you change the above password! Add the flag -- MUST_CHANGE_PASSWORD = true to force a password change too
GRANT ROLE DBT_JOINING_SNAPSHOTS_TRANSFORMER TO USER DBT_JOINING_SNAPSHOTS_DBT_CLOUD;

-- grant all roles to sysadmin (always do this)
GRANT ROLE DBT_JOINING_SNAPSHOTS_TRANSFORMER  TO ROLE SYSADMIN;