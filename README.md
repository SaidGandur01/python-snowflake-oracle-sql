## Run the project

python auto_reload.py

## Request data structure

{
"username": "username",
"password": "password",
"account": "",
"warehouse": "warehouse",
"database": "database",
"schema": "schema",
"connectorName": "Snowflake"
}

### Considerations for snowflake

- The url account usually has this format: https://123456.us-east-1.snowflakecomputing.com/this means the account is 123456

### Considerations for mysql

- It's necessary to add in the request the host like this:

{
"username": "root",
"password": "root",
"account": "",
"warehouse": "",
"database": "databasename",
"schema": "",
"host": "127.0.0.1"
"connectorName": "mysql",
}
