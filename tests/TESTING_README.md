# Unit Tests
Unit tests should be run one at a time and not in parallel. Many of the tests might use the same database which can cause conflicts if run in parallel.

## Running Unit Tests
You should be in the `tests` folder to run the tests. 

To run all unit_tests you can use the provided script
```sh
python3 run_unit_tests.sh>
```


To run individual tests you can run them using the following command.

```sh
python3 -m unittest unit_tests/<test_file.py>
```

# Testing Configuration 
By default the tests use a separate configuration section underneath the default in `test`.
For help with creating test users/dbs with `mysql` see the [DATABASE README](https://github.com/parnell/sqlalchemy-extensions/blob/main/README_DATABASE.md)


## Example toml extended with test section
This `.toml` file has test sections for both `sqlite3` and `mysql`. With this setting it will use `sqlite3.test` for testing. And if default was changed to `default="mysql"` then it would use `mysql.test`

```toml
default="sqlite3" 

[sqlite3]
# sqlite3 settings
url="sqlite:///:memory:"

[sqlite3.test]
# sqlite3 test settings
url="sqlite:///:memory:"

[mysql]
# mysql settings
url="mysql+pymysql://<username>:<password>@<host>/<database>?charset=utf8mb4"

[mysql.test]
# mysql test settings
# We aren't specifying the url to allow dynamic databases names
username="<database tester name>"
password="<database tester password>"
driver="mysql+pymysql"
host="localhost"


[logging]
# Logging options 
# level: set logging level Valid levels 
#        "" for nothing, "debug", "info", "warn", "error", "critical" 
level=""
```
