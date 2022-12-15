# DB Tip Sheet
## Mysql

### Creating a user
```sql
create user '<user>'@'<host>' IDENTIFIED by '<password>';

GRANT ALL PRIVILEGES ON <database>.* TO '<user>'@'<host>' IDENTIFIED BY '<password';
```

### Creating a test user
Grant access to only `<username>_*` tables

```sql
GRANT ALL PRIVILEGES ON `<test user>_%` .  * TO '<test user>'@'<host>';
```

### seeing admin tables
```sql
use mysql;
show tables;
```

