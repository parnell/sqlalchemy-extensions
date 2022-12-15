# Added SQLAlchemy Functionality
Additional functions for session

* insert_ignore
* insert_ignore_all
* find_keys

## Logical Key Functionality
* linsert_ignore
* linsert_ignore_all
* lget
* lexists

# Installing
## Requirements
* Python 3.11+ 
* SQLAlchemy 1.4+

## Pip installation
```sh
python3 -m pip install git+https://github.com/parnell/sqlalchemy-extensions.git
```

# Using
SQLAlchemy-Extensions is made so it can be a drop in add functionality on top of existing SQLAlchemy code. In the following example `Session` and `declarative_base` have been replaced with the `sqlalchemy_extensions` equivalents.

```python
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Mapped, mapped_column
## Session and declarative_base from sqlalchemy_extensions
from sqlalchemy_extensions import Session
from sqlalchemy_extensions.ext.declarative import declarative_base

Base = declarative_base()

class MyClass(Base):
    """Create your SQLAlchemy class as normal"""

    __tablename__ = "tclass"

    id: Mapped[int] = mapped_column(primary_key=True)


## Connect our engine to our session.
engine = create_engine("sqlite://", echo=False)
Base.metadata.create_all(engine)
session = Session(engine)

# Add an object
session.add(MyClass(id=1))
session.commit()
dbobjs = session.scalars(select(MyClass)).all()
assert len(dbobjs) == 1

# SQLAlchemy-Extensions: insert_ignore function
# SQLAlchemy.add() will throw IntegrityError, UNIQUE constraint failed
# insert_ignore() will only add if the id is not present in the database
session.insert_ignore(MyClass(id=1))
session.commit()
dbobjs = session.scalars(select(MyClass)).all()
assert len(dbobjs) == 1
```


For some full code examples see [examples](https://github.com/parnell/sqlalchemy-extensions/blob/main/examples)


# Configuration 
While you can pass in connection urls, or dictionaries with config variables, you can also set defaults using a `config.toml`, locations can be found in (#Creating the `config.toml` file)
For help with creating initial users see the [DATABASE README](https://github.com/parnell/sqlalchemy-extensions/blob/main/README_DATABASE.md)

## Creating the `config.toml` file
This project uses default options loaded from a `.toml` file. This file is located in the system config directory that is different for every system. 

It will check for the config.toml in the following locations in order.

### Config locations
These locations will be checked in order
* ```~/.config/sqlalchemy-extensions/config.toml```
* ```<config directory for user(system dependent)>/sqlalchemy-extensions/config.toml```




```toml
default="sqlite3" 

[sqlite3]
# sqlite3 settings
url="sqlite:///:memory:"

[mysql]
# mysql settings
url="mysql+pymysql://<username>:<password>@<host>/<database>?charset=utf8mb4"


[logging]
# Logging options 
# level: set logging level Valid levels 
#        "" for nothing, "debug", "info", "warn", "error", "critical" 
level=""
```
