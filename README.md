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

