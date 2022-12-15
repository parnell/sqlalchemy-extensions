from sqlalchemy import create_engine, select
from sqlalchemy.orm import Mapped, mapped_column
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

# Cleanup
obj = session.merge(MyClass(id=1))
session.delete(obj)
session.commit()

# Add multiple objects
session.add_all([MyClass(id=i) for i in range(10)])
session.commit()
dbobjs = session.scalars(select(MyClass)).all()
assert len(dbobjs) == 10

# New: SQLAlchemy-Extensions insert_ignore_all
# SQLAlchemy.add_all() will throw IntegrityError, UNIQUE constraint failed
# insert_ignore_all() will only add if the id is not present in the database
session.insert_ignore_all([MyClass(id=i) for i in range(10)])
session.commit()
dbobjs = session.scalars(select(MyClass)).all()
assert len(dbobjs) == 10
