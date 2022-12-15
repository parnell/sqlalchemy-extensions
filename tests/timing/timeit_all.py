import statistics
import timeit
import uuid
from sqlalchemy_extensions import create_db

number = 2
repeat = 20
size = 10000
setup = """
size = {size}

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy_extensions import Base, Session, DB, create_db
import warnings
from sqlalchemy import exc as sa_exc

class TClass{ext}(Base):
    __tablename__ = "tclass_{ext}"
    __table_args__ = {{'extend_existing': True}}

    id{ext}: Mapped[int] = mapped_column(primary_key=True)
    name{ext}: Mapped[int] = mapped_column(logical_key=True)

with warnings.catch_warnings():
    warnings.simplefilter("ignore", category=sa_exc.SAWarning)
    db = create_db("sqlite:///:memory:")
    db.drop_all()
    db.create_all()
"""


def run(stmt, name):
    """All of these extensions (ext) are to avoid mysql failing on trying to recreate
    indexes.
    """
    
    times = []
    for r in range(repeat):
        ext = uuid.uuid4().hex    
        times.append(
            timeit.timeit(
                setup=setup.format(ext=ext, size=size),
                stmt=stmt.format(ext=ext),
                number=number,
            )
        )
    mean = statistics.mean(times)
    print(
        f"{name:>32}: count={size:.2f}, mean={mean:.2f}, min={min(times):.2f}, max={max(times):.2f}, "
        f"avg (stmts/s)={size/mean:.2f}"
    )


stmt = """
with db.Session() as s:
    s.add_all([TClass{ext}(name{ext}=i) for i in range(size)])
    s.commit()
"""
run(stmt, "add_all")

stmt = """
with db.Session() as s:
    s.insert_ignore_all([TClass{ext}(name{ext}=i) for i in range(size)])
    s.commit()
"""
run(stmt, "insert_ignore_all")

stmt = """
with db.Session() as s:
    for o in [TClass{ext}(name{ext}=i) for i in range(size)]:
        s.add(o)
    s.commit()
"""
run(stmt, "add")

stmt = """
with db.Session() as s:
    for o in [TClass{ext}(name{ext}=i) for i in range(size)]:
        s.insert_ignore(o)
    s.commit()
"""
run(stmt, "insert_ignore")

stmt = """
with db.Session() as s:
    for o in [dict(id{ext}=i, name{ext}=i) for i in range(size)]:
        s.get_or_create(TClass{ext}, o)
    s.commit()
"""
run(stmt, "get_or_create")


create_db().drop_db()
