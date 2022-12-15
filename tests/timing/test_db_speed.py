import inspect
import unittest

from sqlalchemy import String, select
from sqlalchemy.orm import Mapped, mapped_column
from sqlgold import DB
from sqlgold.utils.test_db_utils import create_test_db, set_test_config
from utils.timer_utils import Timer

from sqlalchemy_extensions import sessionmaker
from sqlalchemy_extensions.orm import Base

set_test_config("sqlalchemy-extensions")

DB.default_base = Base
DB.default_sessionmaker = sessionmaker


class TClass(Base):
    __tablename__ = "tclass"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)


def func_name() -> str:
    return inspect.stack()[1][3]


class TestStraight(unittest.TestCase):
    def test_insert_speed_tclass(self):
        with create_test_db() as db:
            size = 1000

            sources = {str(x): TClass(name=str(x)) for x in range(size)}
            with Timer(
                f"{func_name()}:Straight insert size={size}", count=size
            ), db.Session.begin() as s:
                s.add_all(sources.values())

                names = [x.name for x in sources.values()]

            with Timer(f"select={size}"), db.Session.begin() as s:
                stmt = select(TClass.id).where(TClass.name.in_(names))
                ids = set(s.scalars(stmt))
                assert len(ids) == size
                sources = {str(x): TClass(name=str(x)) for x in range(size * 2)}
                s.add_all(sources.values())

    def test_straight_insert_speed_tclass(self):
        with create_test_db() as db:
            size = 100000

            sources = {str(x): TClass(name=str(x)) for x in range(size)}

            with Timer(
                f"{func_name()}:Straight insert size={size}", count=size
            ), db.Session.begin() as s:
                s.add_all(sources.values())

    def test_linsert_ignore_speed_tclass_1000(self):
        with create_test_db() as db:
            size = 1000

            sources = {str(x): TClass(name=str(x)) for x in range(size)}

            with Timer(
                f"{func_name()}:linsert_ignore size={size}", count=size
            ), db.Session.begin() as s:
                s.linsert_ignore(sources.values())
            with Timer(
                f"{func_name()}:linsert_ignore duplicates size={size}", count=size
            ), db.Session.begin() as s:
                s.linsert_ignore(sources.values())

    def test_linsert_ignore_speed_tclass_dups_100000(self):
        with create_test_db() as db:

            size = 100000

            sources = {str(x): TClass(name=str(x)) for x in range(size)}

            with Timer(
                f"{func_name()}:linsert_ignore size={size}", count=size
            ), db.Session() as s:
                s.linsert_ignore(sources.values(), commit=True)
            with Timer(
                f"{func_name()}:linsert_ignore duplicates size={size}", count=size
            ), db.Session() as s:
                s.linsert_ignore(sources.values(), commit=True)

    def test_linsert_ignore_speed_tclass_mix_200000(self):
        with create_test_db() as db:

            size = 100000
            dup_size = 200000

            sources = {str(x): TClass(name=str(x)) for x in range(size)}

            with Timer(
                f"{func_name()}:linsert_ignore size={size}", count=size
            ), db.Session() as s:
                s.linsert_ignore(sources.values(), commit=True)
            sources = {str(x): TClass(name=str(x)) for x in range(dup_size)}
            with Timer(
                f"{func_name()}:linsert_ignore duplicates size={dup_size}",
                count=dup_size,
            ), db.Session() as s:
                s.linsert_ignore(sources.values(), commit=True)


if __name__ == "__main__":
    unittest.main()
