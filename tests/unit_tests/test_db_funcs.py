"""Unit tests for db.py """
import unittest

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column
from sqlgold import DB
from sqlgold.utils.test_db_utils import create_test_db, set_test_config

from sqlalchemy_extensions import sessionmaker
from sqlalchemy_extensions.orm import Base

set_test_config("sqlalchemy-extensions")

DB.default_base = Base
DB.default_sessionmaker = sessionmaker


class TClass(Base):
    __tablename__ = "tclasses"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)


class TClass2(Base):
    __tablename__ = "tclasses_multi_id"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    id2: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)


class TestDBFuncs(unittest.TestCase):
    def test_find_keys_single(self):
        name = "1"
        with create_test_db() as db:
            with db.Session() as s:
                o = TClass(name=name)
                s.add(o)
                s.commit()
                robj = s.find_keys(TClass, name)
                self.assertEqual(robj, o.id)
                robj = s.lget(TClass, "NotFound")
                self.assertIsNone(robj)

    def test_find_keys_multiple(self):
        name = "1"
        o = TClass2(name=name, id=int(name), id2=int(name))
        with create_test_db() as db:
            with db.Session() as s:
                o = TClass2(name=name, id=int(name), id2=int(name))
                s.add(o)
                s.commit()
                ## We should have an id
                self.assertEqual(o.id, 1)

                ## We should have matching ids
                robj = s.find_keys(TClass2, name)
                self.assertEqual(robj.id, 1)
                self.assertEqual(robj, (o.id, o.id2))

    def test_lget_single(self):
        name = "1"
        with create_test_db() as db:
            with db.Session() as s:
                o = TClass(name=name)
                s.add(o)
                s.commit()
                robj = s.lget(TClass, name)
                self.assertEqual(robj.id, o.id)
                robj = s.lget(TClass, "NotFound")
                self.assertIsNone(robj)

    def test_count(self):
        size = 2
        with create_test_db() as db:
            with db.Session() as s:
                os = [TClass(name=str(x)) for x in range(size)]
                s.add_all(os)
                s.commit()
                self.assertEqual(s.count(TClass), size)


if __name__ == "__main__":
    unittest.main()
