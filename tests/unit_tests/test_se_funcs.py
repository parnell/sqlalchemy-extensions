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
    name: Mapped[str] = mapped_column(String(32), index=True, logical_key=True)


class TClassOther(Base):
    __tablename__ = "tclasses_with_other"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(32), index=True, logical_key=True)
    other: Mapped[str] = mapped_column(String(32))


class TClass2(Base):
    __tablename__ = "tclasses_multi_id"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    id2: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(String(32), index=True, logical_key=True)


class DClass(Base):
    __tablename__ = "tclasses_multi_lid"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(32), index=True, logical_key=True)
    name2: Mapped[str] = mapped_column(String(32), index=True, logical_key=True)


class TestDBFuncs(unittest.TestCase):
    def test_linsert_update_insert(self):
        with create_test_db() as db:
            with db.Session() as s:
                o = TClassOther(name="1", other="1")
                s.add(o)
                s.commit()
                o = TClassOther(name="2", other="2")
                s.linsert_update(o)
                s.commit()
                o2 = s.get(TClassOther, 2)
                self.assertEqual(o2.id, 2)
                self.assertEqual(o2.other, "2")

    def test_linsert_update_update(self):
        with create_test_db() as db:
            with db.Session() as s:
                o = TClassOther(name="1", other="1")
                s.add(o)
                s.commit()
                o2 = TClassOther(name="1", other="2")
                s.linsert_update(o2)
                dbo = s.get(TClassOther, 1)
                self.assertEqual(o.id, o2.id)
                self.assertEqual(dbo.id, o2.id)
                self.assertEqual(dbo.other, o2.other)

    def test_linsert_update_all_insert(self):
        size = 2
        with create_test_db() as db:
            with db.Session() as s:
                os = [TClassOther(name=str(i), other=str(i)) for i in range(size)]
                s.add_all(os)
                s.commit()
                os2 = [
                    TClassOther(name=str(i), other=str(i))
                    for i in range(size, size + size)
                ]
                s.linsert_update_all(os2)
                s.commit()
                count = s.count(TClassOther)
                self.assertEqual(count, size + size)

    def test_linsert_update_all_update(self):
        size = 2
        with create_test_db() as db:
            with db.Session() as s:
                os = [TClassOther(name=str(i), other=str(i)) for i in range(size)]
                s.add_all(os)
                s.commit()
                os2 = [
                    TClassOther(name=str(i), other=str(i + size)) for i in range(size)
                ]
                s.linsert_update_all(os2)
                count = s.count(TClassOther)
                self.assertEqual(count, size)
                dbos = [s.get(TClassOther, i + 1) for i in range(size)]
                for i, dbo in enumerate(dbos):
                    self.assertEqual(dbo.id, i + 1)
                    self.assertEqual(dbo.name, str(i))
                    self.assertEqual(dbo.other, str(i + size))

    def test_linsert_update_all_mix(self):
        size = 2
        with create_test_db() as db:
            with db.Session() as s:
                os = [TClassOther(name=str(i), other=str(i)) for i in range(size)]
                s.add_all(os)
                s.commit()
                os2 = [
                    TClassOther(name=str(i), other=str(i + size))
                    for i in range(1, size + 1)
                ]
                s.linsert_update_all(os2)
                count = s.count(TClassOther)
                self.assertEqual(count, size + 1)
                dbos = [s.get(TClassOther, i + 1) for i in range(size + 1)]
                for i, dbo in enumerate(dbos):
                    self.assertEqual(dbo.id, i + 1)
                    self.assertEqual(dbo.name, str(i))
                    if i >= 1:
                        self.assertEqual(dbo.other, str(i + size))
                    else:
                        self.assertEqual(dbo.other, str(i))

    def test_attach_keys(self):
        with create_test_db() as db:
            with db.Session() as s:
                o = TClass(name="1")
                s.add(o)
                s.commit()

                o = TClass(name="1")
                s.attach_keys(o)
                self.assertEqual(o.id, 1)

    def test_attach_keys_all(self):
        size = 2
        with create_test_db() as db:
            with db.Session() as s:
                os = [TClass(name=str(i)) for i in range(size)]
                s.add_all(os)
                s.commit()

                os2 = [TClass(name=str(i)) for i in range(size)]
                s.attach_keys_all(os2)
                for o1, o2 in zip(os, os2):
                    self.assertEqual(o1.id, o2.id)

    def test_find_keys_all_single(self):
        size = 2
        with create_test_db() as db:
            with db.Session() as s:
                os = [TClass(name=str(i)) for i in range(size)]
                lids = [o._log_vals for o in os]
                s.add_all(os)
                s.commit()
                ids = s.find_keys_all(TClass, lids)
                self.assertEqual(ids[0][0], 1)
                self.assertEqual(ids[1][0], 2)

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

    def test_find_keys_multiple_lids(self):
        name = "1"
        with create_test_db() as db:
            with db.Session() as s:
                o = DClass(name=name, name2=name)
                s.add(o)
                s.commit()
                ## We should have an id
                self.assertEqual(o.id, 1)

                ## We should have matching ids
                oid = s.find_keys(DClass, [name, name])
                self.assertEqual(oid, 1)

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
