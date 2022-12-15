"""Unit tests for db.py """
import unittest

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from sqlalchemy_extensions.orm import Base


class TClass(Base):
    __tablename__ = "tclass"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)


class TClass2(Base):
    __tablename__ = "tclasses_multi_id"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    id2: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)


class DClass(Base):
    __tablename__ = "dclasses"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)
    name2: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)


class TestDeclarativeBase(unittest.TestCase):
    def test_log_vals(self):
        o = TClass(name="1")
        self.assertEqual(o._log_vals, ("1",))

    def test_key_vals(self):
        o = TClass(id=1)
        self.assertEqual(o._key_vals, (1,))

    def test_tuple_log_vals(self):
        o = DClass(name="1", name2="1")
        self.assertEqual(o._log_vals, ("1", "1"))

    def test_tuple_key_vals(self):
        o = TClass2(id=1, id2=1)
        self.assertEqual(o._key_vals, (1, 1))

    def test_repr(self):
        o = TClass(name="1")
        self.assertTrue("TClass" in str(o))
        self.assertTrue("id=None" in str(o))
        self.assertTrue("name=1" in str(o))

    def test_to_json(self):
        name = "1"
        o = TClass(name=name)
        js = o.to_json(include_all_columns=True)
        self.assertEqual(js["name"], name)
        self.assertEqual(js["id"], None)

    def test_to_json2(self):
        o = DClass(id=1)
        js = o.to_json(include_all_columns=True)
        self.assertEqual(js["id"], 1)
        self.assertEqual(js["name"], None)
        self.assertEqual(js["name2"], None)


if __name__ == "__main__":
    unittest.main()
