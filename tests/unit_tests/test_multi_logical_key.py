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


class DClass(Base):
    __tablename__ = "dclasses"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)
    name2: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)


class TestMultiLogicalKeyClass(unittest.TestCase):
    def test_single_object_linsert_ignore_lexists_id(self):

        with create_test_db() as db:
            size = 10
            sources = {str(x): DClass(name=str(x), name2=str(x)) for x in range(size)}

            with db.Session.begin() as s:
                s.add_all(sources.values())

            test_name = next(iter(sources))
            with db.Session() as s:
                dup_s = DClass(name=test_name, name2=test_name)
                dup_s = s.linsert_ignore(dup_s, commit=True)
                # check id in session
                self.assertEqual(dup_s.id - 1, int(test_name))
            # check id out of session
            self.assertEqual(dup_s.id - 1, int(test_name))

            with db.Session() as s:
                dup_s = DClass(name=test_name, name2=test_name)
                dup_s = s.linsert_ignore(dup_s, commit=True)
                # check id in session
                self.assertEqual(dup_s.id - 1, int(test_name))

            # check id out of session
            self.assertEqual(dup_s.id - 1, int(test_name))


if __name__ == "__main__":
    unittest.main()
