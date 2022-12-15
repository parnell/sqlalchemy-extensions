"""Unit tests for db.py """
import unittest
from typing import List

from sqlalchemy import ForeignKey, String, select
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlgold import DB
from sqlgold.utils.test_db_utils import create_test_db, set_test_config

from sqlalchemy_extensions import sessionmaker
from sqlalchemy_extensions.orm import Base

set_test_config("sqlalchemy-extensions")


DB.default_base = Base
DB.default_sessionmaker = sessionmaker


class TChild(Base):
    __tablename__ = "tchild_nb"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)

    parent_id: Mapped[int] = mapped_column(
        ForeignKey("tparent_nb.id"), logical_key=True
    )


class TParent(Base):
    __tablename__ = "tparent_nb"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)

    children: Mapped[List["TChild"]] = relationship()


class TestPC_DBFuncs(unittest.TestCase):
    def test_parent_child(self):
        with create_test_db() as db:
            size = 10
            nchildren = 3
            sources = {str(x): TParent(name=str(x)) for x in range(size)}

            with db.Session() as s:
                s.linsert_ignore_all(sources.values(), commit=True)
                stmt = select(TParent)
                self.assertEqual(len(list(s.scalars(stmt))), size)
            all_children = []
            for s in sources.values():
                children = {
                    str(x): TChild(name=str(x), parent_id=s.id)
                    for x in range(nchildren)
                }
                all_children.extend(children.values())

            with db.Session() as s:
                s.linsert_ignore_all(all_children, commit=True)
                stmt = select(TParent)
                self.assertEqual(len(list(s.scalars(stmt))), size)
                stmt = select(TChild)
                self.assertEqual(len(list(s.scalars(stmt))), size * nchildren)

    def test_object_linsert_ignore_parent_child_ids(self):
        with create_test_db() as db:
            size = 10

            sources = {str(x): TParent(id=10 + x, name=str(x)) for x in range(size)}
            with db.Session() as s:
                s.linsert_ignore_all(sources.values(), commit=True)
                stmt = select(TParent)
                sources2 = list(s.scalars(stmt))
                self.assertEqual(len(sources), len(sources2))
                for x, y in zip(sources.values(), sources2):
                    self.assertEqual(x.id, y.id)
                    self.assertEqual(x.name, y.name)
            children = {
                str(x): TChild(id=10 + x, parent_id=10 + x, name=str(x))
                for x in range(size)
            }
            with db.Session() as s:
                s.linsert_ignore_all(children.values())
                stmt = select(TChild)
                children2 = list(s.scalars(stmt))
                self.assertEqual(len(children), len(children2))
                for x, y in zip(children.values(), children2):
                    self.assertEqual(x.id, y.id)
                    self.assertEqual(x.parent_id, y.parent_id)
                    self.assertEqual(x.name, y.name)

    def test_basic_linsert_parent_child_noparent_backpop_inserts(self):
        with create_test_db() as db:
            size = 3
            sources = {str(x): TParent(name=str(x)) for x in range(size)}
            all_children = {}
            for k, s in sources.items():
                children = {str(x): TChild(name=str(x)) for x in range(size)}
                all_children[k] = children.values()
                s.children = list(children.values())
            with db.Session() as s:
                inserted_objs = list(sources.values())
                s.linsert_ignore_all(inserted_objs, commit=True)

                dbobjs = list(s.scalars(select(TParent)))
                self.assertEqual(len(dbobjs), size)
                # check id in session
                for o in inserted_objs:
                    self.assertIsNotNone(o.id)
                    for c in o.children:
                        self.assertIsNotNone(c.id)
                        self.assertEqual(c.parent_id, o.id)
                        # self.assertEqual(c.parent, o)

            # check id out of session
            for o in inserted_objs:
                self.assertIsNotNone(o.id)
                for c in o.children:
                    self.assertIsNotNone(c.id)
                    self.assertEqual(c.parent_id, o.id)

    def test_basic_linsert_parent_child_noparent_backpop_duplicates(self):
        with create_test_db() as db:
            size = 3
            sources = {str(x): TParent(name=str(x)) for x in range(size)}
            all_children = {}
            for k, s in sources.items():
                children = {str(x): TChild(name=str(x)) for x in range(size)}
                all_children[k] = children.values()
                s.children = list(children.values())
            with db.Session(expire_on_commit=False) as s:
                inserted_objs = list(sources.values())
                s.linsert_ignore_all(inserted_objs, commit=True)
            ## Duplicates
            all_dup_children = {}
            for k, s in sources.items():
                dup_children = {str(x): TChild(name=str(x)) for x in range(size)}
                all_dup_children[k] = dup_children.values()
                s.children = list(dup_children.values())

            with db.Session() as s:
                inserted_objs = list(sources.values())
                s.linsert_ignore_all(inserted_objs, commit=True)

                dbobjs = list(s.scalars(select(TParent)))
                self.assertEqual(len(dbobjs), size)
                # check id in session
                for o in inserted_objs:
                    self.assertIsNotNone(o.id)
                    for c in o.children:
                        self.assertIsNotNone(c.id)
                        self.assertEqual(c.parent_id, o.id)

            # check id out of session
            for o in inserted_objs:
                self.assertIsNotNone(o.id)
                for c in o.children:
                    self.assertIsNotNone(c.id)
                    self.assertEqual(c.parent_id, o.id)

            for k, s in sources.items():
                for old, new in zip(all_children[k], all_dup_children[k]):
                    self.assertEqual(old.id, new.id)
                    self.assertEqual(old.name, new.name)
                    self.assertEqual(old.parent_id, new.parent_id)


if __name__ == "__main__":
    unittest.main()
