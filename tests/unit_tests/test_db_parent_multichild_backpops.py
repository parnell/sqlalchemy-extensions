"""Unit tests for db.py """
import unittest
from typing import List

from sqlalchemy import ForeignKey, String, select
from sqlalchemy.orm import Mapped, mapped_column, relationship

from sqlalchemy_extensions import Base
from sqlgold.utils._test_db_utils import create_test_db


class TParent(Base):
    __tablename__ = "tparent"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)

    children1: Mapped[List["TChild1"]] = relationship(
        back_populates="parent", cascade="all, delete-orphan"
    )
    children2: Mapped[List["TChild2"]] = relationship(
        back_populates="parent", cascade="all, delete-orphan"
    )


class TChild1(Base):
    __tablename__ = "tchild1"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)

    parent_id: Mapped[int] = mapped_column(ForeignKey(TParent.id), logical_key=True)
    parent: Mapped[TParent] = relationship(back_populates="children1")


class TChild2(Base):
    __tablename__ = "tchild2"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)

    parent_id: Mapped[int] = mapped_column(ForeignKey(TParent.id), logical_key=True)
    parent: Mapped[TParent] = relationship(back_populates="children2")


class TestPCB_DBFuncs(unittest.TestCase):
    def test_parent_child(self):
        with create_test_db() as db:
            size = 10
            nchildren = 3
            sources = {str(x): TParent(name=str(x)) for x in range(size)}

            with db.Session() as s:
                s.linsert_ignore_all(sources.values(), commit=True)
                stmt = select(TParent)
                self.assertEqual(len(list(s.scalars(stmt))), size)
            all_children1 = []
            all_children2 = []
            for s in sources.values():
                children1 = {
                    str(x): TChild1(name=str(x), parent=s) for x in range(nchildren)
                }
                all_children1.extend(children1.values())
                children2 = {
                    str(x): TChild2(name=str(x), parent=s) for x in range(nchildren)
                }
                all_children2.extend(children2.values())

            with db.Session() as s:
                s.linsert_ignore_all(all_children1, commit=True)
                s.linsert_ignore_all(all_children2, commit=True)
                stmt = select(TParent)
                self.assertEqual(len(list(s.scalars(stmt))), size)
                stmt = select(TChild1)
                self.assertEqual(len(list(s.scalars(stmt))), size * nchildren)
                stmt = select(TChild2)
                self.assertEqual(len(list(s.scalars(stmt))), size * nchildren)

    def test_basic_linsert_parent_child_backpop_inserts(self):
        with create_test_db() as db:
            size = 3
            nchildren = 3
            sources = {str(x): TParent(name=str(x)) for x in range(size)}
            for k, s in sources.items():
                children1 = {str(x): TChild1(name=str(x)) for x in range(nchildren)}
                s.children1 = list(children1.values())
                children2 = {str(x): TChild2(name=str(x)) for x in range(nchildren)}
                s.children2 = list(children2.values())

            with db.Session() as s:
                inserted_objs = list(sources.values())
                s.linsert_ignore_all(inserted_objs, commit=True)

                dbobjs = list(s.scalars(select(TParent)))
                self.assertEqual(len(dbobjs), size)
                # check id in session
                for o in inserted_objs:
                    self.assertIsNotNone(o.id)
                    for c in o.children1:
                        self.assertIsNotNone(c.id)
                        self.assertEqual(c.parent_id, o.id)
                        self.assertEqual(c.parent, o)
                    for c in o.children2:
                        self.assertIsNotNone(c.id)
                        self.assertEqual(c.parent_id, o.id)
                        self.assertEqual(c.parent, o)

            # check id out of session
            for o in inserted_objs:
                self.assertIsNotNone(o.id)
                for c in o.children1:
                    self.assertIsNotNone(c.id)
                    self.assertEqual(c.parent_id, o.id)
                    self.assertEqual(c.parent, o)
                for c in o.children2:
                    self.assertIsNotNone(c.id)
                    self.assertEqual(c.parent_id, o.id)
                    self.assertEqual(c.parent, o)

    def test_basic_linsert_parent_child_backpop_duplicates(self):
        with create_test_db() as db:
            size = 3
            nchildren = 2
            sources = {str(x): TParent(name=str(x)) for x in range(size)}
            all_children = {}
            for k, s in sources.items():
                children1 = {str(x): TChild1(name=str(x)) for x in range(nchildren)}
                all_children[k] = list(children1.values())
                s.children1 = list(children1.values())
                children2 = {str(x): TChild2(name=str(x)) for x in range(nchildren)}
                all_children[k].extend(children2.values())
                s.children2 = list(children2.values())

            with db.Session(expire_on_commit=False) as s:
                inserted_objs = list(sources.values())
                s.linsert_ignore_all(inserted_objs, commit=True)

            ## Duplicates
            all_dup_children = {}
            for k, s in sources.items():
                children1 = {str(x): TChild1(name=str(x)) for x in range(nchildren)}
                all_dup_children[k] = list(children1.values())
                s.children1 = list(children1.values())
                children2 = {str(x): TChild2(name=str(x)) for x in range(nchildren)}
                all_dup_children[k].extend(children2.values())
                s.children2 = list(children2.values())

            with db.Session() as s:
                inserted_objs = list(sources.values())
                s.linsert_ignore_all(inserted_objs, commit=True)

                dbobjs = list(s.scalars(select(TParent)))
                self.assertEqual(len(dbobjs), size)
                # check id in session
                for o in inserted_objs:
                    self.assertIsNotNone(o.id)
                    for c in o.children1:
                        self.assertIsNotNone(c.id)
                        self.assertEqual(c.parent_id, o.id)
                        self.assertEqual(c.parent, o)

            # check id out of session
            for o in inserted_objs:
                self.assertIsNotNone(o.id)
                for c in o.children1:
                    self.assertIsNotNone(c.id)
                    self.assertEqual(c.parent_id, o.id)
                    self.assertEqual(c.parent, o)

            for k, s in sources.items():
                for old, new in zip(all_children[k], all_dup_children[k]):
                    self.assertEqual(old.id, new.id)
                    self.assertEqual(old.name, new.name)
                    self.assertEqual(old.parent_id, new.parent_id)


if __name__ == "__main__":
    unittest.main()
