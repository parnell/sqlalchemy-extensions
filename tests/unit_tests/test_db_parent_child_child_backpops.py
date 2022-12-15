"""Unit tests for db.py """
import unittest
from typing import List

from sqlalchemy import ForeignKey, String, select
from sqlalchemy.orm import Mapped, mapped_column, relationship

from sqlalchemy_extensions.orm import Base
from sqlgold.utils._test_db_utils import create_test_db


class TParent(Base):
    __tablename__ = "tparent_r"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)

    children1: Mapped[List["TChild1"]] = relationship(
        back_populates="parent", cascade="all, delete-orphan"
    )


class TChild1(Base):
    __tablename__ = "tchild1_r"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)

    parent_id: Mapped[int] = mapped_column(ForeignKey(TParent.id), logical_key=True)
    parent: Mapped[TParent] = relationship(back_populates="children1")

    children2: Mapped[List["TChild2"]] = relationship(
        back_populates="child1", cascade="all, delete-orphan"
    )


class TChild2(Base):
    __tablename__ = "tchild2_r"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)

    child1_id: Mapped[int] = mapped_column(ForeignKey(TChild1.id), logical_key=True)
    child1: Mapped[TChild1] = relationship(back_populates="children2")

    def __repr__(self):
        return f"{self.__class__.__name__}(id={self.id}, name={self.name}, child1_id={self.child1_id})"


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

                for c in children1.values():
                    children2 = {
                        str(x): TChild2(name=str(x), child1=c) for x in range(nchildren)
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
                self.assertEqual(
                    len(list(s.scalars(stmt))), size * nchildren * nchildren
                )

    def test_basic_linsert_parent_child_backpop_inserts(self):
        with create_test_db() as db:
            size = 3
            nchildren = 2
            sources = {str(x): TParent(name=str(x)) for x in range(size)}
            for k, s in sources.items():
                children1 = {str(x): TChild1(name=str(x)) for x in range(nchildren)}
                s.children1 = list(children1.values())
                for c in children1.values():
                    children2 = {str(x): TChild2(name=str(x)) for x in range(nchildren)}
                    c.children2 = list(children2.values())

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
                        for c2 in c.children2:
                            self.assertIsNotNone(c2.id)
                            self.assertEqual(c2.child1_id, c.id)
                            self.assertEqual(c2.child1, c)

            # check id out of session
            for o in inserted_objs:
                self.assertIsNotNone(o.id)
                for c in o.children1:
                    self.assertIsNotNone(c.id)
                    self.assertEqual(c.parent_id, o.id)
                    self.assertEqual(c.parent, o)
                    for c2 in c.children2:
                        self.assertIsNotNone(c2.id)
                        self.assertEqual(c2.child1_id, c.id)
                        self.assertEqual(c2.child1, c)

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
                for c in children1.values():
                    children2 = {str(x): TChild2(name=str(x)) for x in range(nchildren)}
                    c.children2 = list(children2.values())

            with db.Session(expire_on_commit=False) as s:
                inserted_objs = list(sources.values())
                s.linsert_ignore_all(inserted_objs, commit=True)

                dbobjs = list(s.scalars(select(TParent)))
                self.assertEqual(len(dbobjs), size)

            ## Duplicates
            all_dup_children = {}
            for k, s in sources.items():
                children1 = {str(x): TChild1(name=str(x)) for x in range(nchildren)}
                all_dup_children[k] = list(children1.values())
                s.children1 = list(children1.values())
                for c in children1.values():
                    children2 = {str(x): TChild2(name=str(x)) for x in range(nchildren)}
                    c.children2 = list(children2.values())

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

                        for c2 in c.children2:
                            self.assertIsNotNone(c2.id)
                            self.assertEqual(c2.child1_id, c.id)
                            self.assertEqual(c2.child1, c)
            # check id out of session
            for o in inserted_objs:
                self.assertIsNotNone(o.id)
                for c in o.children1:
                    self.assertIsNotNone(c.id)
                    self.assertEqual(c.parent_id, o.id)
                    self.assertEqual(c.parent, o)
                    for c2 in c.children2:
                        self.assertIsNotNone(c2.id)
                        self.assertEqual(c2.child1_id, c.id)
                        self.assertEqual(c2.child1, c)

            for k, s in sources.items():
                for old, new in zip(all_children[k], all_dup_children[k]):
                    self.assertEqual(old.id, new.id)
                    self.assertEqual(old.name, new.name)
                    self.assertEqual(old.parent_id, new.parent_id)
                    for oldc2, newc2 in zip(old.children2, new.children2):
                        self.assertEqual(oldc2.id, newc2.id)
                        self.assertEqual(oldc2.child1_id, newc2.child1_id)
                        self.assertEqual(oldc2.name, newc2.name)


if __name__ == "__main__":
    unittest.main()
