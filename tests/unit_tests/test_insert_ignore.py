"""Unit tests for db.py """
import unittest
from typing import List

from sqlalchemy import ForeignKey, String, select
from sqlalchemy.orm import Mapped, mapped_column, relationship

from sqlalchemy_extensions.orm import Base
from sqlgold.utils._test_db_utils import create_test_db


class TClass(Base):
    __tablename__ = "tclass"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True)


class TParent(Base):
    __tablename__ = "tparent"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True)

    children: Mapped[List["TChild"]] = relationship()


class TChild(Base):
    __tablename__ = "tchild"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True)

    parent_id: Mapped[int] = mapped_column(ForeignKey(TParent.id))
    parent: Mapped[TParent] = relationship(back_populates="children")


class SingleTest(unittest.TestCase):
    def test_basic_insert_ignore_all(self):
        with create_test_db() as db:
            size = 2
            sources = {str(x): TClass(name=str(x)) for x in range(1, size + 1)}
            with db.Session() as s:
                inserted_objs = list(sources.values())
                s.insert_ignore_all(inserted_objs, commit=True)

                dbobjs = list(s.scalars(select(TClass)))
                self.assertEqual(len(dbobjs), size)
                # check id in session
                for o in inserted_objs:
                    self.assertIsNotNone(o.id)
            # check id out of session
            for o in inserted_objs:
                self.assertIsNotNone(o.id)

    def test_basic_insert_ignore_duplicates(self):
        with create_test_db() as db:
            size = 3
            dup_size = 2
            sources = {str(x): TClass(id=x, name=str(x)) for x in range(1, size + 1)}
            with db.Session() as s:
                inserted_objs = sources.values()
                s.insert_ignore_all(inserted_objs, commit=True)

                dbobjs = list(s.scalars(select(TClass)))
                self.assertEqual(len(dbobjs), size)
                # check id in session
                for o in inserted_objs:
                    self.assertIsNotNone(o.id)
            # check id out of session
            for o in inserted_objs:
                self.assertIsNotNone(o.id)

            sources2 = {
                str(x): TClass(id=x, name=str(x)) for x in range(1, dup_size + 1)
            }
            with db.Session() as s:
                inserted_objs2 = list(sources2.values())
                s.insert_ignore_all(inserted_objs2, commit=True)
                dbobjs2 = list(s.scalars(select(TClass)))
                mx = max(size, dup_size)
                self.assertEqual(len(dbobjs2), mx)

                # check id in session
                for o in inserted_objs2:
                    self.assertIsNotNone(o.id)
            # check id out of session
            for o in inserted_objs2:
                self.assertIsNotNone(o.id)

    def test_multiple_object_insert_ignore_mix_ids(self):
        with create_test_db() as db:
            size = 10
            dup_size = 15
            sources = {str(x): TClass(id=x, name=str(x)) for x in range(1, size + 1)}

            with db.Session() as s:
                s.insert_ignore_all(sources.values(), commit=True)

                ## Insert a mix of classes we've made and not
                dup_sources = {
                    str(x): TClass(id=x, name=str(x)) for x in range(1, dup_size + 1)
                }

                # Make sure something was created
                mix = s.insert_ignore_all(dup_sources.values(), commit=True)
                stmt = select(TClass)
                self.assertEqual(len(list(s.scalars(stmt))), dup_size)
                for m in mix:
                    self.assertIsNotNone(m.id)

    def test_parent_child(self):
        with create_test_db() as db:
            size = 10
            nchildren = 3
            child_start_id = size * nchildren
            sources = {str(x): TParent(id=x, name=str(x)) for x in range(1, size + 1)}

            with db.Session() as s:
                s.insert_ignore_all(sources.values(), commit=True)
                stmt = select(TParent)
                self.assertEqual(len(list(s.scalars(stmt))), size)
            children = {
                str(x): TChild(
                    id=child_start_id + x, name=str(x), parent=sources[str(x)]
                )
                for x in range(1, nchildren + 1)
            }

            with db.Session() as s:
                s.insert_ignore_all(children.values(), commit=True)
                stmt = select(TChild)
                self.assertEqual(len(list(s.scalars(stmt))), nchildren)
                stmt = select(TParent)
                self.assertEqual(len(list(s.scalars(stmt))), size)

    def test_parent_child_id(self):
        with create_test_db() as db:
            size = 2
            nchildren = 3
            child_start_id = size * nchildren
            sources = {str(x): TParent(id=x, name=str(x)) for x in range(1, size + 1)}
            for s in sources.values():
                children = {
                    str(x): TChild(id=s.id * nchildren + x, name=str(x), parent_id=s.id)
                    for x in range(1, nchildren + 1)
                }
                s.children = list(children.values())

            with db.Session(expire_on_commit=False) as s:
                s.insert_ignore_all(sources.values(), commit=True)
                stmt = select(TParent)
                self.assertEqual(len(list(s.scalars(stmt))), size)
                stmt = select(TChild)
                self.assertEqual(len(list(s.scalars(stmt))), size * nchildren)

            with db.Session() as s:
                s.insert_ignore_all(sources.values(), commit=True)
                stmt = select(TParent)
                self.assertEqual(len(list(s.scalars(stmt))), size)
                stmt = select(TChild)
                self.assertEqual(len(list(s.scalars(stmt))), size * nchildren)

    def test_multiple_object_insert_ignore_dups(self):
        with create_test_db() as db:
            size = 10
            dup_size = 5
            sources = {str(x): TClass(id=x, name=str(x)) for x in range(1, size + 1)}

            with db.Session() as s:
                s.insert_ignore_all(sources.values(), commit=True)

                ## Insert the duplicates
                dup_sources = {
                    str(x): TClass(id=x, name=str(x)) for x in range(1, dup_size + 1)
                }

                # Make sure nothing was created
                s.insert_ignore_all(dup_sources.values(), commit=True)
                stmt = select(TClass)
                self.assertEqual(len(list(s.scalars(stmt))), size)

    def test_multiple_object_insert_ignore_mix(self):
        with create_test_db() as db:
            size = 10
            dup_size = 15
            sources = {str(x): TClass(id=x, name=str(x)) for x in range(1, size + 1)}

            with db.Session() as s:
                s.insert_ignore_all(sources.values(), commit=True)

                ## Insert a mix of classes we've made and not
                dup_sources = {
                    str(x): TClass(id=x, name=str(x)) for x in range(1,dup_size+1)
                }

                # Make sure something was created
                s.insert_ignore_all(dup_sources.values(), commit=True)
                stmt = select(TClass)
                self.assertEqual(len(list(s.scalars(stmt))), dup_size)

    def test_single_object_insert_ignore_no_dups(self):
        with create_test_db() as db:
            size = 10
            sources = {str(x): TClass(id=x, name=str(x)) for x in range(1, size + 1)}

            with db.Session() as s:
                s.add_all(sources.values())
                s.commit()

                not_dup_idx = 1
                no_dup_s = TClass(id=not_dup_idx, name=str(not_dup_idx))
                s.insert_ignore(no_dup_s, commit=True)
                ## Make sure something was created
                stmt = select(TClass).where(TClass.name == str(not_dup_idx))
                self.assertEqual(len(list(s.scalars(stmt))), 1)

    def test_single_object_insert_ignore_dups(self):
        with create_test_db() as db:
            size = 10
            sources = {str(x): TClass(id=x, name=str(x)) for x in range(1, size + 1)}

            with db.Session() as s:
                s.add_all(sources.values())
                s.commit()

                ## Check upserts
                test_idx = 1
                dup_s = TClass(id=test_idx, name=str(test_idx))
                s.insert_ignore(dup_s, commit=True)

                ## Make sure nothing new was created
                stmt = select(TClass).where(TClass.name == str(test_idx))
                self.assertEqual(len(list(s.scalars(stmt))), 1)


if __name__ == "__main__":
    unittest.main()
