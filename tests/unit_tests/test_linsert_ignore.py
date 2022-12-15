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
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)


class TParent(Base):
    __tablename__ = "tparent"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)

    children: Mapped[List["TChild"]] = relationship()


class TChild(Base):
    __tablename__ = "tchild"
    __table_args__ = {"extend_existing": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True, logical_key=True)

    parent_id: Mapped[int] = mapped_column(ForeignKey(TParent.id), logical_key=True)
    parent: Mapped[TParent] = relationship(back_populates="children")


class SingleTest(unittest.TestCase):
    def test_basic_linsert_ignore_all(self):
        with create_test_db() as db:
            size = 2
            sources = {str(x): TClass(name=str(x)) for x in range(size)}
            with db.Session() as s:
                inserted_objs = list(sources.values())
                s.linsert_ignore_all(inserted_objs, commit=True)

                dbobjs = list(s.scalars(select(TClass)))
                self.assertEqual(len(dbobjs), size)
                # check id in session
                for o in inserted_objs:
                    self.assertIsNotNone(o.id)
            # check id out of session
            for o in inserted_objs:
                self.assertIsNotNone(o.id)

    def test_basic_linsert_ignore_duplicates(self):
        with create_test_db() as db:
            size = 2
            dup_size = 2
            sources = {str(x): TClass(name=str(x)) for x in range(size)}
            with db.Session() as s:
                inserted_objs = list(sources.values())
                s.linsert_ignore_all(inserted_objs, commit=True)

                dbobjs = list(s.scalars(select(TClass)))
                self.assertEqual(len(dbobjs), size)
                # check id in session
                for o in inserted_objs:
                    self.assertIsNotNone(o.id)
            # check id out of session
            for o in inserted_objs:
                self.assertIsNotNone(o.id)

            sources2 = {str(x): TClass(name=str(x)) for x in range(dup_size)}
            with db.Session() as s:
                inserted_objs2 = list(sources2.values())
                s.linsert_ignore_all(inserted_objs2, commit=True)
                dbobjs2 = list(s.scalars(select(TClass)))
                self.assertEqual(len(dbobjs2), size)

                # check id in session
                for o in inserted_objs2:
                    self.assertIsNotNone(o.id)
            # check id out of session
            for o in inserted_objs2:
                self.assertIsNotNone(o.id)

    def test_multiple_object_linsert_ignore_mix_ids(self):
        with create_test_db() as db:
            size = 10
            dup_size = 15
            sources = {str(x): TClass(name=str(x)) for x in range(size)}

            with db.Session() as s:
                s.linsert_ignore_all(sources.values(), commit=True)

                ## Insert a mix of classes we've made and not
                dup_sources = {str(x): TClass(name=str(x)) for x in range(dup_size)}

                # Make sure something was created
                mix = s.linsert_ignore_all(dup_sources.values(), commit=True)
                stmt = select(TClass)
                self.assertEqual(len(list(s.scalars(stmt))), dup_size)
                for m in mix:
                    self.assertIsNotNone(m.id)

    def test_parent_child(self):
        with create_test_db() as db:
            size = 10
            sources = {str(x): TParent(name=str(x)) for x in range(size)}

            with db.Session() as s:
                s.linsert_ignore_all(sources.values(), commit=True)
                stmt = select(TParent)
                self.assertEqual(len(list(s.scalars(stmt))), size)
            children = {
                str(x): TChild(name=str(x), parent=sources[str(x)]) for x in range(size)
            }

            with db.Session() as s:
                s.linsert_ignore_all(children.values(), commit=True)
                stmt = select(TChild)
                self.assertEqual(len(list(s.scalars(stmt))), size)
                stmt = select(TParent)
                self.assertEqual(len(list(s.scalars(stmt))), size)

    def test_parent_child_id(self):
        with create_test_db() as db:
            size = 10
            sources = {str(x): TParent(name=str(x)) for x in range(size)}

            with db.Session() as s:
                s.linsert_ignore_all(sources.values(), commit=True)
                stmt = select(TParent)
                self.assertEqual(len(list(s.scalars(stmt))), size)
            children = {
                str(x): TChild(name=str(x), parent_id=sources[str(x)].id)
                for x in range(size)
            }

            with db.Session() as s:
                s.linsert_ignore_all(children.values(), commit=True)
                stmt = select(TChild)
                self.assertEqual(len(list(s.scalars(stmt))), size)
                stmt = select(TParent)
                self.assertEqual(len(list(s.scalars(stmt))), size)

    def test_multiple_object_linsert_ignore_dups(self):
        with create_test_db() as db:
            size = 10
            dup_size = 5
            sources = {str(x): TClass(name=str(x)) for x in range(size)}

            with db.Session() as s:
                s.linsert_ignore_all(sources.values(), commit=True)

                ## Insert the duplicates
                dup_sources = {str(x): TClass(name=str(x)) for x in range(dup_size)}

                # Make sure nothing was created
                s.linsert_ignore_all(dup_sources.values(), commit=True)
                stmt = select(TClass)
                self.assertEqual(len(list(s.scalars(stmt))), size)

    def test_multiple_object_linsert_ignore_mix(self):
        with create_test_db() as db:
            size = 10
            dup_size = 15
            sources = {str(x): TClass(name=str(x)) for x in range(size)}

            with db.Session() as s:
                s.linsert_ignore_all(sources.values(), commit=True)

                ## Insert a mix of classes we've made and not
                dup_sources = {str(x): TClass(name=str(x)) for x in range(dup_size)}

                # Make sure something was created
                s.linsert_ignore_all(dup_sources.values(), commit=True)
                stmt = select(TClass)
                self.assertEqual(len(list(s.scalars(stmt))), dup_size)

    def test_single_object_linsert_ignore_no_dups(self):
        with create_test_db() as db:
            size = 10
            sources = {str(x): TClass(name=str(x)) for x in range(size)}

            with db.Session() as s:
                s.add_all(sources.values())
                s.commit()

                test_name = str(size)
                no_dup_s = TClass(name=test_name)
                s.linsert_ignore(no_dup_s, commit=True)
                ## Make sure nothing was created
                stmt = select(TClass).where(TClass.name == test_name)
                self.assertEqual(len(list(s.scalars(stmt))), 1)

    def test_single_object_linsert_ignore_dups(self):
        with create_test_db() as db:
            size = 10
            sources = {str(x): TClass(name=str(x)) for x in range(size)}

            with db.Session() as s:
                s.add_all(sources.values())
                s.commit()

                ## Check upserts
                test_name = "1"
                dup_s = TClass(name=test_name)
                s.linsert_ignore(dup_s, commit=True)

                ## Make sure nothing new was created
                stmt = select(TClass).where(TClass.name == test_name)
                self.assertEqual(len(list(s.scalars(stmt))), 1)

    def test_single_object_linsert_ignore_lexists_id(self):
        with create_test_db() as db:
            size = 10
            sources = {str(x): TClass(name=str(x)) for x in range(size)}

            with db.Session.begin() as s:
                s.add_all(sources.values())

            test_name = "1"
            with db.Session() as s:
                dup_s = TClass(name=test_name)
                dup_s = s.linsert_ignore(dup_s)
                # check id in session
                self.assertEqual(dup_s.id - 1, int(test_name))
            # check id out of session
            self.assertEqual(dup_s.id - 1, int(test_name))

            with db.Session(expire_on_commit=False) as s:
                dup_s = TClass(name=test_name)
                dup_s = s.linsert_ignore(dup_s)
                # check id in session
                self.assertEqual(dup_s.id - 1, int(test_name))
            # check id out of session
            self.assertEqual(dup_s.id - 1, int(test_name))

    def test_single_object_linsert_ignore_not_lexists_id(self):
        with create_test_db() as db:
            size = 10
            sources = {str(x): TClass(name=str(x)) for x in range(size)}

            with db.Session.begin() as s:
                s.add_all(sources.values())

            test_name = str(size)

            with db.Session(expire_on_commit=False) as s:
                no_dup_s = TClass(name=test_name)
                no_dup_s = s.linsert_ignore(no_dup_s, commit=True)
                # check id in session
                self.assertEqual(no_dup_s.id - 1, int(test_name))

            # check id out of session
            self.assertEqual(no_dup_s.id - 1, int(test_name))

            test_name = str(size + 1)
            with db.Session(expire_on_commit=False) as s:
                no_dup_s = TClass(name=test_name)
                no_dup_s = s.linsert_ignore(no_dup_s, commit=True)
                # check id in session
                self.assertEqual(no_dup_s.id - 1, int(test_name))

            # check id out of session
            self.assertEqual(no_dup_s.id - 1, int(test_name))


if __name__ == "__main__":
    unittest.main()
