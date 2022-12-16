"""Module for all of the extensions to the SQLAlchemy session class """
import itertools
import logging
from collections.abc import Iterable as CollectionsIterable
from functools import partial
from typing import Any, Iterable, List, Set, Tuple, Type, Union

from sqlalchemy import func, select
from sqlalchemy.exc import DatabaseError, IntegrityError, NoResultFound
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import tuple_

from sqlalchemy_extensions.orm.decl_base import DeclarativeBase, Registry


def _eq_filter(l1: List, l2: List) -> List[bool]:
    """Given two lists, compare the

    Args:
        l1 (List): list1 to compare
        l2 (List): list2 to compare

    Returns:
        List[bool]: list of bool equalities
    """
    return [x == y for x, y in zip(l1, l2)]


class SessionExtensions(Session):
    """Class that holds all of the extensions to the SQLAlchemy session class"""

    def count(self, cls):
        return self.scalar(select(func.count()).select_from(cls))

    def _commit_flush_refresh(
        self,
        commit: bool = False,
        flush: bool = False,
        refresh: bool = False,
        instances: object = None,
    ):
        if commit:
            self.commit()
        if flush:
            self.flush()
        if refresh:
            if isinstance(instances, CollectionsIterable):
                for o in instances:
                    self.refresh(o)
            else:
                self.refresh(instances)

    def insert_ignore_all(
        self,
        objects: Iterable[DeclarativeBase],
        commit: bool = False,
        flush: bool = False,
        refresh: bool = False,
        classes_seen: Set[Type[DeclarativeBase]] = None,
    ) -> List[DeclarativeBase]:
        """insert any objects that aren't found into the db

        Args:
            objects (Iterable[DeclarativeBase]): valid db objects
            commit (bool, optional): commit to the db. Defaults to False.
            flush (bool, optional): flush the db. Defaults to False.

        Raises:
            IOError: _description_

        Returns:
            List[DeclarativeBase]: All objects (with primary keys) from the db
        """
        if not objects:
            return objects

        obj = next(iter(objects))

        ## get ids that were in the database
        # Example: .in_([('0', '0'), ('1', '1')])
        db_ids_key_vals = self.execute(
            select(*obj._key_columns).filter(
                tuple_(*obj._key_columns).in_([x._key_vals for x in objects])
            )
        ).all()

        ## get what objects were found and not found in db
        not_found_objs = [o for o in objects if o._key_vals not in db_ids_key_vals]

        ## Add in whatever objects were not found
        if not_found_objs:
            self.add_all(not_found_objs)
        if commit or flush or refresh:
            self._commit_flush_refresh(commit, flush, refresh, not_found_objs)
        ## Handle Relationships
        ## self.insert_ignore chosen on purpose as it will call
        ## insert_ignore_all if the objects are iterable
        if obj._rel_columns:
            if classes_seen is None:
                classes_seen = set()
            classes_seen.add(obj.__class__)
            self._handle_relationships(
                self.insert_ignore,
                objects=objects,
                commit=commit,
                flush=flush,
                refresh=refresh,
                classes_seen=classes_seen,
            )

        return objects

    def insert_ignore(
        self,
        obj: Union[DeclarativeBase, Iterable[DeclarativeBase]],
        commit: bool = False,
        flush: bool = False,
        refresh: bool = False,
        classes_seen: Set[Type[DeclarativeBase]] = None,
    ) -> DeclarativeBase:
        """insert the object if it is not already in the db

        Args:
            obj (DeclarativeBase): valid db obj
            commit (bool, optional): commit to the db. Defaults to False.
            flush (bool, optional): flush the db. Defaults to False.

        Returns:
            DeclarativeBase: _description_
        """
        if not obj:
            return obj
        if isinstance(obj, CollectionsIterable):
            return self.insert_ignore_all(
                obj,
                commit=commit,
                flush=flush,
                refresh=refresh,
                classes_seen=classes_seen,
            )
        stmt = select(obj.__class__).where(*_eq_filter(obj._key_columns, obj._key_vals))
        try:
            return self.scalars(stmt).one_or_none()
        except NoResultFound:
            pass

        self.add(obj)
        if commit or flush or refresh:
            self._commit_flush_refresh(commit, flush, refresh, obj)
        if obj._rel_columns:
            if classes_seen is None:
                classes_seen = set()
            classes_seen.add(obj.__class__)
            self._handle_relationships(
                self.insert_ignore,
                objects=[obj],
                commit=commit,
                flush=flush,
                refresh=refresh,
                classes_seen=classes_seen,
            )
        return obj

    def find_keys(
        self,
        obj_class: Type[DeclarativeBase],
        logical_values: Union[Any, Iterable[Any]],
    ) -> Tuple[Any]:
        """Query to get the primary key values from the logical key values

        Args:
            obj_class (Type[DeclarativeBase]): valid class
            logical_keys (Union[Any, List[Any]]): logical keys

        Returns:
            Tuple[Any]: primary keys or None
        """

        if not isinstance(logical_values, CollectionsIterable):
            logical_values = tuple(logical_values)
        if not obj_class._log_columns:
            raise Exception(
                f"Error! No logical_key columns were specified for class '{obj_class}'"
            )
        try:
            stmt = select(*obj_class._key_columns).where(
                *_eq_filter(obj_class._log_columns, logical_values)
            )
            v = self.execute(stmt).one_or_none()
            return v[0] if v is not None and len(v) == 1 else v
        except Exception as e:
            e.add_note(
                f"stmt={stmt}\nobj._log_columns={obj_class._log_columns}, "
                f"obj._log_vals={logical_values}"
            )
            raise

    def _handle_relationships(
        self,
        insert_func: Any,
        objects: Iterable[DeclarativeBase],
        commit: bool,
        flush: bool,
        refresh: bool,
        classes_seen: Set[Type[DeclarativeBase]] = None,
    ):
        """Find any relationships, i.e. child classes, that should also have
        the insert_func called for them. Update the child class values as appropriate

        Args:
            insert_func (Any): insert function to call for relationship objects
            objects (Iterable[DeclarativeBase]): objects with potential relationships
            commit (bool): pass to insert_func
            flush (bool): pass to insert_func
            refresh (bool): pass to insert_func
            classes_seen (Set[DeclarativeBase]): set of already seen classes to avoid
                adding already handled classes
        """
        obj = next(iter(objects))

        for rcol_str, rcol in obj._rel_columns.items():
            toinsertobjects = []
            rclass = rcol.mapper.class_manager.class_
            ### we assume that previous classes handled the relationship
            if rclass in classes_seen:
                continue
            classes_seen.add(rclass)
            for parentobj in objects:
                instance_or_list = getattr(parentobj, rcol_str)
                if not instance_or_list:
                    continue
                if not isinstance(instance_or_list, CollectionsIterable):
                    instance_or_list = [instance_or_list]
                ### I have my column of children to update
                ### Now I need the child class, and all of the child columns to update
                fk_relationships = Registry.get_relationships(
                    parentobj.__tablename__, next(iter(instance_or_list)).__class__
                )
                childcol_parentvalues = [
                    (r.end_col, getattr(parentobj, r.start_col))
                    for r in fk_relationships
                ]
                # Set the parent value to all the objects
                for childobj in instance_or_list:
                    for childcol, parentvalue in childcol_parentvalues:
                        setattr(childobj, childcol, parentvalue)
                toinsertobjects.extend(instance_or_list)

            insert_func(
                toinsertobjects,
                commit=commit,
                flush=flush,
                refresh=refresh,
                classes_seen=classes_seen,
            )

    def linsert_ignore_all(
        self,
        objects: Iterable[DeclarativeBase],
        commit: bool = False,
        flush: bool = False,
        refresh: bool = False,
        classes_seen: Set[Type[DeclarativeBase]] = None,
    ) -> List[DeclarativeBase]:
        """Logical insert_ignore_all, insert any objects that aren't found into the db

        Args:
            objects (Iterable[DeclarativeBase]): valid db objects
            commit (bool, optional): commit to the db. Defaults to False.
            flush (bool, optional): flush the db. Defaults to False.

        Raises:
            IOError: _description_

        Returns:
            List[DeclarativeBase]: All objects (with primary keys) from the db
        """
        if not objects:
            return objects

        obj = next(iter(objects))
        if not obj._log_columns:
            raise Exception(
                f"Error! No logical_key columns were specified for class '{obj.__class__}'"
            )
        ## get ids that were in the database
        # Example: in_([('0', '0'), ('1', '1')])
        stmt = select(*obj._key_columns, *obj._log_columns).filter(
            tuple_(*obj._log_columns).in_([x._log_vals for x in objects])
        )
        db_ids_log_vals = self.execute(stmt).all()

        nkeys = len(obj._key_columns)
        logvals_to_keyvals = {tuple(x[nkeys:]): x[:nkeys] for x in db_ids_log_vals}

        ## get what objects were found and not found in db
        not_found_objs = []
        for o in objects:
            if o._log_vals in logvals_to_keyvals:
                for kc, lv in zip(o._key_columns, logvals_to_keyvals[o._log_vals]):
                    setattr(o, kc.name, lv)
            else:
                not_found_objs.append(o)
        ## Add in whatever objects were not found
        if not_found_objs:
            self.add_all(not_found_objs)

        if commit or flush or refresh:
            self._commit_flush_refresh(commit, flush, refresh, not_found_objs)

        ## Handle Relationships
        ## self.linsert_ignore is passed on purpose as it calls
        ##   linsert_ignore_all if it is a collection
        if obj._rel_columns:
            if classes_seen is None:
                classes_seen = set()
            classes_seen.add(obj.__class__)
            self._handle_relationships(
                self.linsert_ignore,
                objects=objects,
                commit=commit,
                flush=flush,
                refresh=refresh,
                classes_seen=classes_seen,
            )
        return objects

    def linsert_ignore(
        self,
        obj: Union[DeclarativeBase, Iterable[DeclarativeBase]],
        commit: bool = False,
        flush: bool = False,
        refresh: bool = False,
        classes_seen: Set[DeclarativeBase] = None,
    ) -> DeclarativeBase:
        """Logical insert_ignore, insert the object if it is not already in the db

        Args:
            obj (DeclarativeBase): valid db obj
            commit (bool, optional): commit to the db. Defaults to False.
            flush (bool, optional): flush the db. Defaults to False.

        Returns:
            DeclarativeBase: _description_
        """
        if not obj:
            return obj
        if isinstance(obj, CollectionsIterable):
            return self.linsert_ignore_all(
                obj,
                commit=commit,
                flush=flush,
                refresh=refresh,
                classes_seen=classes_seen,
            )
        if not obj._log_columns:
            raise Exception(
                f"Error! No logical_key columns were specified for class '{obj.__class__}'"
            )
        stmt = select(obj.__class__).where(*_eq_filter(obj._log_columns, obj._log_vals))
        try:
            return self.scalars(stmt).one()
        except NoResultFound:
            pass
        self.add(obj)
        self._commit_flush_refresh(commit, flush, refresh, obj)
        ## Handle Relationships
        if obj._rel_columns:
            if classes_seen is None:
                classes_seen = set()
            classes_seen.add(obj.__class__)
            self._handle_relationships(
                self.linsert_ignore,
                objects=[obj],
                commit=commit,
                flush=flush,
                refresh=refresh,
                classes_seen=classes_seen,
            )
        return obj

    def lexists(
        self,
        obj: DeclarativeBase,
    ) -> Any:
        """Query to see if the object is in the db

        Args:
            obj (DeclarativeBase): valid db obj

        Returns:
            Any or None: id/ids (primary key/s values) of the object or None
        """
        return self.find_keys(obj, obj._log_vals)

    def lget(
        self, obj_class: Type[DeclarativeBase], values: Union[Any, Iterable[Any]]
    ) -> DeclarativeBase:
        """Query to see if the object is in the db based on the logical keys

        Args:
            obj (Type[DeclarativeBase]): valid db obj

        Returns:
            DeclarativeBase Instance or None: The object with the given keys or None
        """
        if not isinstance(values, CollectionsIterable):
            values = [values]
        if not obj_class._log_columns:
            raise Exception(
                f"Error! No logical_key columns were specified for class '{obj_class}'"
            )
        stmt = select(obj_class).where(*_eq_filter(obj_class._log_columns, values))
        try:
            return self.scalars(stmt).one_or_none()
        except:
            logging.error(
                f"stmt={stmt}\nobj._log_columns={obj_class._log_columns}, "
                f"obj._log_vals={values}, "
            )
            raise


class extended_sessionmaker(sessionmaker):
    """Class to create an extended Session"""

    def add_functions(self, session: Session) -> SessionExtensions:
        """Add the functions

        Args:
            session (Session): session object

        Returns:
            Session: session object
        """

        # fmt: off
        session.count = partial(SessionExtensions.count, session)
        session.insert_ignore = partial(SessionExtensions.insert_ignore, session)
        session.insert_ignore_all = partial(SessionExtensions.insert_ignore_all, session)
        session.find_keys = partial(SessionExtensions.find_keys, session)
        session.lexists = partial(SessionExtensions.lexists, session)
        session.linsert_ignore = partial(SessionExtensions.linsert_ignore, session)
        session.linsert_ignore_all = partial(SessionExtensions.linsert_ignore_all, session)
        session.lget = partial(SessionExtensions.lget, session)
        session._handle_relationships = partial(SessionExtensions._handle_relationships, session)
        session._commit_flush_refresh = partial(SessionExtensions._commit_flush_refresh, session)
        # fmt: on
        return session

    def begin(self, nested: bool = False) -> SessionExtensions:
        """begin function

        Returns:
            SessionExtensions: session extended with extra functions
        """
        s = super().begin()
        return self.add_functions(s)

    def begin_nested(self, nested: bool = False) -> SessionExtensions:
        """begin function

        Returns:
            SessionExtensions: session extended with extra functions
        """
        s = super().begin()
        return self.add_functions(s)

    def __call__(self, **local_kw: Any) -> SessionExtensions:
        """__call__

        Returns:
            SessionExtensions: session extended with extra functions
        """
        s = super().__call__(**local_kw)
        return self.add_functions(s)
