from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Self, Tuple, Type

from sqlalchemy import orm
from sqlalchemy.dialects import registry
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.orm import declarative_base as sa_declarative_base
from sqlalchemy.schema import Column


class LogicalKey(DefaultDialect):
    """Class to allow logical_key to be passed inside of Columns
    and mapped_column without warnings.
    Specifically registers "logical" and the Dialect arguments is "key"
    or any other _prefix.
    Example: logical_foo=True would be "logical" : {"foo":True}
    """


registry.register("logical", "sqlalchemy_extensions.orm.decl_base", "LogicalKey")


@dataclass
class Relationship:
    start_col: str = None
    end_col: str = None


class DeclarativeBase:
    _key_columns: List[Column] = None
    _log_columns: List[Column] = None
    _all_columns: List[Column] = None
    _rel_columns: Dict = None

    def __init_subclass__(cls, *args, **kwargs) -> None:
        cls._make_columns()
        super().__init_subclass__(*args, **kwargs)

    @classmethod
    def _make_columns(cls) -> None:
        """Make the key, log, all, and rel class columns"""
        if cls.__module__ == "sqlalchemy.orm.decl_api":
            return

        ## make key and logical key columns
        cls._key_columns = []
        cls._log_columns = []
        all_cols = []
        cls._rel_columns = {}

        for k, v in vars(cls).items():
            col = None
            if isinstance(v, orm.relationships.Relationship):
                cls._rel_columns[k] = v  # back populates is important
            elif isinstance(v, orm.properties.MappedColumn):
                col = v.column
            elif isinstance(v, Column):
                col = v
            if col is not None:
                found = Registry._columns2name.get(col)
                Registry._columns2name[col] = (cls, k)
                ## Add in Foreign Keys
                for fk in col.foreign_keys:
                    try:
                        fkcol = fk.column
                        if not fkcol.table:
                            fktable = cls  ## self reference
                        else:
                            fktable = fkcol.table.name
                        Registry._fk_referred_from[fktable][cls][k] = Relationship(
                            fkcol.name, k
                        )
                    except:
                        fktable, fkcol = fk.target_fullname.split(".")
                        Registry._fk_referred_from[fktable][cls][k] = Relationship(
                            fkcol, k
                        )
                found = False
                if col.primary_key:
                    cls._key_columns.append(col)
                    found = True
                logical = col.dialect_options.get("logical", None)
                if logical and logical.get("key", False):
                    cls._log_columns.append(col)
                    found = True
                if not found:
                    all_cols.append(col)

        _all_columns = [*cls._key_columns, *cls._log_columns, *all_cols]
        cls._all_columns = list(set(_all_columns))

    def to_json(
        self, include_all_columns=False, include_private=False
    ) -> Dict[str, Any]:
        """Return a json that has the variables and values from this object

        Args:
            include_private (bool, optional):
                include '_' prefixed variables. Defaults to False.

        Returns:
            Dict: dict of variables and values
        """
        if include_all_columns:
            d = {c.name: getattr(self, c.name) for c in self._all_columns}
            d.update(vars(self))
        else:
            d = vars(self)

        if include_private:
            return d
        return {k: v for k, v in d.items() if not k.startswith("_")}

    def to_dict(self, include_all_columns=False, include_private=False) -> Dict:
        """Return a dict that has the variables and values from this object

        Args:
            include_private (bool, optional):
                include '_' prefixed variables. Defaults to False.

        Returns:
            Dict: dict of variables and values
        """
        return self.to_json(include_private=include_private)

    @property
    def _log_vals(self) -> Tuple[Any]:
        return tuple([getattr(self, k.name) for k in self._log_columns])

    @property
    def _key_vals(self) -> Tuple[Any]:
        return tuple([getattr(self, k.name) for k in self._key_columns])

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> Self:
        """Create an object from a dict of variables and values

        Args:
            d (Dict[str, Any]): dict of variables and values

        Returns:
            DeclarativeBase: obj created from dict
        """
        c = cls()
        for k, v in d.items():
            setattr(c, k, v)
        return c

    def __repr__(self):
        cols = [*self._key_columns, *self._log_columns]
        cols = list(set([col.name for col in cols]))
        d = set(dir(self.__class__))
        remove = ["metadata", "registry", *cols]
        d.difference_update(remove)
        cols.extend(sorted(list(d)))
        l = [
            f"{var}={getattr(self, var)}"
            for var in cols
            if not callable(getattr(self.__class__, var)) and not var.startswith("_")
        ]
        ## ex <class 'tests.models.models.TClass'>
        clsname = str(self.__class__).split(".")[-1][:-2]
        return f"{clsname}({','.join(l)})"


class Registry:
    """Class to hold Table/Column information that is
    not easily obtained from SQLAlchemy
    """

    ## _fk_referred_from : A nested dict object that will return a Relationship
    ## When given a tablename(start) and the desired target class(end)
    _fk_referred_from: Dict[str, Dict[Type[DeclarativeBase], Dict]] = defaultdict(
        lambda: defaultdict(dict)
    )
    _columns2name = {}

    def get_relationships(
        start_class_table_name: str, end_class: Type[DeclarativeBase]
    ) -> Iterable[Relationship]:
        """Get the Relationships between two tables.

        Args:
            start_class_table_name (str): table name of the "start" table
            end_class (Type[DeclarativeBase]): class of the "end" table

        Returns:
            Iterable[Relationship]: Relationships
        """
        return Registry._fk_referred_from[start_class_table_name][end_class].values()


def declarative_base(*args, **kwargs) -> Any:
    """Call SQLAlchemy declarative base with the parameter cls=DeclarativeBase

    Returns:
        Any: The declarative base
    """
    return sa_declarative_base(cls=DeclarativeBase, *args, **kwargs)


Base = sa_declarative_base(cls=DeclarativeBase)
