import unittest
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy_extensions.ext.declarative import declarative_base
from sqlalchemy.orm import backref
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import relationship
from sqlalchemy_extensions.orm import Session
from sqlalchemy.orm.collections import attribute_keyed_dict
from sqlalchemy_extensions import create_db
from sqlalchemy import ForeignKey, String, select
import logging
Base = declarative_base()


class TreeNode(Base):
    __tablename__ = "tree"
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey(id))
    name = Column(String(50), nullable=False)

    children = relationship(
        "TreeNode",
        # cascade deletions
        cascade="all, delete-orphan",
        # many to one + adjacency list - remote_side
        # is required to reference the 'remote'
        # column in the join condition.
        backref=backref("parent", remote_side=id),
        # children will be represented as a dictionary
        # on the "name" attribute.
        collection_class=attribute_keyed_dict("name"),
    )

    def __init__(self, name, parent=None, id=None):
        self.name = name
        self.parent = parent
        if id is not None:
            self.id = id

    def __repr__(self):
        return "TreeNode(name=%r, id=%r, parent_id=%r)" % (
            self.name,
            self.id,
            self.parent_id,
        )

    def dump(self, _indent=0):
        return (
            "   " * _indent
            + repr(self)
            + "\n"
            + "".join([c.dump(_indent + 1) for c in self.children.values()])
        )

def msg(msg, *args):
    msg = msg % args
    logging.debug("\n\n\n" + "-" * len(msg.split("\n")[0]))
    logging.debug(msg)
    logging.debug("-" * len(msg.split("\n")[0]))

class TestAdjacencyList(unittest.TestCase):
    def test(self):
        ## SQAlchemy Extension
        db = create_db("sqlite://", echo=True)
        db.create_all()
        session = Session()

        ## Sqlalchemy Normal code
        # engine = create_engine("sqlite://", echo=True)
        # Base.metadata.create_all(engine)
        # session = Session(engine)


        msg("Creating Tree Table:")

        node = TreeNode("rootnode")
        TreeNode("node1", parent=node)
        TreeNode("node3", parent=node)

        node2 = TreeNode("node2")
        TreeNode("subnode1", parent=node2)
        node.children["node2"] = node2
        TreeNode("subnode2", parent=node.children["node2"])

        msg("Created new tree structure:\n%s", node.dump())

        msg("flush + commit:")

        session.add(node)
        session.commit()
        dbobjs = list(session.scalars(select(TreeNode)))
        self.assertEqual(len(dbobjs), 6)

        msg("Tree After Save:\n %s", node.dump())

        TreeNode("node4", parent=node)
        TreeNode("subnode3", parent=node.children["node4"])
        TreeNode("subnode4", parent=node.children["node4"])
        TreeNode("subsubnode1", parent=node.children["node4"].children["subnode3"])
        dbobjs = list(session.scalars(select(TreeNode)))
        self.assertEqual(len(dbobjs), 6)

        # remove node1 from the parent, which will trigger a delete
        # via the delete-orphan cascade.
        del node.children["node1"]

        msg("Removed node1.  flush + commit:")
        session.commit()

        msg("Tree after save:\n %s", node.dump())

        msg(
            "Emptying out the session entirely, selecting tree on root, using "
            "eager loading to join four levels deep."
        )
        session.expunge_all()
        dbobjs = list(session.scalars(select(TreeNode)))
        self.assertEqual(len(dbobjs), 5)
        node = (
            session.query(TreeNode)
            .filter(TreeNode.name == "rootnode")
            .first()
        )

        msg("Full Tree:\n%s", node.dump())

        msg("Marking root node as deleted, flush + commit:")

        session.delete(node)
        session.commit()
        dbobjs = list(session.scalars(select(TreeNode)))
        self.assertEqual(len(dbobjs), 0)

    def test_replace_add_with_insert_ignore(self):
        ## SQAlchemy Extension
        db = create_db("sqlite://", echo=True)
        db.create_all()
        session = Session()

        ## Sqlalchemy Normal code
        # engine = create_engine("sqlite://", echo=True)
        # Base.metadata.create_all(engine)
        # session = Session(engine)


        msg("Creating Tree Table:")

        node = TreeNode("rootnode")
        TreeNode("node1", parent=node)
        TreeNode("node3", parent=node)

        node2 = TreeNode("node2")
        TreeNode("subnode1", parent=node2)
        node.children["node2"] = node2
        TreeNode("subnode2", parent=node.children["node2"])

        msg("Created new tree structure:\n%s", node.dump())

        msg("flush + commit:")

        session.insert_ignore(node)
        session.commit()
        dbobjs = list(session.scalars(select(TreeNode)))
        self.assertEqual(len(dbobjs), 6)

        msg("Tree After Save:\n %s", node.dump())

        TreeNode("node4", parent=node)
        TreeNode("subnode3", parent=node.children["node4"])
        TreeNode("subnode4", parent=node.children["node4"])
        TreeNode("subsubnode1", parent=node.children["node4"].children["subnode3"])
        dbobjs = list(session.scalars(select(TreeNode)))
        self.assertEqual(len(dbobjs), 6)

        # remove node1 from the parent, which will trigger a delete
        # via the delete-orphan cascade.
        del node.children["node1"]

        msg("Removed node1.  flush + commit:")
        session.commit()

        msg("Tree after save:\n %s", node.dump())

        msg(
            "Emptying out the session entirely, selecting tree on root, using "
            "eager loading to join four levels deep."
        )
        session.expunge_all()
        dbobjs = list(session.scalars(select(TreeNode)))
        self.assertEqual(len(dbobjs), 5)
        node = (
            session.query(TreeNode)
            .filter(TreeNode.name == "rootnode")
            .first()
        )

        msg("Full Tree:\n%s", node.dump())

        msg("Marking root node as deleted, flush + commit:")

        session.delete(node)
        session.commit()
        dbobjs = list(session.scalars(select(TreeNode)))
        self.assertEqual(len(dbobjs), 0)


    def test_insert_ignore_all(self):
        ## SQAlchemy Extension
        db = create_db("sqlite://", echo=True)
        db.create_all()
        session = Session()

        ## Sqlalchemy Normal code
        # engine = create_engine("sqlite://", echo=True)
        # Base.metadata.create_all(engine)
        # session = Session(engine)


        msg("Creating Tree Table:")

        node = TreeNode("rootnode")
        TreeNode("node1", parent=node)
        TreeNode("node3", parent=node)

        node2 = TreeNode("node2")
        TreeNode("subnode1", parent=node2)
        node.children["node2"] = node2
        TreeNode("subnode2", parent=node.children["node2"])

        msg("Created new tree structure:\n%s", node.dump())

        msg("flush + commit:")

        session.add(node)
        session.commit()
        # this will cause integrity errors with a normal add
        dup_rootnode = TreeNode(id=node.id, name=node.name) 
        session.insert_ignore(dup_rootnode)
        session.commit()
        dbobjs = list(session.scalars(select(TreeNode)))
        self.assertEqual(len(dbobjs), 6)

        msg("Tree After Save:\n %s", node.dump())

        TreeNode("node4", parent=node)
        TreeNode("subnode3", parent=node.children["node4"])
        TreeNode("subnode4", parent=node.children["node4"])
        TreeNode("subsubnode1", parent=node.children["node4"].children["subnode3"])
        dbobjs = list(session.scalars(select(TreeNode)))
        self.assertEqual(len(dbobjs), 6)

        # remove node1 from the parent, which will trigger a delete
        # via the delete-orphan cascade.
        del node.children["node1"]

        msg("Removed node1.  flush + commit:")
        session.commit()

        msg("Tree after save:\n %s", node.dump())

        msg(
            "Emptying out the session entirely, selecting tree on root, using "
            "eager loading to join four levels deep."
        )
        session.expunge_all()
        dbobjs = list(session.scalars(select(TreeNode)))
        self.assertEqual(len(dbobjs), 5)
        node = (
            session.query(TreeNode)
            .filter(TreeNode.name == "rootnode")
            .first()
        )

        msg("Full Tree:\n%s", node.dump())

        msg("Marking root node as deleted, flush + commit:")

        session.delete(node)
        session.commit()
        dbobjs = list(session.scalars(select(TreeNode)))
        self.assertEqual(len(dbobjs), 0)

if __name__ == "__main__":
    unittest.main()
