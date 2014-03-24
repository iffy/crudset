from unittest import TestCase

from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy import create_engine

from crudset.error import MissingRequiredFields
from crudset.crud import _CrudMaker


metadata = MetaData()
families = Table('family', metadata,
    Column('id', Integer(), primary_key=True),
    Column('created', DateTime()),
    Column('surname', String()),
)

people = Table('people', metadata,
    Column('id', Integer(), primary_key=True),
    Column('created', DateTime()),
    Column('family_id', Integer()),
    Column('name', String()),
)


class _CrudMakerFunctionalTest(TestCase):
    """
    These tests use an sqlite backend and are therefore functional.
    """

    def engine(self):
        engine = create_engine('sqlite://')
        metadata.create_all(bind=engine)
        return engine


    def assertSame(self, expr1, expr2, msg=''):
        self.assertEqual(expr1, expr2, '\n%s\n!=\n%s\n%s' % (expr1, expr2, msg))


    def test_create(self):
        """
        It should return a valid statment for execution.
        """
        engine = self.engine()
        
        maker = _CrudMaker(families)
        stmt = maker.create({'surname': 'Johnson'})
        engine.execute(stmt)


    def test_create_required(self):
        """
        You can require certain attributes on create.
        """
        maker = _CrudMaker(families, create_requires=['surname'])
        self.assertRaises(MissingRequiredFields, maker.create, {})

