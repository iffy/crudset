from twisted.trial.unittest import TestCase
from twisted.internet import defer, reactor

from alchimia import TWISTED_STRATEGY

from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy import create_engine
from sqlalchemy.schema import CreateTable

from crudset.error import MissingRequiredFields, NotEditable
from crudset.crud import _CrudMaker, Crud, Policy

from twisted.python import log
import logging
class TwistedLogStream(object):
    def write(self, msg):
        log.msg(msg.rstrip())
    def flush(self):
        pass
    def close(self):
        pass
logging.basicConfig(stream=TwistedLogStream())
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


metadata = MetaData()
families = Table('family', metadata,
    Column('id', Integer(), primary_key=True),
    Column('location', String()),
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


    def test_get(self):
        """
        You can get a single row by primary key
        """
        engine = self.engine()

        maker = _CrudMaker(families)
        engine.execute(maker.create({'surname': 'Jones'}))
        hogan = engine.execute(maker.create({'surname': 'Hogan'}))
        key = hogan.inserted_primary_key

        stmt = maker.get(key)
        result = engine.execute(stmt)
        rows = list(result)
        self.assertEqual(rows[0][2], 'Hogan')


    def test_fetch(self):
        """
        You can fetch rows from the database.
        """
        engine = self.engine()

        maker = _CrudMaker(families)
        stmt = maker.fetch({})
        result = engine.execute(stmt)
        rows = list(result)
        self.assertEqual(rows, [])


    def test_fetch_data(self):
        """
        You can actually fetch data out.
        """
        engine = self.engine()

        maker = _CrudMaker(families)
        stmt = maker.create({'surname': 'Thomas'})
        engine.execute(stmt)

        stmt = maker.fetch({})
        result = engine.execute(stmt)
        rows = list(result)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], 'Thomas', 'Actual row: %r' % (rows[0],))


    def test_fetch_viewable_attributes(self):
        """
        Only viewable attributes should be given
        """
        engine = self.engine()

        maker = _CrudMaker(families, viewable_attributes=['surname'])
        stmt = maker.create({'surname': 'Thomas'})
        engine.execute(stmt)

        stmt = maker.fetch({})
        result = engine.execute(stmt)
        rows = list(result)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 'Thomas', 'Actual row: %r' % (rows[0],))


class CrudTest(TestCase):

    timeout = 2


    @defer.inlineCallbacks
    def engine(self):
        fname = self.mktemp()
        engine = create_engine('sqlite:///' + fname,
                               connect_args={'check_same_thread': False},
                               reactor=reactor,
                               strategy=TWISTED_STRATEGY)
        yield engine.execute(CreateTable(families))
        yield engine.execute(CreateTable(people))
        defer.returnValue(engine)


    @defer.inlineCallbacks
    def test_create(self):
        """
        You can create an object.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families, editable=['surname']))

        family = yield crud.create({'surname': 'Jones'})
        self.assertEqual(family['surname'], 'Jones')
        self.assertNotEqual(family['id'], None)
        self.assertEqual(family['location'], None)


    @defer.inlineCallbacks
    def test_create_fixed(self):
        """
        You can create a Crud with fixed attributes.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families, editable=['surname']))
        crud = crud.fix({'surname':'Hammond'})

        family = yield crud.create({})
        self.assertEqual(family['surname'], 'Hammond')

        fam2 = yield crud.create({'surname': 'Jones'})
        self.assertEqual(fam2['surname'], 'Hammond')


    @defer.inlineCallbacks
    def test_create_required(self):
        """
        You can require fields to be set.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families, required=['surname']))
        exc = self.failureResultOf(crud.create({})).value
        self.assertTrue(isinstance(exc, MissingRequiredFields), exc)


    @defer.inlineCallbacks
    def test_create_notEditable(self):
        """
        You can only set editable fields.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families, editable=[]))

        exc = self.failureResultOf(crud.create({'surname':'foo'})).value
        self.assertTrue(isinstance(exc, NotEditable))


    @defer.inlineCallbacks
    def test_create_notEditable_fixed(self):
        """
        Fixed fields can be used to update non-editable fields.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families, editable=[])).fix({'surname':'bo'})

        family = yield crud.create({})
        self.assertEqual(family['surname'], 'bo')


    @defer.inlineCallbacks
    def test_fix_succession(self):
        """
        You can fix attributes one after the other.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families, editable=['location', 'surname']))
        crud = crud.fix({'surname': 'Jones'})
        crud = crud.fix({'location': 'Sunnyville'})

        family = yield crud.create({})
        self.assertEqual(family['surname'], 'Jones')
        self.assertEqual(family['location'], 'Sunnyville')


    @defer.inlineCallbacks
    def test_fetch(self):
        """
        When you fetch, you see the viewable fields, which means every field
        by default.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families))
        yield crud.create({'surname': '13'})

        fams = yield crud.fetch()
        self.assertEqual(len(fams), 1)
        self.assertEqual(fams[0]['surname'], '13')


    @defer.inlineCallbacks
    def test_fetch_fixed(self):
        """
        Fixed attributes restrict the fetched objects.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families))
        yield crud.create({'surname': 'Jones'})

        crud2 = crud.fix({'surname': 'Johnson'})
        fams = yield crud2.fetch()
        self.assertEqual(len(fams), 0, "Should only find (non-existent) "
                         "records matching the fixed values")




