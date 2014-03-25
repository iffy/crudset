from twisted.trial.unittest import TestCase
from twisted.internet import defer, reactor

from alchimia import TWISTED_STRATEGY

from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy.schema import CreateTable

from crudset.error import MissingRequiredFields, NotEditable
from crudset.crud import Crud, Policy, Paginator

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
    Column('id', Integer, primary_key=True),
    Column('location', String),
    Column('surname', String),
)

people = Table('people', metadata,
    Column('id', Integer, primary_key=True),
    Column('created', DateTime),
    Column('family_id', Integer, ForeignKey('family.id')),
    Column('name', String),
)

pets = Table('pets', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('family_id', Integer, ForeignKey('family.id')),
    Column('owner_id', Integer, ForeignKey('people.id')),
)



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
        yield engine.execute(CreateTable(pets))
        defer.returnValue(engine)


    @defer.inlineCallbacks
    def test_create(self):
        """
        You can create an object.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families, writeable=['surname']))

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
        crud = Crud(engine, Policy(families, writeable=['surname']))
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
        You can only set writeable fields.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families, writeable=[]))

        exc = self.failureResultOf(crud.create({'surname':'foo'})).value
        self.assertTrue(isinstance(exc, NotEditable))


    @defer.inlineCallbacks
    def test_create_notEditable_fixed(self):
        """
        Fixed fields can be used to update non-writeable fields.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families, writeable=[])).fix({'surname':'bo'})

        family = yield crud.create({})
        self.assertEqual(family['surname'], 'bo')


    @defer.inlineCallbacks
    def test_fix_succession(self):
        """
        You can fix attributes one after the other.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families, writeable=['location', 'surname']))
        crud = crud.fix({'surname': 'Jones'})
        crud = crud.fix({'location': 'Sunnyville'})

        family = yield crud.create({})
        self.assertEqual(family['surname'], 'Jones')
        self.assertEqual(family['location'], 'Sunnyville')


    @defer.inlineCallbacks
    def test_fetch(self):
        """
        When you fetch, you see the readable fields, which means every field
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


    @defer.inlineCallbacks
    def test_fetch_expression(self):
        """
        You can limit even further.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families))

        for i in xrange(10):
            yield crud.create({'surname': 'Family %d' % (i,)})

        family4 = yield crud.fetch(families.c.surname == 'Family 4')
        self.assertEqual(len(family4), 1)
        self.assertEqual(family4[0]['surname'], 'Family 4')


    @defer.inlineCallbacks
    def test_fetch_readable(self):
        """
        You can limit the set of readable fields.
        """
        engine = yield self.engine()
        crud1 = Crud(engine, Policy(families))
        yield crud1.create({'surname': 'Johnson', 'location': 'Alabama'})
        
        crud2 = Crud(engine, Policy(families, readable=['surname']))
        fams = yield crud2.fetch()
        self.assertEqual(fams[0], {'surname': 'Johnson'}, "Should only show "
                         "the readable fields.")


    @defer.inlineCallbacks
    def test_fetch_limit(self):
        """
        You can limit the number of returned records.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families))
        for i in xrange(10):
            yield crud.create({'surname': 'Johnson %d' % (i,)})

        fams = yield crud.fetch(limit=5)
        self.assertEqual(len(fams), 5)


    @defer.inlineCallbacks
    def test_fetch_order(self):
        """
        You can specify an ordering
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families))
        for i in xrange(10):
            yield crud.create({'surname': 'sodkevoiuans'[i]})
        
        fams = yield crud.fetch(order=families.c.surname)
        ordered = sorted(fams, key=lambda x:x['surname'])
        self.assertEqual(fams, ordered, "Should be ordered")


    @defer.inlineCallbacks
    def test_fetch_offset(self):
        """
        You can offset the limit.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families))
        fams = []
        for i in xrange(10):
            fam = yield crud.create({'surname': 'abcdefghijklmnop'[i]})
            fams.append(fam)

        results = yield crud.fetch(limit=5, offset=2, order=families.c.surname)
        self.assertEqual(results, fams[2:2+5])


    def test_writeableIsReadableSusbset(self):
        """
        The writeable list must be a subset of the readable list.
        """
        self.assertRaises(ValueError,
            Policy, families,
            writeable=['surname', 'location'],
            readable=['surname'])


    @defer.inlineCallbacks
    def test_update(self):
        """
        You can update sets.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families))
        yield crud.create({'surname': 'Jones'})
        fams = yield crud.update({'surname': 'Jamison'})
        self.assertEqual(len(fams), 1)
        self.assertEqual(fams[0]['surname'], 'Jamison')


    @defer.inlineCallbacks
    def test_update_fixed(self):
        """
        Fixed attributes are part of the update.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families))
        yield crud.create({'surname': 'Jones', 'location': 'anvilania'})
        yield crud.create({'surname': 'James', 'location': 'gotham'})

        crud2 = crud.fix({'surname': 'James'})
        yield crud2.update({'location': 'middle earth'})

        fams = yield crud.fetch(families.c.surname == u'Jones')
        self.assertEqual(fams[0]['location'], 'anvilania')

        fams = yield crud.fetch(families.c.surname == u'James')
        self.assertEqual(fams[0]['location'], 'middle earth')


    @defer.inlineCallbacks
    def test_update_expression(self):
        """
        You can filter the update by expression, too.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families))
        yield crud.create({'surname': 'Jones', 'location': 'anvilania'})
        yield crud.create({'surname': 'James', 'location': 'gotham'})

        fams = yield crud.update({'location': 'middle earth'},
                                 families.c.surname == 'James')
        self.assertEqual(len(fams), 1)

        fams = yield crud.fetch(families.c.surname == u'Jones')
        self.assertEqual(fams[0]['location'], 'anvilania')

        fams = yield crud.fetch(families.c.surname == u'James')
        self.assertEqual(fams[0]['location'], 'middle earth')


    @defer.inlineCallbacks
    def test_update_notEditable(self):
        """
        Only writeable fields are writeable.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families, writeable=['surname']))

        exc = self.failureResultOf(crud.update({'location':'foo'})).value
        self.assertTrue(isinstance(exc, NotEditable))


    @defer.inlineCallbacks
    def test_update_notEditable_fixed(self):
        """
        If you try to update an attribute that is fixed and not writeable,
        it shouldn't be writeable.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families, writeable=['surname']))
        crud = crud.fix({'location': '10'})

        exc = self.failureResultOf(crud.update({'location':'foo'})).value
        self.assertTrue(isinstance(exc, NotEditable))


    @defer.inlineCallbacks
    def test_delete(self):
        """
        You can delete sets of things.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families))
        yield crud.create({'surname': 'Jones'})
        yield crud.delete()
        fams = yield crud.fetch()
        self.assertEqual(len(fams), 0)


    @defer.inlineCallbacks
    def test_delete_fixed(self):
        """
        The fixed variables influence what is deleted.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families))
        yield crud.create({'surname': 'Jones'})
        crud2 = crud.fix({'surname': 'Arnold'})
        yield crud2.create({})
        yield crud2.delete()

        fams = yield crud.fetch()
        self.assertEqual(len(fams), 1, "Should have only deleted the fixed")
        self.assertEqual(fams[0]['surname'], 'Jones')


    @defer.inlineCallbacks
    def test_delete_expression(self):
        """
        You can filter by expression.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(families))
        yield crud.create({'surname': 'Jones'})
        yield crud.create({'surname': 'Arnold'})
        yield crud.delete(families.c.surname == 'Arnold')

        fams = yield crud.fetch()
        self.assertEqual(len(fams), 1, "Should have deleted Arnold")


    @defer.inlineCallbacks
    def test_references_null(self):
        """
        You can nest referenced tables when fetching.  They will be None
        if there is no row.
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(people), references=[
            ('family', Policy(families), people.c.family_id == families.c.id),
        ])

        yield crud.create({'name': 'Sam'})
        peeps = yield crud.fetch()
        self.assertEqual(len(peeps), 1)
        sam = peeps[0]
        self.assertEqual(sam['family'], None, str(sam))
        self.assertEqual(sam['name'], 'Sam')


    @defer.inlineCallbacks
    def test_references_notNull(self):
        """
        You can nest objects by reference.
        """
        engine = yield self.engine()
        fam_crud = Crud(engine, Policy(families))
        family = yield fam_crud.create({'surname': 'Jones'})

        crud = Crud(engine, Policy(people), references=[
            ('family', Policy(families), people.c.family_id == families.c.id),
        ])
        sam = yield crud.create({'name': 'Sam', 'family_id': family['id']})
        self.assertEqual(sam['family'], family)


    @defer.inlineCallbacks
    def test_references_multiple(self):
        """
        You can have multiple references.
        """
        engine = yield self.engine()
        fam_crud = Crud(engine, Policy(families))
        johnson = yield fam_crud.create({'surname': 'Johnson'})

        person_crud = Crud(engine, Policy(people))
        john = yield person_crud.create({
            'family_id': johnson['id'],
            'name': 'John',
        })

        pets_crud = Crud(engine, Policy(pets), references=[
            ('family', Policy(families), pets.c.family_id == families.c.id),
            ('owner', Policy(people), pets.c.owner_id == people.c.id),
        ])
        cat = yield pets_crud.create({
            'family_id': johnson['id'],
            'name': 'cat',
            'owner_id': john['id'],
        })
        self.assertEqual(cat['name'], 'cat')
        self.assertEqual(cat['family'], johnson)
        self.assertEqual(cat['owner'], john)

        dog = yield pets_crud.create({
            'name': 'dog',
            'owner_id': john['id']
        })
        self.assertEqual(dog['name'], 'dog')
        self.assertEqual(dog['owner'], john)
        self.assertEqual(dog['family'], None)

        fish = yield pets_crud.create({
            'name': 'bob',
            'family_id': johnson['id'],
        })
        self.assertEqual(fish['name'], 'bob')
        self.assertEqual(fish['owner'], None)
        self.assertEqual(fish['family'], johnson)



class PaginatorTest(TestCase):

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
        yield engine.execute(CreateTable(pets))
        defer.returnValue(engine)


    @defer.inlineCallbacks
    def test_page(self):
        """
        You can paginate a Crud
        """
        engine = yield self.engine()
        crud = Crud(engine, Policy(pets))
        pager = Paginator(crud, page_size=10, order=pets.c.id)

        monkeys = []
        for i in xrange(40):
            monkey = yield crud.create({'name': 'seamonkey %d' % (i,)})
            monkeys.append(monkey)

        page1 = yield pager.page(0)
        self.assertEqual(page1, monkeys[:10])
        page2 = yield pager.page(1)
        self.assertEqual(page2, monkeys[10:20])








