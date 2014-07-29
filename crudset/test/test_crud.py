from twisted.trial.unittest import TestCase
from twisted.internet import defer, reactor

from alchimia import TWISTED_STRATEGY

from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy.schema import CreateTable
from sqlalchemy.pool import StaticPool

from crudset.error import TooMany
from crudset.crud import Crud, Paginator, Ref, Sanitizer, Readset, crudFromSpec

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

    timeout = 10


    @defer.inlineCallbacks
    def engine(self):
        engine = create_engine('sqlite://',
                               connect_args={'check_same_thread': False},
                               reactor=reactor,
                               strategy=TWISTED_STRATEGY,
                               poolclass=StaticPool)
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
        crud = Crud(Readset(families),
                    Sanitizer(families, ['surname']))

        family = yield crud.create(engine, {'surname': 'Jones'})
        self.assertEqual(family['surname'], 'Jones')
        self.assertNotEqual(family['id'], None)
        self.assertEqual(family['location'], None)


    @defer.inlineCallbacks
    def test_create_fixed(self):
        """
        You can create a Crud with fixed attributes.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families),
                    Sanitizer(families, ['surname']))
        crud = crud.fix({'surname':'Hammond'})

        family = yield crud.create(engine, {})
        self.assertEqual(family['surname'], 'Hammond')

        fam2 = yield crud.create(engine, {'surname': 'Jones'})
        self.assertEqual(fam2['surname'], 'Hammond')


    @defer.inlineCallbacks
    def test_create_sanitize(self):
        """
        A policy's sanitizer should be used to sanitize fields.
        """
        engine = yield self.engine()
        called = []
        class Foo(object):
            sanitizer = Sanitizer(families)
            @sanitizer.sanitizeData
            def sani(self, engine, action, data, context):
                called.append(action)
                return {'surname': 'Jones'}

        crud = Crud(Readset(families), Foo().sanitizer)
        family = yield crud.create(engine, {})
        self.assertEqual(family['surname'], 'Jones')
        self.assertEqual(called, ['create'])


    @defer.inlineCallbacks
    def test_fix_succession(self):
        """
        You can fix attributes one after the other.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families),
                    Sanitizer(families, ['location', 'surname']))
        crud = crud.fix({'surname': 'Jones'})
        crud = crud.fix({'location': 'Sunnyville'})

        family = yield crud.create(engine, {})
        self.assertEqual(family['surname'], 'Jones')
        self.assertEqual(family['location'], 'Sunnyville')


    @defer.inlineCallbacks
    def test_fetch(self):
        """
        When you fetch, you see the readable fields, which means every field
        by default.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        yield crud.create(engine, {'surname': '13'})

        fams = yield crud.fetch(engine)
        self.assertEqual(len(fams), 1)
        self.assertEqual(fams[0]['surname'], '13')


    @defer.inlineCallbacks
    def test_fetch_fixed(self):
        """
        Fixed attributes restrict the fetched objects.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        yield crud.create(engine, {'surname': 'Jones'})

        crud2 = crud.fix({'surname': 'Johnson'})
        fams = yield crud2.fetch(engine)
        self.assertEqual(len(fams), 0, "Should only find (non-existent) "
                         "records matching the fixed values")


    @defer.inlineCallbacks
    def test_fetch_expression(self):
        """
        You can limit even further.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))

        for i in xrange(10):
            yield crud.create(engine, {'surname': 'Family %d' % (i,)})

        family4 = yield crud.fetch(engine, families.c.surname == 'Family 4')
        self.assertEqual(len(family4), 1)
        self.assertEqual(family4[0]['surname'], 'Family 4')


    @defer.inlineCallbacks
    def test_fetch_readable(self):
        """
        You can limit the set of readable fields.
        """
        engine = yield self.engine()
        crud1 = Crud(Readset(families))
        yield crud1.create(engine, {'surname': 'Johnson', 'location': 'Alabama'})
        
        crud2 = Crud(Readset(families, ['surname']))
        fams = yield crud2.fetch(engine)
        self.assertEqual(fams[0], {'surname': 'Johnson'}, "Should only show "
                         "the readable fields.")


    @defer.inlineCallbacks
    def test_fetch_limit(self):
        """
        You can limit the number of returned records.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        for i in xrange(10):
            yield crud.create(engine, {'surname': 'Johnson %d' % (i,)})

        fams = yield crud.fetch(engine, limit=5)
        self.assertEqual(len(fams), 5)


    @defer.inlineCallbacks
    def test_fetch_order(self):
        """
        You can specify an ordering
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        for i in xrange(10):
            yield crud.create(engine, {'surname': 'sodkevoiuans'[i]})
        
        fams = yield crud.fetch(engine, order=families.c.surname)
        ordered = sorted(fams, key=lambda x:x['surname'])
        self.assertEqual(fams, ordered, "Should be ordered")


    @defer.inlineCallbacks
    def test_fetch_offset(self):
        """
        You can offset the limit.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        fams = []
        for i in xrange(10):
            fam = yield crud.create(engine, {'surname': 'abcdefghijklmnop'[i]})
            fams.append(fam)

        results = yield crud.fetch(engine, limit=5, offset=2, order=families.c.surname)
        self.assertEqual(results, fams[2:2+5])


    @defer.inlineCallbacks
    def test_getOne(self):
        """
        You can get just one item.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        fam = yield crud.create(engine, {'surname': 'hey'})
        one = yield crud.getOne(engine)
        self.assertEqual(one, fam)


    @defer.inlineCallbacks
    def test_getOne_where(self):
        """
        You can get one by a where clause
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        fam1 = yield crud.create(engine, {'surname': 'bob'})
        yield crud.create(engine, {'surname': 'Jones'})
        one = yield crud.getOne(engine, families.c.surname == 'bob')
        self.assertEqual(one, fam1)


    @defer.inlineCallbacks
    def test_getOne_moreThanOne(self):
        """
        If getOne returns more than one, it's an exception.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        yield crud.create(engine, {'surname': 'bob'})
        yield crud.create(engine, {'surname': 'Jones'})
        self.assertFailure(crud.getOne(engine), TooMany)


    @defer.inlineCallbacks
    def test_getOne_None(self):
        """
        If there is no result, return None.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        one = yield crud.getOne(engine)
        self.assertEqual(one, None)


    @defer.inlineCallbacks
    def test_count(self):
        """
        You can count the records.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        for i in xrange(14):
            yield crud.create(engine, {'surname': str(i)})

        count = yield crud.count(engine)
        self.assertEqual(count, 14)


    @defer.inlineCallbacks
    def test_count_where(self):
        """
        You can count filtered records.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        for i in xrange(14):
            yield crud.create(engine, {'surname': str(i)})

        count = yield crud.count(engine, families.c.surname == '12')
        self.assertEqual(count, 1)


    @defer.inlineCallbacks
    def test_count_fixed(self):
        """
        The count is restricted by fixed attributes.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        yield crud.create(engine, {'surname': 'Jones'})
        yield crud.create(engine, {'surname': 'Arnold'})

        crud2 = crud.fix({'surname': 'Arnold'})
        count = yield crud2.count(engine)
        self.assertEqual(count, 1)


    @defer.inlineCallbacks
    def test_update(self):
        """
        You can update sets.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        yield crud.create(engine, {'surname': 'Jones'})
        fams = yield crud.update(engine, {'surname': 'Jamison'})
        self.assertEqual(len(fams), 1)
        self.assertEqual(fams[0]['surname'], 'Jamison')


    @defer.inlineCallbacks
    def test_update_fixed(self):
        """
        Fixed attributes are part of the update.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        yield crud.create(engine, {'surname': 'Jones', 'location': 'anvilania'})
        yield crud.create(engine, {'surname': 'James', 'location': 'gotham'})

        crud2 = crud.fix({'surname': 'James'})
        yield crud2.update(engine, {'location': 'middle earth'})

        fams = yield crud.fetch(engine, families.c.surname == u'Jones')
        self.assertEqual(fams[0]['location'], 'anvilania')

        fams = yield crud.fetch(engine, families.c.surname == u'James')
        self.assertEqual(fams[0]['location'], 'middle earth')


    @defer.inlineCallbacks
    def test_update_fixedNoChange(self):
        """
        You aren't allowed to update the fixed attributes.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        yield crud.create(engine, {'surname': 'Jones', 'location': 'bar'})

        crud2 = crud.fix({'surname': 'Jones'})
        fams = yield crud2.update(engine, {'surname': 'Allison',
                                           'location': 'hawaii'})
        fam = fams[0]
        self.assertEqual(fam['surname'], 'Jones', "Should keep fixed value")


    @defer.inlineCallbacks
    def test_update_nothing(self):
        """
        It's a no-op to update nothing.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        yield crud.create(engine, {'surname': 'Jones'})
        fams = yield crud.update(engine, {})
        fam = fams[0]
        self.assertEqual(fam['surname'], 'Jones')


    @defer.inlineCallbacks
    def test_update_allFixed(self):
        """
        All the fixed attributes should be taken into consideration.
        """
        engine = yield self.engine()
        crud = Crud(Readset(pets))
        yield crud.create(engine, {'name': 'Jones', 'family_id': 1})
        yield crud.create(engine, {'name': 'James', 'family_id': 20})
        yield crud.create(engine, {'name': 'Jones', 'family_id': 20})
        yield crud.create(engine, {'name': 'James', 'family_id': 1})

        crud2 = crud.fix({'name': 'James', 'family_id': 20})
        yield crud2.update(engine, {'owner_id': -1})

        fams = yield crud.fetch(engine)
        actual = set()
        for fam in fams:
            actual.add((fam['owner_id'], fam['name'], fam['family_id']))

        expected = set([
            (None, 'Jones', 1),
            (-1, 'James', 20),
            (None, 'Jones', 20),
            (None, 'James', 1),
        ])
        self.assertEqual(actual, expected, "Should only change the one thing")


    @defer.inlineCallbacks
    def test_update_expression(self):
        """
        You can filter the update by expression, too.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        yield crud.create(engine, {'surname': 'Jones', 'location': 'anvilania'})
        yield crud.create(engine, {'surname': 'James', 'location': 'gotham'})

        fams = yield crud.update(engine, {'location': 'middle earth'},
                                 families.c.surname == 'James')
        self.assertEqual(len(fams), 1)

        fams = yield crud.fetch(engine, families.c.surname == u'Jones')
        self.assertEqual(fams[0]['location'], 'anvilania')

        fams = yield crud.fetch(engine, families.c.surname == u'James')
        self.assertEqual(fams[0]['location'], 'middle earth')


    @defer.inlineCallbacks
    def test_update_sanitize(self):
        """
        A sanitizer should be used to sanitize fields on update.
        """
        engine = yield self.engine()
        called = []
        class Foo(object):
            sanitizer = Sanitizer(families)
            @sanitizer.sanitizeData
            def sani(self, engine, action, data, context):
                called.append(action)
                return {'surname': 'Jones'}

        crud = Crud(Readset(families), Foo().sanitizer)
        family = yield crud.create(engine, {})
        called.pop()
        fams = yield crud.update(engine, {'surname': 'Arnold'},
                                 families.c.id==family['id'])
        self.assertEqual(fams[0]['surname'], 'Jones')
        self.assertEqual(called, ['update'])



    @defer.inlineCallbacks
    def test_delete(self):
        """
        You can delete sets of things.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        yield crud.create(engine, {'surname': 'Jones'})
        yield crud.delete(engine)
        fams = yield crud.fetch(engine, )
        self.assertEqual(len(fams), 0)


    @defer.inlineCallbacks
    def test_delete_fixed(self):
        """
        The fixed variables influence what is deleted.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        yield crud.create(engine, {'surname': 'Jones'})
        crud2 = crud.fix({'surname': 'Arnold'})
        yield crud2.create(engine, {})
        yield crud2.delete(engine)

        fams = yield crud.fetch(engine, )
        self.assertEqual(len(fams), 1, "Should have only deleted the fixed")
        self.assertEqual(fams[0]['surname'], 'Jones')


    @defer.inlineCallbacks
    def test_delete_expression(self):
        """
        You can filter by expression.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families))
        yield crud.create(engine, {'surname': 'Jones'})
        yield crud.create(engine, {'surname': 'Arnold'})
        yield crud.delete(engine, families.c.surname == 'Arnold')

        fams = yield crud.fetch(engine, )
        self.assertEqual(len(fams), 1, "Should have deleted Arnold")


    @defer.inlineCallbacks
    def test_references_null(self):
        """
        You can nest referenced tables when fetching.  They will be None
        if there is no row.
        """
        engine = yield self.engine()
        crud = Crud(Readset(people, references={
            'family': Ref(Readset(families),
                          people.c.family_id == families.c.id),
        }))

        yield crud.create(engine, {'name': 'Sam'})
        peeps = yield crud.fetch(engine, )
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
        fam_crud = Crud(Readset(families))
        family = yield fam_crud.create(engine, {'surname': 'Jones'})

        crud = Crud(Readset(people, references={
            'family': Ref(Readset(families), people.c.family_id == families.c.id),
        }))
        sam = yield crud.create(engine, {'name': 'Sam', 'family_id': family['id']})
        self.assertEqual(sam['family'], family)


    @defer.inlineCallbacks
    def test_references_multiple(self):
        """
        You can have multiple references.
        """
        engine = yield self.engine()
        fam_crud = Crud(Readset(families))
        johnson = yield fam_crud.create(engine, {'surname': 'Johnson'})

        person_crud = Crud(Readset(people))
        john = yield person_crud.create(engine, {
            'family_id': johnson['id'],
            'name': 'John',
        })

        pets_crud = Crud(Readset(pets, references={
            'family': Ref(Readset(families), pets.c.family_id == families.c.id),
            'owner': Ref(Readset(people), pets.c.owner_id == people.c.id),
        }))

        cat = yield pets_crud.create(engine, {
            'family_id': johnson['id'],
            'name': 'cat',
            'owner_id': john['id'],
        })
        self.assertEqual(cat['name'], 'cat')
        self.assertEqual(cat['family'], johnson)
        self.assertEqual(cat['owner'], john)

        dog = yield pets_crud.create(engine, {
            'name': 'dog',
            'owner_id': john['id']
        })
        self.assertEqual(dog['name'], 'dog')
        self.assertEqual(dog['owner'], john)
        self.assertEqual(dog['family'], None)

        fish = yield pets_crud.create(engine, {
            'name': 'bob',
            'family_id': johnson['id'],
        })
        self.assertEqual(fish['name'], 'bob')
        self.assertEqual(fish['owner'], None)
        self.assertEqual(fish['family'], johnson)


    @defer.inlineCallbacks
    def test_table_attr(self):
        """
        You can expose the table names as an attribute.
        """
        engine = yield self.engine()
        crud = Crud(Readset(families), table_attr='_object')
        r = yield crud.create(engine, {'surname': 'Jones'})
        self.assertEqual(r['_object'], 'family')
        
        rlist = yield crud.fetch(engine)
        self.assertEqual(rlist[0]['_object'], 'family')

        rlist = yield crud.update(engine, {'surname': 'Jamison'})
        self.assertEqual(rlist[0]['_object'], 'family')


    @defer.inlineCallbacks
    def test_table_attr_reference(self):
        """
        table attr works with references, too.
        """
        engine = yield self.engine()
        fam_crud = Crud(Readset(families))
        family = yield fam_crud.create(engine, {'surname': 'Jones'})

        crud = Crud(Readset(people, references={
            'family': Ref(Readset(families), people.c.family_id == families.c.id),
        }), table_attr='foo')
        sam = yield crud.create(engine, {'name': 'Sam', 'family_id': family['id']})
        self.assertEqual(sam['foo'], 'people')
        self.assertEqual(sam['family']['foo'], 'family')


    @defer.inlineCallbacks
    def test_table_map(self):
        """
        You can map table names to something else.
        """
        engine = yield self.engine()
        fam_crud = Crud(Readset(families))
        family = yield fam_crud.create(engine, {'surname': 'Jones'})

        crud = Crud(
            Readset(people, references={
                'family': Ref(Readset(families), people.c.family_id == families.c.id),
            }),
            table_attr='foo',
            table_map={
                people: 'Person',
                families: 'Aardvark',
            },
        )
        sam = yield crud.create(engine, {'name': 'Sam', 'family_id': family['id']})
        self.assertEqual(sam['foo'], 'Person')
        self.assertEqual(sam['family']['foo'], 'Aardvark')


    def test_table_map_attr_fix(self):
        """
        Fixed Cruds should retain the table_attr and map.
        """
        crud = Crud(
            Readset(families),
            table_attr='foo',
            table_map={'foo': 'bar'},
        )
        fixed = crud.fix({'id': 56})
        self.assertEqual(fixed.table_attr, 'foo')
        self.assertEqual(fixed.table_map, {'foo': 'bar'})



class ReadsetTest(TestCase):


    def test_default(self):
        """
        By default, all columns are read.
        """
        r = Readset(families)
        self.assertEqual(r.readable, set(['id', 'location', 'surname']))
        self.assertEqual(r.readable_columns, list(families.columns))
        self.assertEqual(r.references, {})


    def test_readable(self):
        """
        You can specify a list of columns that are readable.
        """
        r = Readset(families, ['location'])
        self.assertEqual(r.readable, set(['location']))
        self.assertEqual(r.readable_columns, [families.c.location])


    def test_references(self):
        """
        You can specify a mapping of references.
        """
        ref = Ref(Readset(families), people.c.family_id == families.c.id)
        r = Readset(people, references={'family': ref})
        self.assertEqual(r.references, {'family': ref})



class PaginatorTest(TestCase):

    timeout = 10


    @defer.inlineCallbacks
    def engine(self):
        engine = create_engine('sqlite://',
                               connect_args={'check_same_thread': False},
                               reactor=reactor,
                               strategy=TWISTED_STRATEGY,
                               poolclass=StaticPool)
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
        crud = Crud(Readset(pets))
        pager = Paginator(crud, page_size=10, order=pets.c.id)

        monkeys = []
        for i in xrange(40):
            monkey = yield crud.create(engine, {'name': 'seamonkey %d' % (i,)})
            monkeys.append(monkey)

        page1 = yield pager.page(engine, 0)
        self.assertEqual(page1, monkeys[:10])
        page2 = yield pager.page(engine, 1)
        self.assertEqual(page2, monkeys[10:20])


    @defer.inlineCallbacks
    def test_page_where(self):
        """
        You can paginate filtered results, too
        """
        engine = yield self.engine()
        crud = Crud(Readset(pets))
        pager = Paginator(crud, page_size=3, order=pets.c.id)

        things = []
        _things = [
            {'name': 'thing 1'},
            {'name': 'thing 2'},
            {'name': 'dog'},
            {'name': 'cat'},
            {'name': 'dog'},
        ]
        for thing in _things:
            t = yield crud.create(engine, thing)
            things.append(t)

        page1 = yield pager.page(engine, 0, pets.c.name.startswith('thing'))
        self.assertEqual(page1, [things[0], things[1]])
        count = yield pager.pageCount(engine, pets.c.name.startswith('thing'))
        self.assertEqual(count, 1)


    @defer.inlineCallbacks
    def test_pageCount(self):
        """
        You can count the pages
        """
        engine = yield self.engine()
        crud = Crud(Readset(pets))
        pager = Paginator(crud, page_size=10, order=pets.c.id)

        monkeys = []
        for i in xrange(43):
            monkey = yield crud.create(engine, {'name': 'seamonkey %d' % (i,)})
            monkeys.append(monkey)

        pages = yield pager.pageCount(engine)
        self.assertEqual(pages, 5)


    @defer.inlineCallbacks
    def test_pageCountForills(self):
        """
        The page count should be accurate for all numbers.
        """
        engine = yield self.engine()
        crud = Crud(Readset(pets))
        pager = Paginator(crud, page_size=3, order=pets.c.id)
        
        count = yield pager.pageCount(engine)
        self.assertEqual(count, 0, "no records, no pages")

        yield crud.create(engine, {})
        count = yield pager.pageCount(engine)
        self.assertEqual(count, 1, "1 record, 1 page")

        yield crud.create(engine, {})
        yield crud.create(engine, {})
        count = yield pager.pageCount(engine)
        self.assertEqual(count, 1, "3 records, 1 page")

        yield crud.create(engine, {})
        count = yield pager.pageCount(engine)
        self.assertEqual(count, 2, "4 records, 2 pages")



class SanitizerTest(TestCase):


    @defer.inlineCallbacks
    def test_default(self):
        """
        An empty sanitizer will allow any column to be updated and strip out
        unknown fields.
        """
        class Foo(object):
            sanitizer = Sanitizer(pets)
        sanitizer = Foo().sanitizer
        
        data = {'foo': 'bar', 'id': 12, 'family_id': 19, 'owner_id': -1,
                'name': 'bob'}
        output = yield sanitizer.sanitize('engine', 'update', data)
        self.assertEqual(output,
            {'id': 12, 'family_id': 19, 'owner_id': -1, 'name': 'bob'})


    @defer.inlineCallbacks
    def test_writeable(self):
        """
        You can specify a list of writeable fields.  Non-writeable fields will
        be removed from the sanitized data.
        """
        class Foo(object):
            sanitizer = Sanitizer(pets, writeable=['name'])
        sanitizer = Foo().sanitizer
        
        data = {'foo': 'bar', 'id': 12, 'family_id': 19, 'owner_id': -1,
                'name': 'bob'}
        output = yield sanitizer.sanitize('engine', 'update', data)
        self.assertEqual(output, {'name': 'bob'})


    @defer.inlineCallbacks
    def test_sanitizeData(self):
        """
        You can specify a function that will sanitize the whole piece of data.
        """
        called = {}
        class Foo(object):
            sanitizer = Sanitizer(pets)

            @sanitizer.sanitizeData
            def myFunc(self, engine, action, data, context):
                called['engine'] = engine
                called['action'] = action
                called['data'] = data
                called['context'] = context
                return {'name': 'john'}

        sanitizer = Foo().sanitizer

        indata = {
            'foo': 'bar',
            'name': 'bob',
        }
        output = yield sanitizer.sanitize('engine', 'update', indata)
        self.assertEqual(output, {'name': 'john'})
        self.assertEqual(called['engine'], 'engine')
        self.assertEqual(called['action'], 'update')
        self.assertEqual(called['data'], indata)
        self.assertEqual(called['context'], {})


    @defer.inlineCallbacks
    def test_sanitizeField(self):
        """
        You can sanitize individual fields.
        """
        called = {}
        class Foo(object):
            sanitizer = Sanitizer(pets)

            @sanitizer.sanitizeField('name')
            def name(self, engine, action, data, field, context):
                called['engine'] = engine
                called['action'] = action
                called['data'] = data.copy()
                called['field'] = field
                called['context'] = context
                return 'new name'

        sanitizer = Foo().sanitizer

        indata = {'name': 'sam'}
        output = yield sanitizer.sanitize('engine', 'update', indata)
        self.assertEqual(output, {'name': 'new name'})
        self.assertEqual(called['engine'], 'engine')
        self.assertEqual(called['action'], 'update')
        self.assertEqual(called['data'], {'name': 'sam'})
        self.assertEqual(called['field'], 'name')
        self.assertEqual(called['context'], {})


    @defer.inlineCallbacks
    def test_sanitizeField_order(self):
        """
        Fields are sanitized in the order added.
        """
        called = []
        class Foo(object):
            sanitizer = Sanitizer(pets)

            @sanitizer.sanitizeField('name')
            def name(self, engine, action, data, field, context):
                called.append('name')
                return data[field]

            @sanitizer.sanitizeField('family_id')
            def family_id(self, engine, action, data, field, context):
                called.append('family_id')
                return data[field]

        sanitizer = Foo().sanitizer

        indata = {'name': 'sam', 'family_id': 12}
        output = yield sanitizer.sanitize('engine', 'update', indata)
        self.assertEqual(output, {'name': 'sam', 'family_id': 12})
        self.assertEqual(called, ['name', 'family_id'], "Should be called "
                         "in the order added")


    @defer.inlineCallbacks
    def test_sanitizeField_onlyCalledIfPresent(self):
        """
        The sanitizeField sanitizers should only be called if the field is
        present in the update/create data.
        """
        called = []
        class Foo(object):
            sanitizer = Sanitizer(pets)

            @sanitizer.sanitizeField('name')
            def name(self, engine, action, data, field, context):
                called.append('name')
                return data[field]

        sanitizer = Foo().sanitizer

        indata = {'family_id': 12}
        output = yield sanitizer.sanitize('engine', 'update', indata)
        self.assertEqual(output, {'family_id': 12})
        self.assertEqual(called, [], "Should not call name validator since "
                         "name wasn't present")


    def test_getSanitizedFields(self):
        """
        You can list the fields that are being sanitized.
        """
        class Foo(object):
            sanitizer = Sanitizer(pets)

            @sanitizer.sanitizeField('name')
            def name(self, engine, action, data, field, context):
                pass

        self.assertEqual(Foo.sanitizer.getSanitizedFields(), ['name'])



class crudFromSpecTest(TestCase):


    def test_defaults(self):
        """
        By default, all columns are readable and all are writeable
        """
        class Base:
            table = families
        crud = crudFromSpec(Base)
        
        # readset
        self.assertTrue(isinstance(crud.readset, Readset))
        self.assertEqual(crud.readset.table, families)
        self.assertEqual(crud.readset.readable, list(families.columns),
            "All columns should be readable by default")
        self.assertEqual(crud.readset.references, {})

        self.assertEqual(crud.table_attr, None)
        self.assertEqual(crud.table_map, {})

        # sanitizer
        from crudset.crud import _BoundSanitizer
        self.assertTrue(isinstance(crud.sanitizer, _BoundSanitizer))
        self.assertEqual(crud.sanitizer.table, families)
        self.assertEqual(crud.sanitizer.sanitizer.writeable_columns, set())














