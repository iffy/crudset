from twisted.trial.unittest import TestCase
from twisted.internet import defer, reactor

from alchimia import TWISTED_STRATEGY

from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy.schema import CreateTable
from sqlalchemy.pool import StaticPool

from crudset.error import MissingRequiredFields, NotEditable, TooMany
from crudset.crud import Crud, Policy, Paginator, Ref

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
        crud = Crud(Policy(families, writeable=['surname']))

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
        crud = Crud(Policy(families, writeable=['surname']))
        crud = crud.fix({'surname':'Hammond'})

        family = yield crud.create(engine, {})
        self.assertEqual(family['surname'], 'Hammond')

        fam2 = yield crud.create(engine, {'surname': 'Jones'})
        self.assertEqual(fam2['surname'], 'Hammond')


    @defer.inlineCallbacks
    def test_create_required(self):
        """
        You can require fields to be set.
        """
        engine = yield self.engine()
        crud = Crud(Policy(families, required=['surname']))
        exc = self.failureResultOf(crud.create(engine, {})).value
        self.assertTrue(isinstance(exc, MissingRequiredFields), exc)


    @defer.inlineCallbacks
    def test_create_notEditable(self):
        """
        You can only set writeable fields.
        """
        engine = yield self.engine()
        crud = Crud(Policy(families, writeable=[]))

        exc = self.failureResultOf(crud.create(engine, {'surname':'foo'})).value
        self.assertTrue(isinstance(exc, NotEditable))


    @defer.inlineCallbacks
    def test_create_notEditable_fixed(self):
        """
        Fixed fields can be used to update non-writeable fields.
        """
        engine = yield self.engine()
        crud = Crud(Policy(families, writeable=[])).fix({'surname':'bo'})

        family = yield crud.create(engine, {})
        self.assertEqual(family['surname'], 'bo')


    @defer.inlineCallbacks
    def test_fix_succession(self):
        """
        You can fix attributes one after the other.
        """
        engine = yield self.engine()
        crud = Crud(Policy(families, writeable=['location', 'surname']))
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
        crud = Crud(Policy(families))
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
        crud = Crud(Policy(families))
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
        crud = Crud(Policy(families))

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
        crud1 = Crud(Policy(families))
        yield crud1.create(engine, {'surname': 'Johnson', 'location': 'Alabama'})
        
        crud2 = Crud(Policy(families, readable=['surname']))
        fams = yield crud2.fetch(engine)
        self.assertEqual(fams[0], {'surname': 'Johnson'}, "Should only show "
                         "the readable fields.")


    @defer.inlineCallbacks
    def test_fetch_limit(self):
        """
        You can limit the number of returned records.
        """
        engine = yield self.engine()
        crud = Crud(Policy(families))
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
        crud = Crud(Policy(families))
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
        crud = Crud(Policy(families))
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
        crud = Crud(Policy(families))
        fam = yield crud.create(engine, {'surname': 'hey'})
        one = yield crud.getOne(engine)
        self.assertEqual(one, fam)


    @defer.inlineCallbacks
    def test_getOne_where(self):
        """
        You can get one by a where clause
        """
        engine = yield self.engine()
        crud = Crud(Policy(families))
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
        crud = Crud(Policy(families))
        yield crud.create(engine, {'surname': 'bob'})
        yield crud.create(engine, {'surname': 'Jones'})
        self.assertFailure(crud.getOne(engine), TooMany)


    @defer.inlineCallbacks
    def test_getOne_None(self):
        """
        If there is no result, return None.
        """
        engine = yield self.engine()
        crud = Crud(Policy(families))
        one = yield crud.getOne(engine)
        self.assertEqual(one, None)


    @defer.inlineCallbacks
    def test_count(self):
        """
        You can count the records.
        """
        engine = yield self.engine()
        crud = Crud(Policy(families))
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
        crud = Crud(Policy(families))
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
        crud = Crud(Policy(families))
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
        crud = Crud(Policy(families))
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
        crud = Crud(Policy(families))
        yield crud.create(engine, {'surname': 'Jones', 'location': 'anvilania'})
        yield crud.create(engine, {'surname': 'James', 'location': 'gotham'})

        crud2 = crud.fix({'surname': 'James'})
        yield crud2.update(engine, {'location': 'middle earth'})

        fams = yield crud.fetch(engine, families.c.surname == u'Jones')
        self.assertEqual(fams[0]['location'], 'anvilania')

        fams = yield crud.fetch(engine, families.c.surname == u'James')
        self.assertEqual(fams[0]['location'], 'middle earth')


    @defer.inlineCallbacks
    def test_update_expression(self):
        """
        You can filter the update by expression, too.
        """
        engine = yield self.engine()
        crud = Crud(Policy(families))
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
    def test_update_notEditable(self):
        """
        Only writeable fields are writeable.
        """
        engine = yield self.engine()
        crud = Crud(Policy(families, writeable=['surname']))

        exc = self.failureResultOf(crud.update(engine, {'location':'foo'})).value
        self.assertTrue(isinstance(exc, NotEditable))


    @defer.inlineCallbacks
    def test_update_notEditable_fixed(self):
        """
        If you try to update an attribute that is fixed and not writeable,
        it shouldn't be writeable.
        """
        engine = yield self.engine()
        crud = Crud(Policy(families, writeable=['surname']))
        crud = crud.fix({'location': '10'})

        exc = self.failureResultOf(crud.update(engine, {'location':'foo'})).value
        self.assertTrue(isinstance(exc, NotEditable))


    @defer.inlineCallbacks
    def test_delete(self):
        """
        You can delete sets of things.
        """
        engine = yield self.engine()
        crud = Crud(Policy(families))
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
        crud = Crud(Policy(families))
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
        crud = Crud(Policy(families))
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
        crud = Crud(Policy(people, references=[
            Ref('family', Policy(families), people.c.family_id == families.c.id),
        ]))

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
        fam_crud = Crud(Policy(families))
        family = yield fam_crud.create(engine, {'surname': 'Jones'})

        crud = Crud(Policy(people, references=[
            Ref('family', Policy(families), people.c.family_id == families.c.id),
        ]))
        sam = yield crud.create(engine, {'name': 'Sam', 'family_id': family['id']})
        self.assertEqual(sam['family'], family)


    @defer.inlineCallbacks
    def test_references_multiple(self):
        """
        You can have multiple references.
        """
        engine = yield self.engine()
        fam_crud = Crud(Policy(families))
        johnson = yield fam_crud.create(engine, {'surname': 'Johnson'})

        person_crud = Crud(Policy(people))
        john = yield person_crud.create(engine, {
            'family_id': johnson['id'],
            'name': 'John',
        })

        pets_crud = Crud(Policy(pets, references=[
            Ref('family', Policy(families), pets.c.family_id == families.c.id),
            Ref('owner', Policy(people), pets.c.owner_id == people.c.id),
        ]))
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
        crud = Crud(Policy(families), table_attr='_object')
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
        fam_crud = Crud(Policy(families))
        family = yield fam_crud.create(engine, {'surname': 'Jones'})

        crud = Crud(Policy(people, references=[
            Ref('family', Policy(families), people.c.family_id == families.c.id),
        ]), table_attr='foo')
        sam = yield crud.create(engine, {'name': 'Sam', 'family_id': family['id']})
        self.assertEqual(sam['foo'], 'people')
        self.assertEqual(sam['family']['foo'], 'family')


    @defer.inlineCallbacks
    def test_table_map(self):
        """
        You can map table names to something else.
        """
        engine = yield self.engine()
        fam_crud = Crud(Policy(families))
        family = yield fam_crud.create(engine, {'surname': 'Jones'})

        crud = Crud(
            Policy(people, references=[
                Ref('family', Policy(families), people.c.family_id == families.c.id),
            ]),
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
            Policy(families),
            table_attr='foo',
            table_map={'foo': 'bar'},
        )
        fixed = crud.fix({'id': 56})
        self.assertEqual(fixed.table_attr, 'foo')
        self.assertEqual(fixed.table_map, {'foo': 'bar'})


    def test_withPolicy(self):
        """
        You can change the policy of the crud.
        """
        pol1 = Policy(families)
        crud = Crud(pol1,
                    table_attr='foo', table_map={'a': 'b'}).fix({'id':10})

        pol2 = Policy(pets)
        crud2 = crud.withPolicy(pol2)
        self.assertEqual(crud2.policy, pol2)
        self.assertEqual(crud2.table_attr, 'foo')
        self.assertEqual(crud2.table_map, {'a': 'b'})
        self.assertEqual(crud2._fixed, {'id': 10})


class PolicyTest(TestCase):


    def test_writeableDefaultReadable(self):
        """
        The writeable set should be the readable set if not specified.
        """
        p = Policy(families, readable=['surname'])
        self.assertEqual(p.writeable, set(['surname']))


    def test_writeableIsReadableSusbset(self):
        """
        The writeable list must be a subset of the readable list.
        """
        self.assertRaises(ValueError,
            Policy, families,
            writeable=['surname', 'location'],
            readable=['surname'])


    def test_narrow_readable(self):
        """
        You can create policies from other policies with the options narrowed.
        """
        base = Policy(families)
        narrowed = base.narrow(readable=['surname'])
        self.assertEqual(narrowed.table, base.table)
        self.assertEqual(narrowed.readable, set(['surname']))
        self.assertEqual(narrowed.writeable, set(['surname']))


    def test_narrow_writeable(self):
        """
        You can narrow the set of writeable fields.
        """
        base = Policy(families)
        narrowed = base.narrow(writeable=['surname'])
        self.assertEqual(narrowed.writeable, set(['surname']),
            "The writeable set should be as specified")
        self.assertEqual(narrowed.readable, base.readable,
            "The readable set should not be restricted")


    def test_narrow_required(self):
        """
        You can additionally require fields.
        """
        base = Policy(families, required=['surname'])
        narrowed = base.narrow(also_required=['location'])
        self.assertEqual(narrowed.required, set(['surname', 'location']),
                         "Required should be a union of fields")


    def test_narrow_references(self):
        """
        References should be maintained.
        """
        base = Policy(families, references=[Ref('pets', None, None)])
        narrowed = base.narrow()
        self.assertEqual(narrowed.references, base.references)


    def test_narrow_default(self):
        """
        By default, the narrower policy should not be larger than the base.
        """
        base = Policy(families,
            required=['surname'],
            readable=['surname', 'location'],
            writeable=['surname'])
        narrowed = base.narrow()
        self.assertEqual(narrowed.required, set(['surname']),
            "The required set should match the base")
        self.assertEqual(narrowed.readable, set(['surname', 'location']),
            "The readable set should match the base")
        self.assertEqual(narrowed.writeable, set(['surname']),
            "The writeable set should match the base")

    def test_narrow_readableSubset(self):
        """
        The readable set must be a subset of the base policy's readable set.
        """
        base = Policy(families, readable=['surname'])
        self.assertRaises(ValueError, base.narrow,
            readable=['surname', 'location'])


    def test_narrow_writeableSubset(self):
        """
        The writeable fields must be a subset of the base policy's writeable
        set.
        """
        base = Policy(families, writeable=['surname'])
        self.assertRaises(ValueError, base.narrow,
            writeable=['surname', 'location'])


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
        crud = Crud(Policy(pets))
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
        crud = Crud(Policy(pets))
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
        crud = Crud(Policy(pets))
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
        crud = Crud(Policy(pets))
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










