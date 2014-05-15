[![Build Status](https://secure.travis-ci.org/iffy/crudset.png?branch=release)](http://travis-ci.org/iffy/crudset) `release` branch

[![Build Status](https://secure.travis-ci.org/iffy/crudset.png?branch=dev)](http://travis-ci.org/iffy/crudset) `dev` branch 

# crudset #

A tool for automating the creation of CRUDs.

## Read/Write ##

Use a `Readset` to specify what fields are read.  Use a `Writeset` to specify
which fields can be updated.

<!-- test -->

```python
from crudset import Crud, Readset, Writeset

from twisted.internet import defer, task

from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy import Boolean
from sqlalchemy.schema import CreateTable
from sqlalchemy.pool import StaticPool

from alchimia import TWISTED_STRATEGY

metadata = MetaData()
people = Table('people', metadata,
    Column('id', Integer, primary_key=True),
    Column('is_soylent_green', Boolean),
    Column('name', String),
    Column('pay_grade', Integer),
)

# The public crud will return only the id and name fields and will allow
# writing of name and pay_grade.
public_crud = Crud(
    Readset(people, ['id', 'name']),
    Writeset(people, ['name', 'pay_grade']))

# The private crud will return all fields and allows writing all fields.
private_crud = Crud(
    Readset(people),
    Writeset(people, people.columns))


@defer.inlineCallbacks
def main(reactor):
    engine = create_engine('sqlite://',
                           connect_args={'check_same_thread': False},
                           reactor=reactor,
                           strategy=TWISTED_STRATEGY,
                           poolclass=StaticPool)
    yield engine.execute(CreateTable(people))

    # private
    private_joe = yield private_crud.create(engine, {
        'name': 'Joe',
        'pay_grade': 90,
    })
    assert 'id' in private_joe
    assert 'name' in private_joe
    assert 'pay_grade' in private_joe
    assert 'is_soylent_green' in private_joe

    # public
    everyone = yield public_crud.fetch(engine)
    public_joe = everyone[0]
    assert 'pay_grade' not in public_joe, public_joe
    assert 'is_soylent_green' not in public_joe, public_joe
    assert 'id' in public_joe, public_joe
    assert 'name' in public_joe, public_joe


task.react(main, [])
```

## Sanitization ##

Use a `Sanitizer` to restrict/modify how fields can be written.

<!-- test -->

```python
from crudset import Crud, Readset, Sanitizer

from twisted.internet import defer, task

from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy import Boolean
from sqlalchemy.schema import CreateTable
from sqlalchemy.pool import StaticPool

from alchimia import TWISTED_STRATEGY

metadata = MetaData()
people = Table('people', metadata,
    Column('id', Integer, primary_key=True),
    Column('is_soylent_green', Boolean),
    Column('name', String),
    Column('pay_grade', Integer),
)

class EveryoneIsSoylent(object):
    sanitizer = Sanitizer(people)

    @sanitizer.sanitizeData
    def data(self, context, data):
        data['is_soylent_green'] = True
        return data

crud = Crud(Readset(people), EveryoneIsSoylent().sanitizer)


@defer.inlineCallbacks
def main(reactor):
    engine = create_engine('sqlite://',
                           connect_args={'check_same_thread': False},
                           reactor=reactor,
                           strategy=TWISTED_STRATEGY,
                           poolclass=StaticPool)
    yield engine.execute(CreateTable(people))

    joe = yield crud.create(engine, {
        'name': 'Joe',
        'pay_grade': 90,
    })
    assert joe['is_soylent_green'] == True, joe
    
    peeps = yield crud.update(engine, {'is_soylent_green': False})
    assert peeps[0]['is_soylent_green'] == True, peeps



task.react(main, [])
```


## Fixed values ##

You can create child CRUDs with certain attributes fixed.  For example:

<!-- test -->

```python
from crudset import Crud, Readset, Writeset

from twisted.internet import defer, task

from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy.schema import CreateTable
from sqlalchemy.pool import StaticPool

from alchimia import TWISTED_STRATEGY

metadata = MetaData()
people = Table('people', metadata,
    Column('id', Integer, primary_key=True),
    Column('team_id', Integer),
    Column('name', String),
)


@defer.inlineCallbacks
def main(reactor):
    engine = create_engine('sqlite://',
                           connect_args={'check_same_thread': False},
                           reactor=reactor,
                           strategy=TWISTED_STRATEGY,
                           poolclass=StaticPool)
    yield engine.execute(CreateTable(people))

    main_crud = Crud(Readset(people), Writeset(people, people.columns))
    team3_crud = main_crud.fix({'team_id': 3})
    team4_crud = main_crud.fix({'team_id': 4})

    john = yield team4_crud.create(engine, {'name': 'John'})
    assert john['team_id'] == 4, john

    members = yield team3_crud.fetch(engine)
    assert members == [], members

task.react(main, [])
```


## Pagination ##

You can paginate a CRUD.

<!-- test -->

```python
from crudset import Crud, Readset, Paginator, Writeset

from twisted.internet import defer, task

from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy.schema import CreateTable
from sqlalchemy.pool import StaticPool

from alchimia import TWISTED_STRATEGY

metadata = MetaData()
Books = Table('books', metadata,
    Column('id', Integer, primary_key=True),
    Column('title', String),
)

@defer.inlineCallbacks
def main(reactor):
    engine = create_engine('sqlite://',
                           connect_args={'check_same_thread': False},
                           reactor=reactor,
                           strategy=TWISTED_STRATEGY,
                           poolclass=StaticPool)
    yield engine.execute(CreateTable(Books))
    
    crud = Crud(Readset(Books), Writeset(Books, Books.columns))

    for i in xrange(432):
        yield crud.create(engine, {'title': 'Book %s' % (i,)})
    
    pager = Paginator(crud, page_size=13)
    
    count = yield pager.pageCount(engine)
    assert count == 34, count

    page3 = yield pager.page(engine, 2)
    assert len(page3) == 13, page3
    print page3

    # you can filter, too
    count = yield pager.pageCount(engine, Books.c.title.like('% 1'))
    print count

    page1 = yield pager.page(engine, 0, Books.c.title.like('% 1'))
    print page1

task.react(main, [])
```


## Table names ##

You can expose the table name of an object, or even map it to a different name.

<!-- test -->

```python
from crudset import Crud, Readset, Writeset

from twisted.internet import defer, task

from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy.schema import CreateTable
from sqlalchemy.pool import StaticPool

from alchimia import TWISTED_STRATEGY

metadata = MetaData()
people = Table('people', metadata,
    Column('id', Integer, primary_key=True),
    Column('team_id', Integer),
    Column('name', String),
)


@defer.inlineCallbacks
def main(reactor):
    engine = create_engine('sqlite://',
                           connect_args={'check_same_thread': False},
                           reactor=reactor,
                           strategy=TWISTED_STRATEGY,
                           poolclass=StaticPool)
    yield engine.execute(CreateTable(people))

    crud1 = Crud(Readset(people), Writeset(people, people.columns),
                 table_attr='mytable')

    john = yield crud1.create(engine, {'name': 'John'})
    assert john['mytable'] == 'people', john

    crud2 = Crud(Readset(people), table_attr='Object',
                 table_map={people: 'Person'})
    people_list = yield crud2.fetch(engine)
    person1 = people_list[0]
    assert person1['Object'] == 'Person', person1

task.react(main, [])
```
