[![Build Status](https://secure.travis-ci.org/iffy/crudset.png?branch=release)](http://travis-ci.org/iffy/crudset) `release` branch

[![Build Status](https://secure.travis-ci.org/iffy/crudset.png?branch=dev)](http://travis-ci.org/iffy/crudset) `dev` branch 

# crudset #

A tool for automating the creation of CRUDs.


## Defining Cruds ##

### Basic ###

<!-- test -->

```python
from crudset import crudFromSpec

from twisted.internet import defer, task

from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy.schema import CreateTable
from sqlalchemy.pool import StaticPool

from alchimia import TWISTED_STRATEGY

# SQLAlchemy table definition
metadata = MetaData()
people = Table('people', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('pay_grade', Integer),
)


# Crud specification
class PeopleSpec:
    table = people
    writeable = ['name', 'pay_grade']
people_crud = crudFromSpec(PeopleSpec)


# Use it
@defer.inlineCallbacks
def main(reactor):
    engine = create_engine('sqlite://',
                           connect_args={'check_same_thread': False},
                           reactor=reactor,
                           strategy=TWISTED_STRATEGY,
                           poolclass=StaticPool)
    yield engine.execute(CreateTable(people))

    # create
    joe = yield people_crud.create(engine, {
        'name': 'Joe',
        'pay_grade': 90,
    })

    assert joe['name'] == 'Joe', joe
    assert joe['pay_grade'] == 90, joe

    # update
    new_joes = yield people_crud.fix({'id': joe['id']}).update(engine, {
        'name': 'Joseph',
    })

    assert new_joes[0]['name'] == 'Joseph', new_joes

    # fetch
    all_the_joes = yield people_crud.fetch(engine)

    assert all_the_joes == new_joes, all_the_joes

    # delete
    yield people_crud.delete(engine)

task.react(main, [])
```


### Kitchen sink ###

<!-- test -->

```python
from crudset import crudFromSpec, Ref, Readset, Sanitizer

from twisted.internet import defer, task

from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy.schema import CreateTable
from sqlalchemy.pool import StaticPool

from alchimia import TWISTED_STRATEGY

# SQLAlchemy table definition
metadata = MetaData()
people = Table('people', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
)
pets = Table('pet', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('owner_id', Integer),
)


# Crud specification
class PetSpec:
    table = pets
    readable = [
        'name',
        'owner_id',
    ]
    writeable = [
        'name',
        'owner_id',
    ]
    references = {
        'owner': Ref(
            Readset(people, ['name']),
            people.c.id == pets.c.owner_id,
        )
    }

    sanitizer = Sanitizer(table)

    @sanitizer.sanitizeData
    def defaultName(self, context, data):
        if 'name' not in data:
            data['name'] = 'Fido the Fish'
        return data

    @sanitizer.sanitizeField('name')
    def titleCaseNames(self, context, data, field_name):
        return data[field_name].title()

pet_crud = crudFromSpec(PetSpec, table_attr='type', table_map={
    pets: 'Pet',
    people: 'Person',
})

class PeopleSpec:
    table = people
    writeable = ['name']
people_crud = crudFromSpec(PeopleSpec)


# Use it
@defer.inlineCallbacks
def main(reactor):
    engine = create_engine('sqlite://',
                           connect_args={'check_same_thread': False},
                           reactor=reactor,
                           strategy=TWISTED_STRATEGY,
                           poolclass=StaticPool)
    yield engine.execute(CreateTable(people))
    yield engine.execute(CreateTable(pets))

    # create a person
    joe = yield people_crud.create(engine, {
        'name': 'Joe',
    })

    # create a pet
    molly = yield pet_crud.create(engine, {
        'name': 'Molly',
        'owner_id': joe['id'],
    })

    assert molly['name'] == 'Molly'
    assert molly['owner']['name'] == 'Joe', repr(molly)

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
