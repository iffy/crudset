`release` branch [![Build Status](https://secure.travis-ci.org/iffy/crudset.png?branch=release)](http://travis-ci.org/iffy/crudset)

`dev` branch [![Build Status](https://secure.travis-ci.org/iffy/crudset.png?branch=dev)](http://travis-ci.org/iffy/crudset)

# crudset #

A tool for automating the creation of CRUDs.

## Policies ##

In this code, there are two roles:

1. Managers, with full access
2. Users, with limited access

<!-- test -->

```python
from crudset import Crud, Policy

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


# managers can view and edit all the fields (except for changing the id)

manager_policy = Policy(people,
    required=['name', 'pay_grade'],
    readable=['id', 'is_soylent_green', 'name', 'pay_grade'],
    writeable=['is_soylent_green', 'name', 'pay_grade'],
)


# regular employees can only change their name, and can't see that they are
# soylent green.

employee_policy = Policy(people,
    required=['name', 'pay_grade'],
    readable=['id', 'name', 'pay_grade'],
    writeable=['name'],
)


@defer.inlineCallbacks
def main(reactor):
    engine = create_engine('sqlite://',
                           connect_args={'check_same_thread': False},
                           reactor=reactor,
                           strategy=TWISTED_STRATEGY,
                           poolclass=StaticPool)
    yield engine.execute(CreateTable(people))

    # manager
    manager_crud = Crud(engine, manager_policy)
    yield manager_crud.create({
        'is_soylent_green': True,
        'name': 'Fern',
        'pay_grade': 100,
    })
    yield manager_crud.create({
        'name': 'Joe',
        'pay_grade': 90,
    })

    # employee
    employee_crud = Crud(engine, employee_policy)
    employees = yield employee_crud.fetch()
    # This includes only the readable fields from the employee_policy above.
    print employees


task.react(main, [])
```

### Narrowing ###

You can build policies from other policies.

<!-- test -->

```python
from crudset import Policy

from sqlalchemy import MetaData, Table, Column, Integer, String, Boolean

metadata = MetaData()
people = Table('people', metadata,
    Column('id', Integer, primary_key=True),
    Column('is_soylent_green', Boolean),
    Column('name', String),
    Column('pay_grade', Integer),
)

# prevent changing the id at the system level
system_policy = Policy(people,
    writeable=['is_soylent_green', 'name', 'pay_grade'],
)

# prevent changing everything but name at the user level
user_policy = system_policy.narrow(
    readable=['name', 'pay_grade'],
    writeable=['name'],
)
```


## Fixed values ##

You can create child CRUDs with certain attributes fixed.  For example:

<!-- test -->

```python
from crudset import Crud, Policy

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

    main_crud = Crud(engine, Policy(people))    
    team3_crud = main_crud.fix({'team_id': 3})
    team4_crud = main_crud.fix({'team_id': 4})

    john = yield team4_crud.create({'name': 'John'})
    assert john['team_id'] == 4, john

    members = yield team3_crud.fetch()
    assert members == [], members

task.react(main, [])
```


## Pagination ##

You can paginate a CRUD.

<!-- test -->

```python
from crudset import Crud, Policy, Paginator

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
    
    crud = Crud(engine, Policy(Books))

    for i in xrange(432):
        yield crud.create({'title': 'Book %s' % (i,)})
    
    pager = Paginator(crud, page_size=13)
    
    count = yield pager.pageCount()
    assert count == 34, count

    page3 = yield pager.page(2)
    assert len(page3) == 13, page3
    print page3

    # you can filter, too
    count = yield pager.pageCount(Books.c.title.like('% 1'))
    print count

    page1 = yield pager.page(0, Books.c.title.like('% 1'))
    print page1

task.react(main, [])
```


## Table names ##

You can expose the table name of an object, or even map it to a different name.

<!-- test -->

```python
from crudset import Crud, Policy

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

    crud1 = Crud(engine, Policy(people), table_attr='mytable')

    john = yield crud1.create({'name': 'John'})
    assert john['mytable'] == 'people', john

    crud2 = Crud(engine, Policy(people), table_attr='Object',
                 table_map={people: 'Person'})
    people_list = yield crud2.fetch()
    person1 = people_list[0]
    assert person1['Object'] == 'Person', person1

task.react(main, [])
```