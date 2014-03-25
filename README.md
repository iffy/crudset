[![Build Status](https://secure.travis-ci.org/iffy/crudset.png?branch=master)](http://travis-ci.org/iffy/crudset)

# crudset #

A tool for automating the creation of CRUDs.

## Policies ##

In this code, there are two roles:

1. Managers, with full access
2. Users, with limited access

<!-- test -->

```python
from crudset.crud import Crud, Policy

from twisted.internet import defer, task

from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy import Boolean
from sqlalchemy.schema import CreateTable

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
                           strategy=TWISTED_STRATEGY)
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


## Fixed values ##

You can create child CRUDs with certain attributes fixed.  For example:

<!-- test -->

```python
from crudset.crud import Crud, Policy

from twisted.internet import defer, task

from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy.schema import CreateTable

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
                           strategy=TWISTED_STRATEGY)
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
from crudset.crud import Crud, Policy, Paginator

from twisted.internet import defer, task

from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy.schema import CreateTable

from alchimia import TWISTED_STRATEGY

metadata = MetaData()
Books = Table('books', metadata,
    Column('id', Integer, primary_key=True),
    Column('title', String),
)

@defer.inlineCallbacks
def main(reactor):
    import tempfile
    fh, name = tempfile.mkstemp()
    engine = create_engine('sqlite:///' + name,
                           connect_args={'check_same_thread': False},
                           reactor=reactor,
                           strategy=TWISTED_STRATEGY)
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