from twisted.internet import defer

from sqlalchemy.sql import select

from crudset.error import MissingRequiredFields, NotEditable



class Policy(object):
    """
    XXX
    """

    def __init__(self, table, required=None, editable=None):
        """
        @param required: List of required field names.
        @param editable: List of editable fields.  If C{None} then no fields
            are editable.
        """
        self.table = table
        self.viewable = list(table.columns)
        self.required = frozenset(required or [])
        if editable is None:
            self.editable = frozenset([x.name for x in table.columns])
        else:
            self.editable = frozenset(editable)


class Crud(object):
    """
    XXX
    """

    def __init__(self, engine, policy):
        """
        XXX
        """
        self.engine = engine
        self.policy = policy
        self._fixed = {}


    def fix(self, attrs):
        """
        Fix some attributes to a particular value.

        @param attrs: dict of attributes to fix.

        @return: A new L{Crud}.
        """
        crud = Crud(self.engine, self.policy)
        crud._fixed = self._fixed.copy()
        crud._fixed.update(attrs)
        return crud


    @defer.inlineCallbacks
    def create(self, attrs):
        # check for editability
        forbidden = set(attrs) - self.policy.editable
        if forbidden:
            raise NotEditable(', '.join(forbidden))

        # fixed attributes
        attrs.update(self._fixed)

        # check required fields
        missing = self.policy.required - set(attrs)
        if missing:
            raise MissingRequiredFields(', '.join(missing))

        # do it
        table = self.policy.table
        result = yield self.engine.execute(table.insert().values(**attrs))
        pk = result.inserted_primary_key
        obj = yield self._getOne(pk)
        defer.returnValue(obj)


    @defer.inlineCallbacks
    def fetch(self, where=None):
        """
        Get a set of records.

        @param where: Extra restriction of scope.
        """
        query = self._baseQuery()

        # filter by extra where.
        if where is not None:
            query = query.where(where)

        result = yield self.engine.execute(query)
        rows = yield result.fetchall()
        ret = []
        for row in rows:
            ret.append(self._rowToDict(row))
        defer.returnValue(ret)


    @defer.inlineCallbacks
    def update(self, attrs, where=None):
        """
        Update a set of records.
        """
        # check for editability
        forbidden = set(attrs) - self.policy.editable
        if forbidden:
            raise NotEditable(', '.join(forbidden))

        up = self.policy.table.update()
        up = self._applyConstraints(up)

        if where is not None:
            up = up.where(where)

        up = up.values(**attrs)
        yield self.engine.execute(up)

        rows = yield self.fetch(where)
        defer.returnValue(rows)


    def _baseQuery(self):
        base = select(self.policy.viewable)
        return self._applyConstraints(base)


    def _applyConstraints(self, query):
        if self._fixed:
            where = None
            for k, v in self._fixed.items():
                col = getattr(self.policy.table.c, k)
                comp = col == v
                if where:
                    where = where and comp
                else:
                    where = comp
            query = query.where(where)
        return query


    @defer.inlineCallbacks
    def _getOne(self, pk):
        # base query
        query = self._baseQuery()
        
        # pk
        table = self.policy.table
        where = [x == y for (x,y) in zip(table.primary_key.columns, pk)]
        query = query.where(*where)
        
        result = yield self.engine.execute(query)
        row = yield result.fetchone()
        data = self._rowToDict(row)
        defer.returnValue(data)


    def _rowToDict(self, row):
        d = {}
        for (col, v) in zip(self.policy.viewable, row):
            d[col.name] = v
        return d



class _CrudMaker(object):
    """
    I make it easy to create/read/update/delete SQL stuff.
    XXX Lucy, you have some 'splain to do!
    """

    def __init__(self, table, create_requires=None, viewable_attributes=None):
        """
        @param table: An SQLAlchemy Table.

        @param create_requires: A list of fields that must be provided when
            creating.
        @param viewable_attributes: The list of fields to return with read-like
            operations.
        """
        self.table = table
        self.create_requires = frozenset(create_requires or [])

        self.viewable_attributes = []
        if viewable_attributes is None:
            self.viewable_attributes = list(self.table.columns)
        else:
            viewable_attributes = viewable_attributes or []
            for attr in viewable_attributes:
                self.viewable_attributes.append(getattr(self.table.c, attr))


    def create(self, attrs):
        """
        Get an insert row statement.

        @param attrs: Dictionary of attributes to set on the rows.
        """
        # assert required attributes
        missing = self.create_requires - set(attrs)
        if missing:
            raise MissingRequiredFields(', '.join(missing))

        return self.table.insert().values(**attrs)


    def get(self, pk):
        """
        Get a row by the primary key.
        """
        where = [x == y for (x,y) in zip(self.table.primary_key.columns, pk)]
        return select(self.viewable_attributes).where(*where)


    def fetch(self, where):
        """
        Get a select statement.
        """
        return select(self.viewable_attributes)