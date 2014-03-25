from twisted.internet import defer

from sqlalchemy.sql import select

from crudset.error import MissingRequiredFields, NotEditable



class Policy(object):
    """
    XXX
    """

    def __init__(self, table, required=None, writeable=None, readable=None):
        """
        @param required: List of required field names.
        @param writeable: List of writeable fields.  If C{None} then all
            readable fields are writeable.
        @param readable: List of readable fields.  If C{None} then all
            writeable fields are readable.
        """
        self.table = table
        self.required = frozenset(required or [])
        
        # readable
        if readable is None:
            self.readable_columns = list(table.columns)
        else:
            self.readable_columns = [getattr(table.c, x) for x in readable]
        self.readable = frozenset([x.name for x in self.readable_columns])

        # writeable
        if writeable is None:
            self.writeable = self.readable
        else:
            self.writeable = frozenset(writeable)
        self.writeable_columns = [getattr(table.c, x) for x in self.writeable]

        if self.writeable > self.readable:
            raise ValueError('writeable columns must be a subset of readable '
                             'columns: writeable: %r, readable: %r' % (
                             self.writeable, self.readable))




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
        forbidden = set(attrs) - self.policy.writeable
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
        forbidden = set(attrs) - self.policy.writeable
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


    @defer.inlineCallbacks
    def delete(self, where=None):
        """
        Delete a set of records.
        """
        delete = self.policy.table.delete()
        delete = self._applyConstraints(delete)

        if where is not None:
            delete = delete.where(where)

        yield self.engine.execute(delete)


    def _baseQuery(self):
        base = select(self.policy.readable_columns)
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
        for (col, v) in zip(self.policy.readable_columns, row):
            d[col.name] = v
        return d


