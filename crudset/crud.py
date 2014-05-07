from twisted.internet import defer

from sqlalchemy.sql import select, and_

from crudset.error import MissingRequiredFields, NotEditable



class Policy(object):
    """
    I am a read-write policy for a table's attributes.
    """

    def __init__(self, table, required=None, writeable=None, readable=None):
        """
        @param required: List of required field names.

        @param writeable: List of writeable fields.  If C{None} then all
            readable fields are writeable.
        
        @param readable: List of readable fields.  If C{None} then all
            fields are readable.
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


    def narrow(self, also_required=None, writeable=None, readable=None):
        """
        Create a narrower, more restricted L{Policy}.

        @param also_required: The list of B{additional} fields to require.

        @param writeable: List of writeable fields.  If C{None} then the
            writeable fields will be the narrower of the base writeable set and
            C{readable}.

        @param readable: List of readable fields.  If C{None} then this will
            be the same as the base readable set.
        """
        required = set(also_required or []) | self.required
        readable = readable or self.readable
        
        if writeable is None:
            writeable = set(readable) & self.writeable

        # make sure readable is a subset
        extra = set(readable) - self.readable
        if extra:
            raise ValueError("Readable set isn't a subset of base policy."
                             "  These are extra: %r" % (extra,))

        # make sure writeable is a subset
        extra = set(writeable) - self.writeable
        if extra:
            raise ValueError("Writeable set isn't a subset of base policy."
                             "  These are extra: %r" % (extra,))

        return Policy(
            self.table,
            required=required,
            writeable=writeable,
            readable=readable)



class Crud(object):
    """
    This turns a L{Policy} into a CRUD.  See my L{create}, L{fetch}, L{count},
    L{update} and L{delete} methods.

    Also, you can use L{fix} to make new L{Crud} instances with certain
    attributes fixed (unchangeable by the user).
    """

    def __init__(self, engine, policy, references=[], table_attr=None,
                 table_map=None):
        """
        @param engine: An SQLAlchemy engine; preferrably one made with
            alchimia's TWISTED_STRATEGY.

        @param policy: A L{Policy} instance.

        @param references: A list of tuples with 3 things:
            (attr_name, policy, join_on constraint)

        @param table_attr: If set, then the data dictionaries returned by my
            methods will contain an item with C{table_attr} key and SQL table
            name as the value.  Man that's confusing...

        @param table_map: If C{table_attr} is set then this dictionary will
            map table names to something else.
        """
        self.engine = engine
        self.policy = policy
        self.references = references
        self.table_attr = table_attr
        self.table_map = table_map or {}
        self._fixed = {}
        self._select_columns = []
        self._base_query = None


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
    def fetch(self, where=None, order=None, limit=None, offset=None):
        """
        Get a set of records.

        @param where: Extra restriction of scope.
        """
        query = self.base_query

        if where is not None:
            query = query.where(where)

        if order is not None:
            query = query.order_by(order)

        if limit is not None:
            query = query.limit(limit)

        if offset is not None:
            query = query.offset(offset)

        result = yield self.engine.execute(query)
        rows = yield result.fetchall()
        ret = []
        for row in rows:
            ret.append(self._rowToDict(row))
        defer.returnValue(ret)


    @defer.inlineCallbacks
    def count(self, where=None):
        """
        Count a set of records.
        """
        query = self.base_query

        if where is not None:
            query = query.where(where)

        result = yield self.engine.execute(query.count())
        rows = yield result.fetchone()
        defer.returnValue(rows[0])


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


    @property
    def select_columns(self):
        if self._select_columns is None:
            self._select_columns, self._base_query = self._generateBaseQueryAndColumns()
        return self._select_columns


    @property
    def base_query(self):
        if self._base_query is None:
            self._select_columns, self._base_query = self._generateBaseQueryAndColumns()
        return self._base_query

    def _generateBaseQueryAndColumns(self):
        columns = [(None,x) for x in self.policy.readable_columns]
        join = None
        if self.references:
            join = self.policy.table
            for (attr_name, policy, join_on) in self.references:
                join = join.outerjoin(policy.table, join_on)
                columns.extend([(attr_name, x) for x in policy.readable_columns])
        
        base = select([x[1] for x in columns])
        if join is not None:
            base = base.select_from(join)
        base = self._applyConstraints(base)
        return columns, base


    def _applyConstraints(self, query):
        if self._fixed:
            where = None
            for k, v in self._fixed.items():
                col = getattr(self.policy.table.c, k)
                comp = col == v
                if where is not None:
                    where = and_(where, comp)
                else:
                    where = comp
            query = query.where(where)
        return query


    @defer.inlineCallbacks
    def _getOne(self, pk):
        # base query
        query = self.base_query
        
        # pk
        table = self.policy.table
        where = [x == y for (x,y) in zip(table.primary_key.columns, pk)]
        query = query.where(*where)
        
        result = yield self.engine.execute(query)
        row = yield result.fetchone()
        data = self._rowToDict(row)
        defer.returnValue(data)


    def _tableName(self, table):
        return self.table_map.get(table, table.name)


    def _rowToDict(self, row):
        d = {}
        if self.table_attr:
            d[self.table_attr] = self._tableName(self.policy.table)
        # XXX the null-reference checking seems less than optimal (lots of
        # looping and branching.  Maybe there's a way to have the response
        # tell us clearly whether the record is null or not)
        has_value = {}
        for ((ref_name,col), v) in zip(self.select_columns, row):
            if ref_name is None:
                # base object attribute
                d[col.name] = v
            else:
                if ref_name not in has_value:
                    has_value[ref_name] = False
                # referenced object attribute
                if ref_name not in d:
                    d[ref_name] = {}
                    if self.table_attr:
                        d[ref_name][self.table_attr] = self._tableName(col.table)
                d[ref_name][col.name] = v
                if v is not None:
                    has_value[ref_name] = True

        # set Nulls
        for ref_name, ref_has_value in has_value.items():
            if not ref_has_value:
                d[ref_name] = None

        return d


class Paginator(object):
    """
    I provide pagination for a L{Crud}.
    """

    def __init__(self, crud, page_size=10, order=None):
        self.crud = crud
        self.page_size = page_size
        self.order = order


    def page(self, number, where=None):
        """
        Return a page of results.

        @param number: Page number.
        @param where: filter results by this where.
        """
        limit = self.page_size
        offset = number * limit
        return self.crud.fetch(where=where, limit=limit, offset=offset,
                               order=self.order)


    @defer.inlineCallbacks
    def pageCount(self, where=None):
        """
        Return the total number of pages in the set.
        """
        count = yield self.crud.count(where=where)
        pages = count / self.page_size
        if self.page_size % count:
            pages += 1
        defer.returnValue(pages)


