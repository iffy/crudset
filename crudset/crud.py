from twisted.internet import defer
from sqlalchemy.sql import select, and_

from crudset.error import TooMany, MissingRequiredFields



class Ref(object):
    """
    A reference to another object or list of objects for use within a L{Readset}.
    """

    def __init__(self, readset, join, multiple=False):
        """
        @param multiple: If C{True} then this is a reference to multiple things
            rather than just one thing (the default).
        """
        self.readset = readset
        self.join = join
        self.multiple = multiple


    def __repr__(self):
        return 'Ref(%r, %r, multiple=%r)' % (
            self.readset, self.join, self.multiple)



class SanitizationContext(object):
    """
    A context for sanitizers to get more information about how to sanitize.
    """

    def __init__(self, engine, action, query):
        self.engine = engine
        self.action = action
        self.query = query


    def __repr__(self):
        return 'SanitizationContext(%r, %r, %r)' % (
            self.engine, self.action, self.query)


class SaniChain(object):
    """
    I chain sanitizers together.
    """

    def __init__(self, sanitizers):
        self.sanitizers = sanitizers
        tables = set([x.table for x in sanitizers])
        if len(tables) != 1:
            raise TypeError("Chained sanitizers must have the same table: %r" % (
                            tables,))
        self.table = tables.pop()


    def __repr__(self):
        return 'SaniChain(%r, table=%r)' % (self.sanitizers, self.table)


    @defer.inlineCallbacks
    def sanitize(self, context, data):
        output = data
        for sanitizer in self.sanitizers:
            output = yield sanitizer.sanitize(context, output)
        defer.returnValue(output)


class Readset(object):
    """
    A description of the fields and references that are returned from
    data-returning functions.
    """

    def __init__(self, table, readable=None, references=None):
        self.table = table

        if readable is None:
            self.readable_columns = list(table.columns)
        else:
            self.readable_columns = [getattr(table.c, x) for x in readable]
        self.readable = frozenset([x.name for x in self.readable_columns])

        self.references = references or {}


    def __repr__(self):
        return 'Readset(%r, %r, %r)' % (
            self.table, list(self.readable), self.references)


class Writeset(object):
    """
    A description of the fields that are writeable.
    """

    def __init__(self, table, writeable=None):
        """

        """
        self.table = table
        self.writeable = set()
        for field in (writeable or []):
            if type(field) in (str, unicode):
                self.writeable.add(field)
            else:
                self.writeable.add(field.name)


    def __repr__(self):
        return 'Writeset(%r, %r)' % (self.table, self.writeable)


    def sanitize(self, context, data):
        ret = {}
        union = set(data) & self.writeable
        for key in union:
            ret[key] = data[key]
        return defer.succeed(ret)



class Sanitizer(object):


    def __init__(self, table, required=None):
        """
        @param table: An SQLAlchemy Table definition.

        @param required: An optional list of fields that are required
            when creating a record.  If C{None} then no fields are required.
            For updates, the fields may be absent, but can not be C{None}.
        """
        self.table = table
        self._sanitizers = []
        self._sanitized_fields = []
        self._writeset = Writeset(table, table.columns)
        self._post_sanitizers = [self._assertRequired]
        self.required = set(required or [])


    def __repr__(self):
        return 'Sanitizer(%r, %r)' % (self.table, self.required)


    def __get__(self, instance, cls):
        if instance is None:
            return self
        return self.bind(instance)


    def bind(self, instance):
        return _BoundSanitizer(self, instance)


    def sanitizeMethods(self):
        """
        Return a list of methods to be used to sanitize data.
        """
        return self._sanitizers + self._post_sanitizers


    def getSanitizedFields(self):
        """
        List fields being sanitized.  This could be helpful to statically(ish)
        check that all fields are being sanitized.
        """
        return self._sanitized_fields


    def sanitizeData(self, func):
        """
        Add a sanitization function for the whole blob of data.
        """
        self._sanitizers.append(func)
        return func


    def sanitizeField(self, field):
        """
        Add a sanitization function for a specific named field.  This is a
        more specific version of the general L{sanitizeData} method.
        """
        def deco(func):
            self._sanitizers.append(self._fieldSanitizer(func, field))
            self._sanitized_fields.append(field)
            return func
        return deco


    @defer.inlineCallbacks
    def sanitize(self, context, data, instance=None):
        result = data
        for func in self.sanitizeMethods():
            result = yield func(instance, context, result)
        stripped = yield self._writeset.sanitize(context, result)
        defer.returnValue(stripped)


    def _fieldSanitizer(self, func, field):
        @defer.inlineCallbacks
        def _sanitizer(instance, context, data):
            if field not in data:
                defer.returnValue(data)
            else:
                output = yield func(instance, context, data, field)
                data[field] = output
                defer.returnValue(data)
        return _sanitizer


    def _assertRequired(self, instance, context, data):
        """
        Raise an exception if there are required fields missing.
        """
        # check for presence
        if context.action == 'create':
            missing = set(self.required) - set(data)
            if missing:
                raise MissingRequiredFields('Missing required fields: %s' % (
                    ', '.join(missing)))

        # check for nullness
        for required_field in self.required:
            if required_field in data and data[required_field] is None:
                raise MissingRequiredFields('Required field is null: %s' % (
                    required_field,))
        return data




class _BoundSanitizer(object):


    def __init__(self, sanitizer, instance):
        self.sanitizer = sanitizer
        self.instance = instance


    def __repr__(self):
        return '_BoundSanitizer(%r, %r)' % (self.sanitizer, self.instance)


    def sanitize(self, context, data):
        return self.sanitizer.sanitize(context, data, self.instance)        


    @property
    def table(self):
        return self.sanitizer.table



class Crud(object):
    """
    This turns a L{Readset} and a L{Sanitizer} into a CRUD.
    See my L{create}, L{fetch}, L{count}, L{update} and L{delete} methods.

    Also, you can use L{fix} to make new L{Crud} instances with certain
    attributes fixed (unchangeable by the user).
    """

    def __init__(self, readset, sanitizer=None, table_attr=None, table_map=None):
        """
        @param readset: A L{Readset} instance.
        @param sanitizer: An object with a C{sanitize(context, data)} method
            such as a L{Writeset}, L{Sanitizer} or L{SaniChain}.

            If a list or tuple is provided, it will automatically be wrapped
            in a L{SaniChain}.

        @param table_attr: If set, then the data dictionaries returned by my
            methods will contain an item with C{table_attr} key and SQL table
            name as the value.  Man that's confusing...

        @param table_map: If C{table_attr} is set then this dictionary will
            map table names to something else.
        """
        self.readset = readset
        
        if sanitizer is None:
            sanitizer = Sanitizer(readset.table)
        if type(sanitizer) in (list, tuple):
            sanitizer = SaniChain(sanitizer)
        self.sanitizer = sanitizer

        if self.sanitizer.table != self.readset.table:
            raise TypeError("Readset and sanitizer/writeset are for different"
                " tables: %r %r" % (self.readset, self.sanitizer))

        self.table_attr = table_attr
        self.table_map = table_map or {}
        self._fixed = {}
        self._select_columns = []
        self._base_query = None


    def __repr__(self):
        return 'Crud(%r, %r, table_attr=%r, table_map=%r)' % (
            self.readset, self.sanitizer, self.table_attr, self.table_map)


    def fix(self, attrs):
        """
        Fix some attributes to a particular value.

        @param attrs: dict of attributes to fix.

        @return: A new L{Crud}.
        """
        crud = Crud(self.readset, self.sanitizer, self.table_attr,
                    self.table_map)
        crud._fixed = self._fixed.copy()
        crud._fixed.update(attrs)
        return crud


    @defer.inlineCallbacks
    def create(self, engine, attrs):
        """
        Create a single record.
        """
        # fixed attributes
        attrs.update(self._fixed)

        # sanitize
        context = SanitizationContext(engine, 'create', None)
        sanitized = yield self.sanitizer.sanitize(context, attrs)

        # do it
        table = self.sanitizer.table
        result = yield engine.execute(table.insert().values(**sanitized))
        pk = result.inserted_primary_key
        obj = yield self._getOne(engine, pk)
        defer.returnValue(obj)


    @defer.inlineCallbacks
    def update(self, engine, attrs, where=None):
        """
        Update a set of records.
        """
        up = self._applyConstraints(self.sanitizer.table.update())

        # you can't update fixed attributes
        for attr in self._fixed:
            attrs.pop(attr, None)

        query = self._applyConstraints(self.sanitizer.table.select())
        if where is not None:
            up = up.where(where)
            query = query.where(where)

        context = SanitizationContext(engine, 'update', query)
        sanitized = yield self.sanitizer.sanitize(context, attrs)

        if sanitized:
            up = up.values(**sanitized)
            yield engine.execute(up)

        rows = yield self.fetch(engine, where)
        defer.returnValue(rows)


    @defer.inlineCallbacks
    def fetch(self, engine, where=None, order=None, limit=None, offset=None):
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

        result = yield engine.execute(query)
        rows = yield result.fetchall()
        ret = []
        for row in rows:
            ret.append(self._rowToDict(row))
        ret = yield defer.gatherResults(ret)
        defer.returnValue(ret)


    @defer.inlineCallbacks
    def getOne(self, engine, where=None):
        """
        Get one record or fail trying.

        @param where: Where clause.
        """
        rows = yield self.fetch(engine, where, limit=2)
        if len(rows) > 1:
            raise TooMany("Expecting one and found more than that")
        elif not rows:
            defer.returnValue(None)
        defer.returnValue(rows[0])


    @defer.inlineCallbacks
    def count(self, engine, where=None):
        """
        Count a set of records.
        """
        query = self.base_query

        if where is not None:
            query = query.where(where)

        result = yield engine.execute(query.alias().count())
        rows = yield result.fetchone()
        defer.returnValue(rows[0])


    @defer.inlineCallbacks
    def delete(self, engine, where=None):
        """
        Delete a set of records.
        """
        delete = self.sanitizer.table.delete()
        delete = self._applyConstraints(delete)

        if where is not None:
            delete = delete.where(where)

        yield engine.execute(delete)


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
        columns = [(None,x) for x in self.readset.readable_columns]
        join = None
        for ref_name, ref in self.readset.references.items():
            if ref.multiple:
                continue
            if join is None:
                join = self.readset.table
            join = join.outerjoin(ref.readset.table, ref.join)
            columns.extend([(ref_name, x) for x in ref.readset.readable_columns])
        
        base = select([x[1] for x in columns], use_labels=True)
        if join is not None:
            base = base.select_from(join)
        base = self._applyConstraints(base)
        return columns, base


    def _applyConstraints(self, query):
        if self._fixed:
            where = None
            for k, v in self._fixed.items():
                col = getattr(self.readset.table.c, k)
                comp = col == v
                if where is not None:
                    where = and_(where, comp)
                else:
                    where = comp
            query = query.where(where)
        return query


    @defer.inlineCallbacks
    def _getOne(self, engine, pk):
        # base query
        query = self.base_query
        # pk
        table = self.readset.table
        where = [x == y for (x,y) in zip(table.primary_key.columns, pk)]
        query = query.where(*where)
        
        result = yield engine.execute(query)
        row = yield result.fetchone()
        data = yield self._rowToDict(row)
        defer.returnValue(data)


    def _tableName(self, table):
        return self.table_map.get(table, table.name)


    @defer.inlineCallbacks
    def _rowToDict(self, row):
        ret = {}
        if self.table_attr:
            ret[self.table_attr] = self._tableName(self.readset.table)
        # XXX the null-reference checking seems less than optimal (lots of
        # looping and branching.  Maybe there's a way to have the response
        # tell us clearly whether the record is null or not)
        has_value = {}
        for ((ref_name,col), v) in zip(self.select_columns, row):
            if ref_name is None:
                # base object attribute
                ret[col.name] = v
            else:
                if ref_name not in has_value:
                    has_value[ref_name] = False
                # referenced object attribute
                if ref_name not in ret:
                    ret[ref_name] = {}
                    if self.table_attr:
                        ret[ref_name][self.table_attr] = self._tableName(col.table)
                ret[ref_name][col.name] = v
                if v is not None:
                    has_value[ref_name] = True

        # set Nulls
        for ref_name, ref_has_value in has_value.items():
            if not ref_has_value:
                ret[ref_name] = None

        # get multi references
        multi_refs = []
        for (ref_name, ref) in self.readset.references.items():
            if not ref.multiple:
                continue
            join = self.readset.table.outerjoin(
                ref.readset.table, ref.join)
            base = select(ref.readset.readable_columns).select_from(join)
            base = self._applyConstraints(base)
            yield 'foo'

        defer.returnValue(ret)


class Paginator(object):
    """
    I provide pagination for a L{Crud}.
    """

    def __init__(self, crud, page_size=10, order=None):
        self.crud = crud
        self.page_size = page_size
        self.order = order


    def __repr__(self):
        return 'Paginator(%r, %r, %r)' % (
            self.crud, self.page_size, self.order)


    def page(self, engine, number, where=None):
        """
        Return a page of results.

        @param number: Page number.
        @param where: filter results by this where.
        """
        limit = self.page_size
        offset = number * limit
        return self.crud.fetch(engine, where=where, limit=limit, offset=offset,
                               order=self.order)


    @defer.inlineCallbacks
    def pageCount(self, engine, where=None):
        """
        Return the total number of pages in the set.
        """
        count = yield self.crud.count(engine, where=where)
        if count == 0:
            defer.returnValue(0)

        pages = ((count - 1) / self.page_size) + 1
        defer.returnValue(pages)




def crudFromSpec(cls, table_attr=None, table_map=None):
    """
    Create a Crud from a specification class.  See README.md for an example.

    If C{readable} is not given, all fields will be readable.
    If C{writeable} is not given, no fields will be writeable.
    """
    table = cls.table
    readable = getattr(cls, 'readable', None)
    writeable = getattr(cls, 'writeable', None)
    references = getattr(cls, 'references', None)
    sanitizer = getattr(cls, 'sanitizer', None)

    if writeable == 'ALL':
        writeable = [x.name for x in table.columns]

    sanitizers = Writeset(cls.table, writeable)
    if sanitizer:
        sanitizers = [sanitizer, sanitizers]
    return Crud(
        Readset(table, readable, references),
        sanitizers,
        table_attr=table_attr,
        table_map=table_map)



