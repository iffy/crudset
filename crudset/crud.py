
from crudset.error import MissingRequiredFields



class _CrudMaker(object):
    """
    I make it easy to create/read/update/delete SQL stuff.
    XXX Lucy, you have some 'splain to do!
    """

    def __init__(self, table, create_requires=None):
        """
        @param table: An SQLAlchemy Table.
        """
        self.table = table
        if create_requires:
            self.create_requires = frozenset(create_requires)
        else:
            self.create_requires = frozenset()


    def create(self, attrs):
        """
        Get an insert row.
        """
        missing = self.create_requires - set(attrs)
        if missing:
            raise MissingRequiredFields(', '.join(missing))
        return self.table.insert(**attrs)