

class Error(Exception): pass

class MissingRequiredFields(Error): pass
class NotEditable(Error): pass
class TooMany(Error): pass