

class Error(Exception): pass


class MissingRequiredFields(Error): pass
class NotEditable(Error): pass