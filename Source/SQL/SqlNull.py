class SqlNull:

    def __eq__(self, item : object) -> bool:
        return True if isinstance(item, SqlNull) else False

    def __ne__(self, item : object) -> bool:
        return False if isinstance(item, SqlNull) else True

    def __lt__ (self, item : object) -> bool:
        return True if not isinstance(item, SqlNull) else False

    def __le__ (self, item : object) -> bool:
        return True if isinstance(item, SqlNull) else False

    def __gt__ (self, item : object) -> bool:
        return False

    def __ge__ (self, item : object) -> bool:
        return True if isinstance(item, SqlNull) else False