from typing import MutableMapping, Callable


class NamedDict(dict):
    def __init__(self, name, item_type=None):
        super().__init__()
        self.name = name
        self.item_type = item_type

    def with_connection(connection) -> Callable[[str, type], MutableMapping]:
        def ret(collection_name, item_type=None):
            if collection_name not in connection:
                connection[collection_name] = NamedDict(collection_name,
                                                        item_type=item_type)

            return connection[collection_name]

        return ret


class NestedDict(dict):
    def __init__(self, parent, id):
        super().__init__()
        self.id = id

    def dict(self):
        return self