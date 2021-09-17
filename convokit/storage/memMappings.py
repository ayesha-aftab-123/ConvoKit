from typing import MutableMapping, Callable


class NamedDict(MutableMapping):
    def __init__(self, name, item_type=None):
        super().__init__()
        self.name = name
        self.item_type = item_type
        self.data = {}

    def with_connection(connection) -> Callable[[str, type], MutableMapping]:
        def ret(collection_name, item_type=None):
            if collection_name not in connection:
                connection[collection_name] = NamedDict(collection_name,
                                                        item_type=item_type)

            return connection[collection_name]

        return ret

    def __setitem__(self, key, value):
        self.data[key] = value

    def __getitem__(self, key):
        return self.data[key]

    def __delitem__(self, key):
        self.data.__delitem__(key)

    def __iter__(self):
        return self.data.__iter__()

    def __len__(self):
        return self.data.__len__()

    def __contains__(self, x):
        return self.data.__contains__(x)


class NestedDict(MutableMapping):
    def __init__(self, collection_mapping, id):
        super().__init__()
        self.id = id
        self.data = {}
        if id in collection_mapping:
            # if '_' in id:
            #     print(
            #         f'found id {id} in parent; initilizing self.__dict__ to ')
            #     print(parent[id].__dict__)
            self.data = collection_mapping[id].fields.dict()
        self.collection_mapping = collection_mapping
        self.data

    def dict(self):
        return self.data

    def __setitem__(self, key, value):
        self.data[key] = value

    def __getitem__(self, key):
        return self.data[key]

    def __delitem__(self, key):
        self.data.__delitem__(key)

    def __iter__(self):
        return self.data.__iter__()

    def __len__(self):
        return self.data.__len__()

    def __contains__(self, x):
        return self.data.__contains__(x)

    def transfer_to_dbcoll(self, collection_mapping, cls):
        return cls(collection_mapping=collection_mapping,
                   id=self.id,
                   data=self.dict())
