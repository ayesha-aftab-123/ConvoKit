from typing import MutableMapping, Callable


class MemCollectionMapping(MutableMapping):
    def __init__(self, name, item_type=None):
        super().__init__()
        self.name = name
        self.item_type = item_type
        self.data = {}

    @staticmethod
    def with_storage(storage) -> Callable[[str, type], MutableMapping]:
        def ret(collection_name, item_type=None):
            if collection_name not in storage.connection:
                storage.connection[collection_name] = MemCollectionMapping(
                    collection_name, item_type=item_type
                )

            return storage.connection[collection_name]

        return ret

    def __setitem__(self, key, value):
        if key is None:
            key = "None"
        self.data[key] = value

    def __getitem__(self, key):
        if key is None:
            key = "None"
        return self.data[key]

    def __delitem__(self, key):
        self.data.__delitem__(key)

    def __iter__(self):
        return self.data.__iter__()

    def __len__(self):
        return self.data.__len__()

    def __contains__(self, x):
        return self.data.__contains__(x)

    def drop_self(self):
        del self.data

    def filter_by(self, condition):
        self.data = {key: value for key, value in self.data.items() if condition(value)}


class MemDocumentMapping(MutableMapping):
    def __init__(self, collection_mapping, id):
        super().__init__()
        if id is None:
            id = "None"
        self.id = id
        self.data = {}
        if id in collection_mapping:
            self.data = collection_mapping[id].fields.dict()
        self.collection_mapping = collection_mapping

    def dict(self, with_id=True):
        d = self.data.copy()
        if with_id:
            d["id"] = self.id
        elif "id" in d:
            del d["id"]
        return d

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
        return cls(collection_mapping=collection_mapping, id=self.id, data=self.dict())
