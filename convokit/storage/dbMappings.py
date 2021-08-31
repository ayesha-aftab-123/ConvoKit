from typing import MutableMapping
from collections.abc import Callable


class DBCollectionMapping(MutableMapping):
    def __init__(self, db, collection_name, storage, item_type=None):
        super().__init__()
        self.collection = db[collection_name]
        self.db = db
        self.storage = storage
        self.type = item_type
        # print(item_type)
        # print(type(item_type))

    def with_storage(storage) -> Callable[[str, type], MutableMapping]:
        return lambda collection_name, item_type=None: DBCollectionMapping(
            storage.db, collection_name, storage, item_type=item_type)

    # ToDo: Make getitem and setitem work correctly
    # |-> Need to return something of a better type than DBDocumentMapping
    def __getitem__(self, key):  # -> ??????:
        # print(f'({self.collection.name}) Getting {key}')
        if self.type is not None:
            # print('\tbuilding from type {self.type}')
            return self.type.from_dbdoc(DBDocumentMapping(self, key),
                                        self.storage)
        else:
            # print('\treturning dict directly')
            return DBDocumentMapping(self, key).dict()
        # return self.type().from_dict(DBDocumentMapping(self, key).dict())

    def __setitem__(self, key: str, value):
        # print(f'({self.collection.name}) Inserting {key} -> {value}')
        if self.type is None and isinstance(value, dict):
            DBDocumentMapping(self, key, data=value)
        elif isinstance(value, self.type):
            assert self.collection.find_one({'_id': key}) is not None
            # DBDocumentMapping(self, key, data=value.to_dict())
        else:
            raise TypeError(
                f'expected value to be of type {dict if self.type is None else self.type}; got {type(value)} instead'
            )

    def __delitem__(self, key):
        self.collection.delete_one({'_id': key})

    def __iter__(self):
        items = self.collection.find()
        for item in items:
            yield item['_id']

    def __len__(self):
        return len(list(self.collection.find()))


class DBDocumentMapping(MutableMapping):
    def __init__(self, collection_mapping, id, data=None):
        super().__init__()
        if not type(id) == str:
            raise TypeError(f'Found id type {type(id)}; should be str')
        self.collection_mapping = collection_mapping
        self.id = id
        if data is not None:
            if type(data) is not dict:
                raise TypeError('data must be a dict or None;'
                                f' got type {type(data)} instead.')
            self.collection_mapping.collection.update({'_id': self.id},
                                                      data,
                                                      upsert=True)

    def dict(self):
        return self.collection_mapping.collection.find_one({'_id': self.id})

    def __getitem__(self, key):
        return self.dict()[key]

    def __setitem__(self, key, value):
        data = self.dict()
        if data is None:
            data = {'_id': self.id}
        data[key] = value
        self.collection_mapping.collection.update({'_id': self.id},
                                                  data,
                                                  upsert=True)

    def __delitem__(self, key):
        self.collection_mapping.collection.update({'_id': self.id},
                                                  {'$unset': {
                                                      key: "",
                                                  }})

    def __iter__(self):
        return self.dict().__iter__()

    def __len__(self):
        return len(self.dict())
