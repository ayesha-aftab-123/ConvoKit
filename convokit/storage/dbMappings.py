from typing import MutableMapping
from convokit.util import warn
from collections.abc import Callable
from bson import Binary


class DBCollectionMapping(MutableMapping):
    def __init__(self, db, collection_name, storage, item_type=None):
        super().__init__()
        self.db = db
        self.collection = db[collection_name]
        self.storage = storage
        self.type = item_type
        self.name = collection_name
        # print(item_type)
        # print(type(item_type))

    def with_storage(storage) -> Callable[[str, type], MutableMapping]:
        return lambda collection_name, item_type=None: DBCollectionMapping(
            storage.db,
            f'{storage.corpus_name}_{collection_name}',
            storage,
            item_type=item_type)

    def __getitem__(self, key):  # -> ??????:
        # print(f'({self.name}) Getting {key}')
        if self.type is not None:
            # print('\tbuilding from type {self.type}')
            return self.type.from_dbdoc(DBDocumentMapping(
                self, key), self.storage)  # if key is not None else None
        else:
            # print('\treturning dict directly')
            return DBDocumentMapping(self, key).dict()
        # return self.type().from_dict(DBDocumentMapping(self, key).dict())

    def __setitem__(self, key: str, value):
        # if self.collection.name == 'metas':
        # print(f'({self.collection.name}) Inserting {key} -> {value}')
        if self.type is None and isinstance(value, dict):
            DBDocumentMapping(self, key, data=value)
        elif isinstance(value, self.type):
            # print('\tsame collection: ', self.db.name, '.',
            #       self.collection.name)
            if value.fields.collection_mapping.name == self.name:
                data = {'_id': key}
                res = self.collection.find_one(data)
                # print(f'\talready has data {res}')
                if res is None:
                    self.collection.update(data, data, upsert=True)

            else:
                # print(
                #     f'transfering {value} to collection {self.collection.name}'
                # )
                if hasattr(value, 'meta'):
                    value.meta.storage = self.storage
                    value.meta.fields.transfer_to_dbcoll(
                        self.storage._metas, DBDocumentMapping)

                value.storage = self.storage
                value.fields.transfer_to_dbcoll(self, DBDocumentMapping)

                # Todo: Make transfering the meta more robust.

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

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, DBCollectionMapping): return False
        else:
            return (self.db.name == o.db.name
                    and self.collection.name == o.collection.name
                    and self.type == o.type)

    def __contains__(self, key):
        return self.collection.find_one({'_id': key}) is not None

    def drop_self(self):
        warn(f'purging the DBCollectionMapping {self.name}')
        self.db.drop_collection(self.name)


class DBDocumentMapping(MutableMapping):
    def __init__(self, collection_mapping, id, data=None):
        super().__init__()
        if id is None:
            id = 'None'
        if not type(id) == str:
            raise TypeError(f'Found id type {type(id)}; should be str')
        self.collection_mapping = collection_mapping
        self.id = id
        if data is not None:
            if type(data) is not dict:
                raise TypeError('data must be a dict or None;'
                                f' got type {type(data)} instead.')
            for key in data:
                if isinstance(data[key], bytearray):
                    data[key] = Binary(data[key])

            self.collection_mapping.collection.update({'_id': self.id},
                                                      data,
                                                      upsert=True)

    def dict(self):
        data = self.collection_mapping.collection.find_one({'_id': self.id})
        if data is None:
            data = {}
        if '_id' in data:
            del data['_id']
        return data

    def __getitem__(self, key):
        # print(f'({self.collection_mapping.collection.name}[{self.id}])'
        #       f' get {key}')
        value = self.dict().get(key, None)
        if isinstance(value, Binary):
            value = bytearray(value)
        return value

    def __setitem__(self, key, value):
        # if self.collection_mapping.collection.name == 'metas':
        #     print(f'({self.collection_mapping.collection.name}[{self.id}])'
        #           f' put {key}, {value}')
        if isinstance(value, bytearray):
            value = Binary(value)
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

    def __contains__(self, key):
        return key in self.dict()

    def transfer_to_dbcoll(self, collection_mapping, cls):
        # print(
        #     f'transfer {self.collection_mapping.type}.{self.id} from '
        #     f'{self.collection_mapping.db.name}.{self.collection_mapping.collection.name} to '
        #     f'{collection_mapping.db.name}.{collection_mapping.collection.name}'
        #     f'\n\twith data {self.dict()}')
        return cls(collection_mapping=collection_mapping,
                   id=self.id,
                   data=self.dict())
