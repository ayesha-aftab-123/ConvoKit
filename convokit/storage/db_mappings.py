from typing import MutableMapping, Callable

from bson import Binary

from convokit.util import warn


class DBCollectionMapping(MutableMapping):
    def __init__(self, db, collection_name, storage, item_type=None):
        super().__init__()
        self.db = db
        self.collection = db[collection_name]
        self.storage = storage
        self.type = item_type
        self.name = collection_name

    @staticmethod
    def with_storage(storage) -> Callable[[str, type], MutableMapping]:
        return lambda collection_name, item_type=None: DBCollectionMapping(
            storage.db,
            f"{storage.full_name}_{collection_name}",
            storage,
            item_type=item_type,
        )

    def __getitem__(self, key):  # -> self.type if self.type is not None else {}
        if not self.__contains__(key):
            raise KeyError
        if self.type is not None:
            return self.type.from_db_document(DBDocumentMapping(self, key))
        else:
            return DBDocumentMapping(self, key).dict(with_id=False)

    def __setitem__(self, key: str, value):
        if self.type is None and isinstance(value, dict):
            DBDocumentMapping(self, key, data=value)
        elif isinstance(value, self.type):
            if (
                    isinstance(value.fields, DBDocumentMapping)
                    and value.fields.collection_mapping.name == self.name
            ):
                data = {"_id": key}
                res = self.collection.find_one(data)
                if res is None:
                    self.collection.update_one(data, {"$set": value.fields.dict()}, upsert=True)
            else:
                if hasattr(value, "meta"):
                    value.meta.fields.transfer_to_dbcoll(self.storage.metas, DBDocumentMapping)

                value.fields.transfer_to_dbcoll(self, DBDocumentMapping)

        else:
            raise TypeError(
                f"expected value to be of type {dict if self.type is None else self.type}; got {type(value)} instead"
            )

    def __delitem__(self, key):
        self.collection.delete_one({"_id": key})

    def __iter__(self):
        items = self.collection.find()
        for item in items:
            yield item["_id"]

    def __len__(self):
        return len(list(self.collection.find()))

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, DBCollectionMapping):
            return False
        else:
            return (
                    self.db.name == o.db.name
                    and self.collection.name == o.collection.name
                    and self.type == o.type
            )

    def __contains__(self, key):
        return self.collection.find_one({"_id": key}) is not None

    def drop_self(self):
        warn(f"purging the DBCollectionMapping {self.name}")
        self.db.drop_collection(self.name)

    def filter_by(self, condition):
        keep = []
        for key, value in self.items():
            if condition(value):
                keep.append(key)

        self.collection.delete_many('{"_id":{$nin:' + str(keep) + "}}")


class DBDocumentMapping(MutableMapping):
    def __init__(self, collection_mapping, id, data=None):
        super().__init__()
        if id is None:
            id = "None"
        if not type(id) == str:
            raise TypeError(f"Found id type {type(id)}; should be str")
        self.collection_mapping = collection_mapping
        self.id = id
        if data is not None:
            if type(data) is not dict:
                raise TypeError("data must be a dict or None;" f" got type {type(data)} instead.")
            for key in data:
                if isinstance(data[key], bytearray):
                    data[key] = Binary(data[key])

            self.collection_mapping.collection.update_one(
                {"_id": self.id}, {"$set": data}, upsert=True
            )

    def dict(self, with_id=True):
        data = self.collection_mapping.collection.find_one({"_id": self.id})
        if data is None:
            data = {}
        else:
            del data["_id"]
            if with_id:
                data["id"] = self.id
            elif "id" in data:
                del data["id"]

        return data

    def __getitem__(self, key):
        value = self.dict().get(key, None)
        if isinstance(value, Binary):
            value = bytearray(value)
        return value

    def __setitem__(self, key, value):
        if isinstance(value, bytearray):
            value = Binary(value)
        data = self.dict()
        if data is None:
            data = {"_id": self.id}
        data[key] = value
        self.collection_mapping.collection.update_one({"_id": self.id}, {"$set": data}, upsert=True)

    def __delitem__(self, key):
        self.collection_mapping.collection.update_one(
            {"_id": self.id},
            {
                "$unset": {
                    key: "",
                }
            },
        )

    def __iter__(self):
        return self.dict().__iter__()

    def __len__(self):
        return len(self.dict(with_id=False))

    def __contains__(self, key):
        return key in self.dict()

    def transfer_to_dbcoll(self, collection_mapping, cls):
        return cls(collection_mapping=collection_mapping, id=self.id, data=self.dict())
