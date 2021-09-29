from convokit import storage
from .dbMappings import DBCollectionMapping, DBDocumentMapping
from .memMappings import NamedDict, NestedDict
from convokit.util import warn
from .convoKitIndex import ConvoKitIndex

from pymongo import MongoClient
from pymongo.database import Database


class StorageManager:
    def __init__(self, storage_type: str = 'db', corpus_name='default_corpus'):
        if corpus_name is None:
            corpus_name = 'default_corpus'
        self.storage_type = storage_type
        self.index = ConvoKitIndex(self)
        self.corpus_name = corpus_name
        if storage_type == 'db':
            self.client = MongoClient()
            if not isinstance(corpus_name, str):
                raise TypeError(f'{corpus_name}: {type(corpus_name)}')
            self.db = self.client['convokit']
            self.connection = None
            self.CollectionMapping = DBCollectionMapping.with_storage(self)
            self.ItemMapping = DBDocumentMapping
        elif storage_type == 'mem':
            self.db = None
            self.connection = {}
            self.CollectionMapping = NamedDict.with_connection(self.connection)
            self.ItemMapping = NestedDict
        else:
            raise ValueError(
                f'Expected storage type to be "mem" or "db"; got {storage_type} instead'
            )

    def __del__(self):
        if self.storage_type == 'db':
            self.client.close()

    def purge_all_collections(self):
        for attr, value in self.__dict__.items():
            if isinstance(value, DBCollectionMapping):
                warn(
                    f'Dropping collection {value.collection.name} (storage.{attr})'
                )
                value.collection.drop()
            if isinstance(value, Database):
                warn(f'Dropping db {value.name} (storage.{attr})')
                for collection_name in value.collection_names():
                    warn(
                        f'\tDropping collection {collection_name} (collection in storage.{attr}))'
                    )
                    value.drop_collection(collection_name)
            else:
                pass
                # print(
                #     f'(purge) found {attr} -> <type: {type(value)}>; not dropping'
                # )
    @staticmethod
    def purge_db():
        warn('purging the DB')
        client = MongoClient()
        db = client['convokit']
        for collection_name in db.collection_names():
            warn(
                f'\tDropping collection {collection_name} (collection in {db.name}))'
            )
            db.drop_collection(collection_name)
        client.close()

    def setup_collections(self, utterance, conversation, speaker,
                          convokitmeta):
        self._utterances = self.CollectionMapping('utterances',
                                                  item_type=utterance)
        self._conversations = self.CollectionMapping('conversations',
                                                     item_type=conversation)
        self._speakers = self.CollectionMapping('speakers', item_type=speaker)
        self._metas = self.CollectionMapping('metas', item_type=convokitmeta)

    def __eq__(self, other):
        if not isinstance(other, StorageManager):
            return False
        else:
            return self.storage_type == other.storage_type \
                and self.corpus_name == other.corpus_name

    def __repr__(self):
        return f'StorageManager(storage_type: {self.storage_type}, corpus_name: {self.corpus_name}'