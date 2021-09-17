from convokit import storage
from .dbMappings import DBCollectionMapping, DBDocumentMapping
from .memMappings import NamedDict, NestedDict
from convokit.util import warn
from .convoKitIndex import ConvoKitIndex

from pymongo import MongoClient
from pymongo.database import Database

client = MongoClient()


class StorageManager:
    def __init__(self, storage_type: str = 'db', corpus_name='default_corpus'):
        if corpus_name is None:
            corpus_name = 'default_corpus'
        self.storage_type = storage_type
        self.index = ConvoKitIndex(self)
        if storage_type == 'db':
            if not isinstance(corpus_name, str):
                raise TypeError(f'{corpus_name}: {type(corpus_name)}')
            self.db = client[corpus_name]
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

    def setup_collections(self, utterance, conversation, speaker,
                          convokitmeta):
        self._utterances = self.CollectionMapping('utterances',
                                                  item_type=utterance)
        self._conversations = self.CollectionMapping('conversations',
                                                     item_type=conversation)
        self._speakers = self.CollectionMapping('speakers', item_type=speaker)
        self._metas = self.CollectionMapping('metas', item_type=convokitmeta)


defaultCorpusStorageManager = StorageManager()
memUntrackedStorageManager = StorageManager(storage_type='mem',
                                            corpus_name='not_a_corpus')
