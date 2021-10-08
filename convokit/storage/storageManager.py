from .dbMappings import DBCollectionMapping, DBDocumentMapping
from .memMappings import MemCollectionMapping, MemDocumentMapping
from convokit.util import warn
from .convoKitIndex import ConvoKitIndex

from pymongo import MongoClient
from pymongo.database import Database


class StorageManager:
    def __init__(self, storage_type: str = 'db', corpus_id='default_corpus'):
        """
        Object to manage data storage. 

        :param storage_type: either 'mem' or 'db'
        :param corpus_id: id of the corpus this StorageManager is connected to.
        
        :ivar storage_type: either 'mem' or 'db'
        :ivar index: ConvoKitIndex to track types of metadata stored in this StorageManager
        :ivar corpus_id: id of the corpus this StorageManager is connected to.
        :ivar connection: ????
        :ivar CollectionMapping: class constructor to use to store collections of items.
            Either a DBCollectionMapping or MemCollectionMapping
        :ivar ItemMapping: class constructor to use to store data for items in the collections.
            Either a DBDocumentMapping or MemDocumentMapping
        """
        if corpus_id is None:
            corpus_id = 'default_corpus'
        self.storage_type = storage_type
        self.index = ConvoKitIndex(self)
        self.corpus_id = corpus_id
        if storage_type == 'db':
            self.client = MongoClient()
            if not isinstance(corpus_id, str):
                raise TypeError(f'{corpus_id}: {type(corpus_id)}')
            self.db = self.client['convokit']
            self.CollectionMapping = DBCollectionMapping.with_storage(self)
            self.ItemMapping = DBDocumentMapping
        elif storage_type == 'mem':
            self.db = None
            self.connection = {}
            self.CollectionMapping = MemCollectionMapping.with_storage(self)
            self.ItemMapping = MemDocumentMapping
        else:
            raise ValueError(
                f'Expected storage type to be "mem" or "db"; got {storage_type} instead'
            )

    def __del__(self):
        if self.storage_type == 'db':
            self.client.close()

    def purge_all_collections(self):
        """
        If this is a db storageManager, remove all data in the database.
        """
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

    @staticmethod
    def purge_db():
        """
        Remove all data in the default database. 
        """
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
        """
        Setup instance variables self._utterances, self._conversations, self._speakers & self._metas.
        Should be called as storageManager.setup_collections(Utterance, Conversation, Speaker, ConvoKitMeta)
        after initilizing the storageManager:StorageManager.
        Utterance, Conversation, Speaker, ConvoKitMeta cannot be imported in the convokit.storage module due 
        to circular imports, so a call to setup_collections is required to configure a StorageManager with 
        the correct types. 
        """
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
                and self.corpus_id == other.corpus_id

    def __repr__(self):
        return f'StorageManager(storage_type: {self.storage_type}, corpus_id: {self.corpus_id}'