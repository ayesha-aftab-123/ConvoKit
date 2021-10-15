from .dbMappings import DBCollectionMapping, DBDocumentMapping
from .memMappings import MemCollectionMapping, MemDocumentMapping
from convokit.util import warn
from .convoKitIndex import ConvoKitIndex

from pymongo import MongoClient
from pymongo.database import Database
from random import randrange
import os
import yaml


class StorageManager:
    def __init__(self,
                 storage_type,
                 db_host=None,
                 data_dir=None,
                 corpus_id=None,
                 unique_id=False):
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
        if not storage_type in ['mem', 'db', None]:
            raise ValueError(
                f'storage_type must be "mem", "db" or None; got "{storage_type} instead"'
            )
        # Use defaults from config file or specified values if provided
        with open(os.path.expanduser('~/.convokit/config.yml'), 'r') as f:
            config = yaml.load(f.read())
        if storage_type is None:
            storage_type = config['default_storage_mode']
        if db_host is None:
            db_host = config['db_host']
        if data_dir is None:
            data_dir = os.path.expanduser(config['data_dir'])

        self.raw_corpus_id = corpus_id
        self.storage_type = storage_type
        self.index = ConvoKitIndex(self)
        self.data_dir = data_dir
        if storage_type == 'db':
            self.client = MongoClient(db_host)
            self.db = self.client['convokit']
            if corpus_id is None:
                corpus_id = safe_corpus_id()
                print(
                    f'No filename or corpus name specified for DB storage; using name {corpus_id}'
                )
            if unique_id:
                collections = self.db.list_collection_names()
                while f'{corpus_id}_utterances' in collections:
                    corpus_id = f'{corpus_id}.1'
            self.corpus_id = corpus_id
            self.CollectionMapping = DBCollectionMapping.with_storage(self)
            self.ItemMapping = DBDocumentMapping
        elif storage_type == 'mem':
            self.connection = {}
            # if unique_id:
            #     files = os.listdir(data_dir)
            #     while f'{corpus_id}' in files:
            #         corpus_id = f'{corpus_id}.1'
            self.corpus_id = corpus_id
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


def safe_corpus_id():
    return str(randrange(2**15, 2**20))