from typing import List, Collection, Callable, Set, Generator, Tuple, Optional, ValuesView, Union, MutableMapping
from pymongo import MongoClient
from pymongo.database import Database
from random import randrange
import os
from yaml import load, dump, Loader, Dumper

from .dbMappings import DBCollectionMapping, DBDocumentMapping
from .memMappings import MemCollectionMapping, MemDocumentMapping
from convokit.util import warn
from .convoKitIndex import ConvoKitIndex


class StorageManager:
    def __init__(self,
                 storage_type: str,
                 db_host: Optional[str] = None,
                 data_dir: Optional[str] = None,
                 corpus_id: Optional[str] = None,
                 version: Optional[str] = '0',
                 in_place: Optional[bool] = True):
        """
        Object to manage data storage. 

        :param storage_type: either 'mem' or 'db'
        :param db_host: name of the DB host
        :param data_dir: path to the directory containing Mem Corpora. If left 
            unspecified, will use the data_dir specified in ~/.convokit/config.yml
        :param corpus_id: id of the corpus this StorageManager will connect to.
        :param version: the version to load from e.g. '0' or '1.0.1'
        :param in_place: whether (if True) to directly connect to the  specified version,
            or (if False) to increment the version number so that the original copy
            of the data can be left unmodified. 
        
        :ivar storage_type: either 'mem' or 'db'
        :ivar index: ConvoKitIndex to track types of metadata stored in this StorageManager
        :ivar corpus_id: id of the corpus this StorageManager is connected to.
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
        self.config_fullpath = os.path.expanduser('~/.convokit/config.yml')
        config = read_or_create_config(self.config_fullpath)

        tmp_mode = os.environ.get('CONVOKIT_STORAGE_MODE')
        if tmp_mode in ['mem', 'db']:
            config['default_storage_mode'] = tmp_mode

        if storage_type is None:
            storage_type = config['default_storage_mode']
        if db_host is None:
            db_host = config['db_host']
        if data_dir is None:
            data_dir = os.path.expanduser(config['data_dir'])

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
            self.corpus_id = corpus_id
            self.CollectionMapping = DBCollectionMapping.with_storage(self)
            self.ItemMapping = DBDocumentMapping
        elif storage_type == 'mem':
            self.connection = {}
            self.corpus_id = corpus_id
            self.CollectionMapping = MemCollectionMapping.with_storage(self)
            self.ItemMapping = MemDocumentMapping
        else:
            raise ValueError(
                f'Expected storage type to be "mem" or "db"; got {storage_type} instead'
            )
        # Set the version number
        self.raw_version = version
        # Need to use var augment to check if this corpus_id exists because
        # in the database, collections such as (e.g.) [corpus1_utterances,
        # corpus1_speakers, etc. ..., corpus2_utterances, corpus2_speakers, etc.]
        # are stored in one name space; where as on disk, collections will just be
        # the list of top level directories (e.g.) [corpus1, corpus2], with
        # child collections utterances, speakers, etc. within those directories.
        if storage_type == 'db':
            collections = self.db.list_collection_names()
            augment = '_utterances'
        else:
            collections = os.listdir(data_dir) if os.path.isdir(
                data_dir) else []
            augment = ''
        if not in_place and f'{make_full_name(self.corpus_id, version)}{augment}' in collections:
            x = 1
            while True:
                test_version = f'{version}.{x}'
                if f'{make_full_name(self.corpus_id, test_version)}{augment}' in collections:
                    x += 1
                else:
                    self.version = test_version
                    break
        else:
            self.version = version

    @property
    def full_name(self):
        return make_full_name(self.corpus_id, self.version)

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
                for collection_name in value.list_collection_names():
                    warn(
                        f'\tDropping collection {collection_name} (collection in storage.{attr}))'
                    )
                    value.drop_collection(collection_name)

    @staticmethod
    def default_storage_mode():
        tmp_mode = os.environ.get('CONVOKIT_STORAGE_MODE')
        if tmp_mode in ['mem', 'db']:
            return tmp_mode
        else:
            config = read_or_create_config(
                os.path.expanduser('~/.convokit/config.yml'))
            return config['default_storage_mode']

    @staticmethod
    def purge_db():
        """
        Remove all data in the default database. 
        """
        warn('purging the DB')
        client = MongoClient()
        db = client['convokit']
        for collection_name in db.list_collection_names():
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
                and self.full_name == other.full_name

    def __repr__(self):
        return f'<StorageManager: {self.storage_type}-{self.full_name}>'


def safe_corpus_id():
    return str(randrange(2**15, 2**20))


def make_full_name(corpus_id, version):
    return f'{corpus_id}_v{version}'


def read_or_create_config(config_fullpath):
    if not os.path.isfile(config_fullpath):
        convo_dir = os.path.expanduser('~/.convokit')
        if not os.path.isdir(convo_dir):
            os.makedirs(convo_dir)
        with open(config_fullpath, 'w') as f:
            text = ("# Default Storage Parameters\n"
                    "db_host : localhost:27017\n"
                    "data_dir: ~/.convokit/saved-corpora\n"
                    "default_storage_mode: mem")
            print(
                f'No configuration file found at {config_fullpath}; writing with contents: \n{text}'
            )
            f.write(text)
            config = load(text, Loader=Loader)
    else:
        with open(config_fullpath, 'r') as f:
            config = load(f.read(), Loader=Loader)
    return config