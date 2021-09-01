import unittest
from pymongo import MongoClient
from convokit.storage import DBDocumentMapping, DBCollectionMapping, StorageManager, defaultStorageManager
from convokit.model import Utterance, Conversation, Speaker

BOBS_TEXT = "Hi, I'm Bob."
JIMS_TEXT = "Hi Bob, I'm Jim!"


class DBStorage(unittest.TestCase):
    def test_insert_and_modify(self):
        storage = StorageManager(storage_type='db')

        CollectionMapping = DBCollectionMapping.with_storage(storage)
        col1 = CollectionMapping("Collection-1")
        col1.collection.drop()  # for testing repetability

        # Test doc1 reads from the DB collection col1
        col1['doc1'] = {'name': 'first document', 'n': 1}
        doc1 = DBDocumentMapping(col1, 'doc1')

        self.assertEqual(doc1['name'], 'first document')
        self.assertEqual(doc1['n'], 1)

        # Test doc1 can modify the db collection col1
        doc1['NAME'] = 'FIRST DOCUMENT'
        doc1['n'] = doc1['n'] + 1

        self.assertDictEqual(
            col1['doc1'], {
                '_id': 'doc1',
                'name': 'first document',
                'NAME': 'FIRST DOCUMENT',
                'n': 2
            })

        self.assertDictEqual(
            doc1.dict(), {
                '_id': 'doc1',
                'name': 'first document',
                'NAME': 'FIRST DOCUMENT',
                'n': 2
            })

        # Test another document, verifying the wrapper class against the DB directly.
        col1['doc2'] = {'I am': 'the second documet'}
        self.assertEqual(
            col1.collection.find_one({'_id': 'doc2'})['I am'],
            'the second documet')
        self.assertEqual(col1['doc2']['I am'], 'the second documet')

        doc2 = DBDocumentMapping(col1, 'doc2')
        doc2['I am'] = 'the best ever'

        self.assertEqual(
            col1.collection.find_one({'_id': 'doc2'})['I am'], 'the best ever')
        self.assertEqual(col1['doc2']['I am'], 'the best ever')

        # for doc in col1:
        #     print(doc)
        #     for field in doc:
        #         print(field)

        # Check lengths
        self.assertEqual(len(col1), 2)
        self.assertEqual(len(doc1), 4)
        self.assertEqual(len(col1['doc2']), 2)

    def test_db_corpusComponent(self):
        storage = StorageManager(storage_type='db')
        # For testing repeatability.
        print('purging DB storage')
        storage.purge_all_collections()

        storage._utterances = storage.CollectionMapping('utterances',
                                                        item_type=Utterance)
        storage._conversations = storage.CollectionMapping(
            'conversations', item_type=Conversation)
        storage._speakers = storage.CollectionMapping('speakers',
                                                      item_type=Speaker)

        u0 = Utterance(id='0',
                       speaker=Speaker(id='Bob', storage=storage),
                       text=BOBS_TEXT,
                       reply_to=None,
                       storage=storage)
        u1 = Utterance(id='1',
                       speaker=Speaker(id='Jim', storage=storage),
                       text=JIMS_TEXT,
                       reply_to='0',
                       storage=storage)

        c0 = Conversation(utterances=['0'], storage=storage)
        c0._add_utterance(u0)
        c0._add_utterance(u1)

        u0_ = storage._utterances['0']
        u1_ = storage._utterances['1']

        self.assertEqual(u0, u0_)
        self.assertEqual(u1, u1_)

        self.assertEqual(u0.speaker, u0_.speaker)
        self.assertEqual(u1.speaker, u1_.speaker)

        self.assertEqual(c0.speaker_ids, ['Bob', 'Jim'])
        self.assertEqual(c0.get_utterance_ids(), ['0', '1'])

        del u0, u1, u0_, u1_, storage, c0

        # Test persistent storage
        storage_ = StorageManager(storage_type='db')
        storage_._utterances = storage_.CollectionMapping('utterances',
                                                          item_type=Utterance)
        storage_._conversations = storage_.CollectionMapping(
            'conversations', item_type=Conversation)
        storage_._speakers = storage_.CollectionMapping('speakers',
                                                        item_type=Speaker)

        # Test persistent storage
        self.assertEqual(storage_._speakers['Bob'].utterances['0'],
                         storage_._utterances['0'])
        self.assertEqual(storage_._utterances['1'].speaker,
                         storage_._speakers['Jim'])

        self.assertEqual(storage_._speakers['Bob'].utterances['0'].text,
                         BOBS_TEXT)
        self.assertEqual(storage_._utterances['1'].speaker.id, 'Jim')
        self.assertEqual(storage_._conversations['0'].speaker_ids,
                         ['Bob', 'Jim'])

        self.assertEqual(storage_._conversations['0'].get_utterance_ids(),
                         ['0', '1'])

    def test_mem_corpusComponent(self):
        storage = StorageManager(storage_type='mem')
        print('purging mem storage (no-op)')
        storage.purge_all_collections()

        storage._utterances = storage.CollectionMapping('utterances',
                                                        item_type=Utterance)
        storage._conversations = storage.CollectionMapping(
            'conversations', item_type=Conversation)
        storage._speakers = storage.CollectionMapping('speakers',
                                                      item_type=Speaker)

        u0 = Utterance(id='0',
                       speaker=Speaker(id='Bob', storage=storage),
                       text=BOBS_TEXT,
                       reply_to=None,
                       storage=storage)
        u1 = Utterance(id='1',
                       speaker=Speaker(id='Jim', storage=storage),
                       text=JIMS_TEXT,
                       reply_to='0',
                       storage=storage)

        c0 = Conversation(utterances=['0'], storage=storage)
        c0._add_utterance(u0)
        c0._add_utterance(u1)

        u0_ = storage._utterances['0']
        u1_ = storage._utterances['1']

        self.assertEqual(u0, u0_)
        self.assertEqual(u1, u1_)

        self.assertEqual(u0.speaker, u0_.speaker)
        self.assertEqual(u1.speaker, u1_.speaker)

        self.assertEqual(c0.speaker_ids, ['Bob', 'Jim'])
        self.assertEqual(c0.get_utterance_ids(), ['0', '1'])

        del u0, u1, u0_, u1_, storage, c0

        # Test the lack of persistent storage
        storage_ = StorageManager(storage_type='mem')
        storage_._utterances = storage_.CollectionMapping('utterances',
                                                          item_type=Utterance)
        storage_._conversations = storage_.CollectionMapping(
            'conversations', item_type=Conversation)
        storage_._speakers = storage_.CollectionMapping('speakers',
                                                        item_type=Speaker)
        with self.assertRaises(KeyError):
            storage_._speakers['Bob']
        with self.assertRaises(KeyError):
            storage_._utterances['1']

    def test_default_corpusComponent(self):
        # For testing repeatability.
        print('purging default storage')
        defaultStorageManager.purge_all_collections(
        )  # Todo: remove before shipping
        u0 = Utterance(id='0',
                       speaker=Speaker(id='Bob'),
                       text=BOBS_TEXT,
                       reply_to=None)
        u1 = Utterance(id='1',
                       speaker=Speaker(id='Jim'),
                       text=JIMS_TEXT,
                       reply_to='0')

        c0 = Conversation(utterances=['0'])
        c0._add_utterance(u0)
        c0._add_utterance(u1)

        u0_ = defaultStorageManager._utterances['0']
        u1_ = defaultStorageManager._utterances['1']

        self.assertEqual(u0, u0_)
        self.assertEqual(u1, u1_)

        self.assertEqual(u0.speaker, u0_.speaker)
        self.assertEqual(u1.speaker, u1_.speaker)

        self.assertEqual(c0.speaker_ids, ['Bob', 'Jim'])
        self.assertEqual(c0.get_utterance_ids(), ['0', '1'])

        del u0, u1, u0_, u1_, c0

        # Test persistent storage
        self.assertEqual(
            defaultStorageManager._speakers['Bob'].utterances['0'],
            defaultStorageManager._utterances['0'])
        self.assertEqual(defaultStorageManager._utterances['1'].speaker,
                         defaultStorageManager._speakers['Jim'])

        self.assertEqual(
            defaultStorageManager._speakers['Bob'].utterances['0'].text,
            BOBS_TEXT)
        self.assertEqual(defaultStorageManager._utterances['1'].speaker.id,
                         'Jim')

        self.assertEqual(defaultStorageManager._conversations['0'].speaker_ids,
                         ['Bob', 'Jim'])
        self.assertEqual(
            defaultStorageManager._conversations['0'].get_utterance_ids(),
            ['0', '1'])


if __name__ == '__main__':
    unittest.main()
