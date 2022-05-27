import unittest
from pymongo import MongoClient
from convokit import DBDocumentMapping, DBCollectionMapping, StorageManager, Utterance, Conversation, Speaker, Corpus, ConvoKitMeta

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

        self.assertDictEqual(col1['doc1'], {
            'name': 'first document',
            'NAME': 'FIRST DOCUMENT',
            'n': 2,
        })

        self.assertDictEqual(doc1.dict(with_id=False), {
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

        # Check lengths
        self.assertEqual(len(col1), 2)
        self.assertEqual(len(doc1), 3)
        self.assertEqual(len(col1['doc2']), 1)

    def test_db_corpusComponent(self):
        storage = StorageManager(storage_type='db', corpus_id='direct_storage')
        print(
            f'(test_db_corpusComponent)\tstorage.CollectionMapping: {storage.CollectionMapping}'
        )
        # For testing repeatability.
        print('purging DB storage')
        storage.purge_all_collections()
        storage.setup_collections(Utterance, Conversation, Speaker,
                                  ConvoKitMeta)

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

        u0_ = storage.utterances['0']
        u1_ = storage.utterances['1']

        self.assertEqual(u0, u0_)
        self.assertEqual(u1, u1_)

        self.assertEqual(u0.speaker, u0_.speaker)
        self.assertEqual(u1.speaker, u1_.speaker)

        self.assertEqual(c0.speaker_ids, ['Bob', 'Jim'])
        self.assertEqual(c0.get_utterance_ids(), ['0', '1'])

        del u0, u1, u0_, u1_, storage, c0

        # Test persistent storage
        storage_ = StorageManager(storage_type='db',
                                  corpus_id='direct_storage',
                                  in_place=True)
        storage_.setup_collections(Utterance, Conversation, Speaker,
                                   ConvoKitMeta)

        # Test persistent storage
        self.assertEqual(storage_._speakers['Bob'].utterances['0'],
                         storage_._utterances['0'])
        self.assertEqual(storage_._utterances['1'].speaker,
                         storage_._speakers['Jim'])

        self.assertEqual(storage_._speakers['Bob'].utterances['0'].text,
                         BOBS_TEXT)
        self.assertEqual(storage_._utterances['1'].speaker.id, 'Jim')
        self.assertEqual(storage_._conversations[None].speaker_ids,
                         ['Bob', 'Jim'])

        self.assertEqual(storage_._conversations[None].get_utterance_ids(),
                         ['0', '1'])

    def test_mem_corpusComponent(self):
        storage = StorageManager(storage_type='mem')
        print(
            f'(test_mem_corpusComponent)\tstorage.CollectionMapping: {storage.CollectionMapping}'
        )
        print('purging mem storage (no-op)')
        storage.purge_all_collections()

        storage.setup_collections(Utterance, Conversation, Speaker,
                                  ConvoKitMeta)
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

        u0_ = storage.utterances['0']
        u1_ = storage.utterances['1']

        self.assertEqual(u0, u0_)
        self.assertEqual(u1, u1_)

        self.assertEqual(u0.speaker, u0_.speaker)
        self.assertEqual(u1.speaker, u1_.speaker)

        self.assertEqual(c0.speaker_ids, ['Bob', 'Jim'])
        self.assertEqual(c0.get_utterance_ids(), ['0', '1'])

        del u0, u1, u0_, u1_, storage, c0

        # Test the lack of persistent storage
        storage_ = StorageManager(storage_type='mem')
        storage_.setup_collections(Utterance, Conversation, Speaker,
                                   ConvoKitMeta)

        with self.assertRaises(KeyError):
            storage_._speakers['Bob']
        with self.assertRaises(KeyError):
            storage_._utterances['1']

    def test_db_migration(self):
        _storage = StorageManager(storage_type='db',
                                  corpus_id='not_the_default_corpus')
        _storage.purge_all_collections()

        del _storage

        outside_storage = StorageManager(storage_type='db',
                                         corpus_id='not_the_default_corpus')
        outside_storage.setup_collections(Utterance, Conversation, Speaker,
                                          ConvoKitMeta)

        corpus = Corpus(storage_type='db')
        corpus.storage.purge_all_collections()

        u = Utterance(id='0',
                      text='Starting the convo.',
                      speaker=Speaker(id='Bob', storage=outside_storage),
                      storage=outside_storage)
        text_len = len(u.text)
        height = "6'2''"
        u.meta['text-len'] = text_len
        u.speaker.meta['height'] = height
        print(u)

        self.assertEqual(u.storage.storage_type, 'db')
        print(231)
        corpus = corpus.add_utterances([u])

        print(u)
        print(corpus.get_utterance('0'))
        print(236)

        self.assertEqual(u, corpus.get_utterance('0'))
        self.assertEqual(corpus.get_utterance('0').text, 'Starting the convo.')

        self.assertEqual(u.meta, {'text-len': text_len})
        self.assertEqual(u.speaker.meta, {'height': height})

        self.assertEqual(u.meta.get('text-len', None), text_len)
        self.assertEqual(u.retrieve_meta('text-len'), text_len)
        self.assertEqual(u.speaker.retrieve_meta('height'), height)

        self.assertEqual(outside_storage.metas['utterance_0'],
                         {'text-len': text_len})

        self.assertEqual(
            corpus.get_utterance('0').meta, {'text-len': text_len})
        self.assertEqual(
            corpus.get_utterance('0').speaker.meta, {'height': height})
        self.assertEqual(
            corpus.get_utterance('0').retrieve_meta('text-len'), text_len)
        self.assertEqual(
            corpus.get_speaker('Bob').retrieve_meta('height'), height)

    def test_init_corpus_from_db(self):
        storage = StorageManager(storage_type='db', corpus_id='testo')
        storage.purge_all_collections()

        storage.utterances = storage.CollectionMapping('utterances',
                                                        item_type=Utterance)
        storage.conversations = storage.CollectionMapping(
            'conversations', item_type=Conversation)
        storage.speakers = storage.CollectionMapping('speakers',
                                                      item_type=Speaker)
        storage.metas = storage.CollectionMapping('metas',
                                                   item_type=ConvoKitMeta)

        s0 = Speaker(id="alice", storage=storage)
        self.assertEqual(storage.speakers['alice'], s0)
        self.assertEqual(s0.id, 'alice')

        s1 = Speaker(id="bob", storage=storage)
        s2 = Speaker(id="charlie", storage=storage)

        u0 = Utterance(id="0",
                       text="hello world!!!",
                       speaker=s0,
                       storage=storage)
        self.assertEqual(u0.fields['speaker_id'], 'alice')
        self.assertEqual(u0.speaker.id, 'alice')

        u1 = Utterance(id="1",
                       text="my name is bob",
                       speaker=s1,
                       storage=storage)
        u2 = Utterance(id="2",
                       text="this is a test",
                       speaker=s2,
                       storage=storage)

        corpus = Corpus(utterances=[u0, u1, u2],
                        storage_type='db',
                        corpus_id='testo')
        self.assertEqual(corpus.get_speaker('alice'), s0)
        self.assertEqual(corpus.get_utterance('1').speaker, s1)


if __name__ == '__main__':
    unittest.main()
