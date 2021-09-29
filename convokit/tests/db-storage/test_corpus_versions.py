import unittest
from pymongo import MongoClient
from convokit import Utterance, Conversation, Speaker, Corpus, ConvoKitMeta, StorageManager

jim = Speaker(id='Jim')
bill = Speaker(id='Bill')


def get_version_test_corpus():
    StorageManager.purge_db()
    return Corpus(corpus_name='version_test_corpus',
                  storage_type='db',
                  utterances=[
                      Utterance(id='0',
                                reply_to=None,
                                conversation_id='0',
                                speaker=jim,
                                text='Hey, hows it going?'),
                      Utterance(id='1',
                                reply_to='0',
                                conversation_id='0',
                                speaker=bill,
                                text='Great thanks!'),
                      Utterance(id='2',
                                reply_to='1',
                                conversation_id='0',
                                speaker=jim,
                                text='Sweet, catch ya later!'),
                      Utterance(id='3',
                                reply_to='2',
                                conversation_id='0',
                                speaker=bill,
                                text='Bye!'),
                  ])


class DBStorage(unittest.TestCase):
    def test_connect_by_name_in_place(self):
        version_test_corpus0 = get_version_test_corpus()
        version_test_corpus1 = Corpus(corpus_name='version_test_corpus',
                                      storage_type='db',
                                      in_place=True)

        self.assertEqual(version_test_corpus0.storage,
                         version_test_corpus1.storage)

        for utt0 in version_test_corpus0.iter_utterances():
            utt1 = version_test_corpus1.get_utterance(utt0.id)
            self.assertEqual(utt0, utt1)
            self.assertEqual(utt0.meta, {})
            self.assertEqual(utt1.meta, {})
            utt0.meta['text-len'] = len(utt0.text)
            # print(0, repr(utt0))
            # print(1, repr(utt1))
            # print(0, repr(utt0.meta))
            # print(1, repr(utt1.meta))
            self.assertEqual(utt0.meta, {'text-len': len(utt0.text)})
            self.assertEqual(utt0.meta, utt1.meta)

        # Changes to utts in version_test_corpus0 should be reflected in
        # version_test_corpus1 since its initilized with in_place=True
        for utt1 in version_test_corpus1.iter_utterances():
            self.assertEqual(utt1.meta, {'text-len': len(utt1.text)})

        version_test_corpus0.add_utterances([
            Utterance(id='4',
                      reply_to=None,
                      conversation_id='1',
                      speaker=jim,
                      text='I could keep talking forever!',
                      meta={'testing': True})
        ])

        # In corpus 0
        self.assertEqual(
            version_test_corpus0.get_utterance('4').text,
            'I could keep talking forever!')
        self.assertEqual(version_test_corpus0.get_utterance('4').speaker, jim)
        self.assertTrue(
            version_test_corpus0.get_utterance('4').meta['testing'])

        # In corpus 1
        self.assertEqual(
            version_test_corpus1.get_utterance('4').text,
            'I could keep talking forever!')
        self.assertEqual(version_test_corpus1.get_utterance('4').speaker, jim)
        self.assertTrue(
            version_test_corpus1.get_utterance('4').meta['testing'])

    def test_connect_by_name_not_in_place(self):
        version_test_corpus0 = get_version_test_corpus()
        version_test_corpus1 = Corpus(corpus_name='version_test_corpus',
                                      storage_type='db',
                                      in_place=False)
        # print('version_test_corpus1.storage',
        #       repr(version_test_corpus1.storage))

        self.assertNotEqual(version_test_corpus0.storage,
                            version_test_corpus1.storage)

        for utt0 in version_test_corpus0.iter_utterances():
            utt1 = version_test_corpus1.get_utterance(utt0.id)
            self.assertEqual(utt0, utt1)
            self.assertEqual(utt0.meta, {})
            self.assertEqual(utt1.meta, {})
            utt0.meta['text-len'] = len(utt0.text)
            # print(0, repr(utt0))
            # print(1, repr(utt1))
            # print(0, repr(utt0.meta))
            # print(1, repr(utt1.meta))
            self.assertEqual(utt0.meta, {'text-len': len(utt0.text)})
            self.assertEqual(utt1.meta, {})

        # Changes to utts in version_test_corpus0 should not be reflected in
        # version_test_corpus1 since its initilized with in_place=False
        for utt1 in version_test_corpus1.iter_utterances():
            self.assertEqual(utt1.meta, {})

        version_test_corpus0.add_utterances([
            Utterance(id='4',
                      reply_to=None,
                      conversation_id='1',
                      speaker=jim,
                      text='I could keep talking forever!',
                      meta={'testing': True})
        ])

        with self.assertRaises(KeyError):
            version_test_corpus1.get_utterance('4')


if __name__ == '__main__':
    unittest.main()
