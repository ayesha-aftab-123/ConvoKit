import unittest
from convokit.model import Utterance, Speaker, Corpus
from convokit import download, StorageManager


class CorpusFromPandas(unittest.TestCase):
    def setUp(self) -> None:
        StorageManager.purge_db()
        self.corpus = Corpus(download('subreddit-hey'))
        print(0, self.corpus.random_conversation().meta)
        utt_df = self.corpus.get_utterances_dataframe()
        convo_df = self.corpus.get_conversations_dataframe()
        speaker_df = self.corpus.get_speakers_dataframe()
        self.new_corpus = Corpus.from_pandas(speaker_df, utt_df, convo_df)

    def test_reconstruction_stats(self):
        """
        Test that reconstructing the Corpus from outputted dataframes results in the same number of corpus components
        """
        self.assertEqual(set(self.new_corpus.iter_utterances()),
                         set(self.corpus.iter_utterances()))
        self.assertEqual(set(self.new_corpus.iter_speakers()),
                         set(self.corpus.iter_speakers()))
        self.assertEqual(set(self.new_corpus.iter_conversations()),
                         set(self.corpus.iter_conversations()))

    def test_reconstruction_metadata(self):
        print(1, self.corpus.random_conversation().meta)
        print(2, self.new_corpus.random_conversation().meta)
        self.assertEqual(set(self.corpus.random_utterance().meta),
                         set(self.new_corpus.random_utterance().meta))
        self.assertEqual(set(self.corpus.random_conversation().meta),
                         set(self.new_corpus.random_conversation().meta))
        self.assertEqual(set(self.corpus.random_speaker().meta),
                         set(self.new_corpus.random_speaker().meta))

    def test_convo_reconstruction(self):
        for convo in self.new_corpus.iter_conversations():
            self.assertTrue(convo.check_integrity(verbose=False))


if __name__ == '__main__':
    unittest.main()
