import unittest
from convokit.model import Utterance, Speaker, Corpus
from convokit import download


class CorpusLoadAndDump(unittest.TestCase):
    """
    Load a variety of existing (small) corpora to verify that there are no backward compatibility issues
    """
    def test_load_dump_subreddit_mem(self):
        corpus = Corpus(download('subreddit-hey'), storage_type='mem')
        corpus.dump('subreddit')

    def test_load_dump_tennis_mem(self):
        corpus = Corpus(download('tennis-corpus'), storage_type='mem')
        corpus.dump('tennis-corpus')

    def test_load_dump_politeness_mem(self):
        corpus = Corpus(download('wikipedia-politeness-corpus'),
                        storage_type='mem')
        corpus.dump('wikipedia-politeness-corpus')

    def test_load_dump_switchboard_mem(self):
        corpus = Corpus(download("switchboard-corpus"), storage_type='mem')
        corpus.dump('switchboard-corpus')

    def test_load_wikiconv_mem(self):
        corpus = Corpus(download('wikiconv-2004'), storage_type='mem')


if __name__ == '__main__':
    unittest.main()
