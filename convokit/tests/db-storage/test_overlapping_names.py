import unittest

from convokit import (
    Utterance,
    Speaker,
    StorageManager,
)

BOBS_TEXT = "Hi, I'm Bob."
JIMS_TEXT = "Hi Bob, I'm Jim!"


# same ID different corpora, avoid putting into DB just by ID, no chance to overwrite


class DBStorage(unittest.TestCase):
    def test_insert_and_modify(self):
        StorageManager.purge_database()

        u0 = Utterance(id="0", speaker=Speaker(id="Sam"), text="Hi!", meta={"color": "red"})

        u0_1 = Utterance(
            id="0",
            speaker=Speaker(id="Bill"),
            text="Good Morning!",
            meta={"color": "green"},
        )

        self.assertEqual(u0.text, "Hi!")
        self.assertEqual(u0.meta, {"color": "red"})

        self.assertEqual(u0_1.text, "Good Morning!")
        self.assertEqual(u0_1.meta, {"color": "green"})


if __name__ == "__main__":
    unittest.main()
