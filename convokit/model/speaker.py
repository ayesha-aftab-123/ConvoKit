# from .utterance import Utterance
# from .conversation import Conversation
from functools import total_ordering
from typing import Dict, List, MutableMapping, Optional, Callable
from convokit.util import deprecation
from .corpusComponent import CorpusComponent
from .corpusUtil import *

from convokit.storage import DBDocumentMapping, StorageManager


@total_ordering
class Speaker(CorpusComponent):
    """
    Represents a single speaker in a dataset.

    :param id: id of the speaker.
    :type id: str
    :param utts: dictionary of utterances by the speaker, where key is utterance id
    :param convos: dictionary of conversations started by the speaker, where key is conversation id
    :param meta: arbitrary dictionary of attributes associated
        with the speaker.
    :type meta: dict

    :ivar id: id of the speaker.
    :ivar meta: A dictionary-like view object providing read-write access to
        speaker-level metadata.
    """

    def __init__(
        self,
        owner=None,
        id: str = None,
        name: str = None,
        utts: MutableMapping = None,
        convos: MutableMapping = None,
        meta: Optional[Dict] = None,
        from_db=False,
        storage: Optional[StorageManager] = None,
    ):
        name_var = id if id is not None else name  # to be deprecated
        super().__init__(
            obj_type="speaker",
            owner=owner,
            id=name_var,
            meta=meta,
            storage=storage,
            from_db=from_db,
        )
        if from_db:
            return

        if utts is not None:
            for u_id, utt in utts:
                self.utterances[u_id] = utt
        if convos is not None:
            for c_id, convo in convos:
                self.conversations[c_id] = convo

        self.storage.speakers[id] = self

    # Properties for get-only access
    @property
    def utterances(self) -> MutableMapping:
        return self.storage.utterances

    @property
    def conversations(self) -> MutableMapping:
        return self.storage.conversations

    # Properties for backwards compatability
    @property
    def name(self):
        deprecation("speaker.name", "speaker.id")
        return self.id

    @name.setter
    def name(self, new_name):
        deprecation("speaker.name", "speaker.id")
        self.id = new_name

    # End properties

    def _check_utterance_id(self, ut_id: str):
        """self.utterance_ids is List for storage reasons, but checking list membership is expensive when many utterances present,
           so this convenience function runs a set conversion first (duplicates ok since we're just checking for membership)"""
        utterances_set = set(self.utterance_ids)
        return ut_id in utterances_set

    def get_utterance(self, ut_id: str):  # -> Utterance:
        """
        Get the Utterance with the specified Utterance id

        :param ut_id: The id of the Utterance
        :return: An Utterance object
        """
        if self._check_utterance_id(ut_id):
            return self.utterances[ut_id]
        else:
            return None

    def iter_utterances(self, selector=lambda utt: True):  # -> Generator[Utterance, None, None]:
        """
        Get utterances made by the Speaker, with an optional selector that selects for Utterances that
        should be included.

                :param selector: a (lambda) function that takes an Utterance and returns True or False (i.e. include / exclude).
                        By default, the selector includes all Utterances in the Corpus.
        :return: An iterator of the Utterances made by the speaker
        """
        for k, v in self.utterances.items():
            if self._check_utterance_id(k) and selector(v):
                yield v

    def get_utterances_dataframe(self, selector=lambda utt: True, exclude_meta: bool = False):
        """
        Get a DataFrame of the Utterances made by the Speaker with fields and metadata attributes.
        Set an optional selector that filters for Utterances that should be included.
        Edits to the DataFrame do not change the corpus in any way.

        :param exclude_meta: whether to exclude metadata
        :param selector: a (lambda) function that takes a Utterance and returns True or False (i.e. include / exclude).
                By default, the selector includes all Utterances in the Corpus.
        :return: a pandas DataFrame
        """
        return get_utterances_dataframe(self, selector, exclude_meta)

    def get_utterance_ids(self, selector=lambda utt: True) -> List[str]:
        """

        :return: a List of the ids of Utterances made by the speaker
        """
        return self.utterance_ids
        # return list([utt.id for utt in self.iter_utterances(selector)])

    def get_conversation(self, cid: str):  # -> Conversation:
        """
        Get the Conversation with the specified Conversation id

        :param cid: The id of the Conversation
        :return: A Conversation object
        """
        if cid in self.conversation_ids:
            return self.conversations[cid]
        else:
            return None

    def iter_conversations(
        self, selector=lambda convo: True
    ):  # -> Generator[Conversation, None, None]:
        """

        :return: An iterator of the Conversations that the speaker has participated in
        """
        for k, v in self.conversations.items():
            if k in self.conversation_ids and selector(v):
                yield v

    def get_conversations_dataframe(self, selector=lambda convo: True, exclude_meta: bool = False):
        """
        Get a DataFrame of the Conversations the Speaker has participated in, with fields and metadata attributes.
        Set an optional selector that filters for Conversations that should be included. Edits to the DataFrame do not
        change the corpus in any way.

        :param exclude_meta: whether to exclude metadata
        :param selector: a (lambda) function that takes a Conversation and returns True or False (i.e. include / exclude).
            By default, the selector includes all Conversations in the Corpus.
        :return: a pandas DataFrame
        """
        return get_conversations_dataframe(self, selector, exclude_meta)

    def get_conversation_ids(self, selector=lambda convo: True) -> List[str]:
        """

        :return: a List of the ids of Conversations started by the speaker
        """
        return self.conversation_ids
        # return [convo.id for convo in self.iter_conversations(selector)]

    def print_speaker_stats(self):
        """
        Helper function for printing the number of Utterances made and Conversations participated in by the Speaker.

        :return: None (prints output)
        """
        print("Number of Utterances: {}".format(len(list(self.iter_utterances()))))
        print("Number of Conversations: {}".format(len(list(self.iter_conversations()))))

    def __lt__(self, other):
        return self.id < other.id

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, Speaker):
            return False
        try:
            return self.id == other.id
        except AttributeError:
            return self.__dict__["_name"] == other.__dict__["_name"]

    def __str__(self):
        return f"Speaker(id: {self.id})"

    def __repr__(self):
        return self.__str__()
