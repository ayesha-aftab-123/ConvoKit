from typing import Dict, Optional
from convokit.util import deprecation, warn
from .corpusComponent import CorpusComponent
from .speaker import Speaker
# from .conversation import Conversation

from convokit.storage import StorageManager


class Utterance(CorpusComponent):
    """Represents a single utterance in the dataset.

    :param id: the unique id of the utterance.
    :param speaker: the speaker giving the utterance.
    :param conversation_id: the id of the root utterance of the conversation.
    :param reply_to: id of the utterance this was a reply to.
    :param timestamp: timestamp of the utterance. Can be any
        comparable type.
    :param text: text of the utterance.

    :ivar id: the unique id of the utterance.
    :ivar speaker: the speaker giving the utterance.
    :ivar conversation_id: the id of the root utterance of the conversation.
    :ivar reply_to: id of the utterance this was a reply to.
    :ivar timestamp: timestamp of the utterance.
    :ivar text: text of the utterance.
    :ivar meta: A dictionary-like view object providing read-write access to
        utterance-level metadata.
    """
    def __init__(self,
                 owner=None,
                 id: Optional[str] = None,
                 speaker: Optional[Speaker] = None,
                 user: Optional[Speaker] = None,
                 conversation_id: Optional[str] = None,
                 root: Optional[str] = None,
                 reply_to: Optional[str] = None,
                 timestamp: Optional[int] = None,
                 text: str = '',
                 meta: Optional[Dict] = None,
                 from_db=False,
                 storage: Optional[StorageManager] = None):
        super().__init__(obj_type="utterance",
                         owner=owner,
                         id=id,
                         meta=meta,
                         storage=storage,
                         from_db=from_db)
        if from_db:
            return
        speaker_ = speaker if speaker is not None else user
        if speaker_ is None:
            raise ValueError(
                "No Speaker found: Utterance must be initialized with a Speaker."
            )
        else:
            self.speaker = speaker_
        conversation_id = conversation_id if conversation_id is not None else root
        if conversation_id is not None and not isinstance(
                conversation_id, str):
            warn(
                "Utterance conversation_id must be a string: conversation_id of utterance with ID: {} "
                "has been casted to a string.".format(self.id))
            conversation_id = str(self.conversation_id)
        elif conversation_id is None:
            conversation_id = id
        self.conversation_id = conversation_id
        # if self.conversation is None:
        #     self.conversation = Conversation(id=self.conversation_id,
        #                                      storage=self.storage)
        self.reply_to = reply_to
        self.timestamp = timestamp  # int(timestamp) if timestamp is not None else timestamp
        if not isinstance(text, str):
            warn(
                "Utterance text must be a string: text of utterance with ID: {} "
                "has been casted to a string.".format(self.id))
            text = '' if text is None else str(text)
        self.text = text

        self.storage._utterances[id] = self

    # Defining Properties for abstract storage
    @property
    def conversation_id(self):
        return self.fields['conversation_id']

    @conversation_id.setter
    def conversation_id(self, new_conversation_id):
        self.fields['conversation_id'] = new_conversation_id

    @property
    def conversation(self):
        return self.storage._conversations[self.conversation_id]

    @conversation.setter
    def conversation(self, new_conversation):
        self.conversation_id = new_conversation.id
        self.storage._conversations[self.conversation_id] = new_conversation

    @property
    def speaker(self):
        return self.storage._speakers[self.speaker_id]

    @speaker.setter
    def speaker(self, new_speaker):
        self.speaker_id = new_speaker.id
        # if self.storage.storage_type == 'db':
        #     print(f'\tUtterance {self.id} - {self.storage.db.name}: '
        #           f'Set self.speaker_id to {self.speaker_id }')
        self.storage._speakers[self.speaker_id] = new_speaker

    @property
    def speaker_id(self):
        return self.fields.__getitem__('speaker_id')

    @speaker_id.setter
    def speaker_id(self, new_speaker_id):
        self.fields.__setitem__('speaker_id', new_speaker_id)

    @property
    def reply_to(self):
        return self.fields.__getitem__('reply_to')

    @reply_to.setter
    def reply_to(self, new_reply_to):
        self.fields.__setitem__('reply_to', new_reply_to)

    @property
    def timestamp(self):
        return self.fields.__getitem__('timestamp')

    @timestamp.setter
    def timestamp(self, new_timestamp):
        self.fields.__setitem__('timestamp', new_timestamp)

    @property
    def text(self):
        return self.fields.__getitem__('text')

    @text.setter
    def text(self, new_text):
        self.fields.__setitem__('text', new_text)

    # Properties for backwards compatability
    @property
    def root(self):
        deprecation("utterance.root", "utterance.conversation_id")
        return self.conversation_id

    @root.setter
    def root(self, new_root: str):
        deprecation("utterance.root", "utterance.conversation_id")
        self.conversation_id = new_root

    @property
    def user(self):
        deprecation("utterance.user", "utterance.speaker")
        return self.speaker

    @user.setter
    def user(self, new_user: str):
        deprecation("utterance.user", "utterance.speaker")
        self.speaker = new_user

    ##

    def get_conversation(self):
        """
        Get the Conversation (identified by Utterance.conversation_id) this Utterance belongs to

        :return: a Conversation object
        """
        return self.storage._conversations[self.conversation_id]

    def get_speaker(self):
        """
        Get the Speaker that made this Utterance.

        :return: a Speaker object
        """
        return self.storage._speakers[self.speaker_id]

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        # print('utterance __eq__:')
        if not isinstance(other, Utterance):
            return False
        try:
            # print('\ttrying comparison of fields')
            mine = (self.id, self.conversation_id, self.reply_to, self.speaker,
                    self.timestamp, self.text)
            theirs = (other.id, other.conversation_id, other.reply_to,
                      other.speaker, other.timestamp, other.text)
            # print('\t', mine)
            # print('\t', theirs)
            return self.id == other.id and self.conversation_id == other.conversation_id and self.reply_to == other.reply_to and \
                   self.speaker == other.speaker and self.timestamp == other.timestamp and self.text == other.text
        except AttributeError as e:  # for backwards compatibility with wikiconv
            # print(f'\tUsing direct __dict__ comparison because of error {e}')
            return self.__dict__ == other.__dict__

    def __str__(self):
        return "Utterance(id: {}, conversation_id: {}, reply-to: {}, " \
               "speaker: {}, timestamp: {}, text: {}, vectors: {}, meta: {})".format(repr(self.id),
                                                                                    self.conversation_id,
                                                                                    self.reply_to,
                                                                                    self.speaker,
                                                                                    self.timestamp,
                                                                                    repr(self.text),
                                                                                    self.vectors,
                                                                                    self.meta)

    def __repr__(self):
        return self.__str__()