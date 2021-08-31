from .convoKitIndex import ConvoKitIndex
from .convoKitMeta import ConvoKitMeta
from convokit.util import warn, deprecation
from typing import List, Optional, Type

from convokit.storage import defaultStorageManager, DBDocumentMapping, StorageManager


class CorpusComponent:
    def __init__(self,
                 obj_type: str,
                 owner=None,
                 id=None,
                 vectors: List[str] = None,
                 meta=None,
                 storage: Optional[StorageManager] = None):
        if storage is not None:
            self.storage = storage
        elif owner is not None:
            self.storage = owner.storage
        else:
            self.storage = defaultStorageManager

        self._utterances = self.storage._utterances
        self._conversations = self.storage._conversations
        self._speakers = self.storage._speakers

        if id is None:
            id = 'tmp'  # Todo: make more robust

        self.fields = self.storage.ItemMapping(
            self._speakers if obj_type == 'speaker' else self._conversations
            if obj_type == 'conversation' else self._utterances, id)

        self.owner = owner
        self.obj_type = obj_type  # utterance, speaker, conversation

        self.meta: ConvoKitMeta = self.init_meta(meta)
        self.vectors = vectors if vectors is not None else []

        self.id = id

    # Defining Properties for abstract storage
    # @property
    # def owner(self):
    #     return self.fields.__getitem__('owner')

    # @owner.setter
    # def owner(self, new_owner):
    #     self.fields.__setitem__('owner', new_owner)
    #     if new_owner is not None and hasattr(self, 'meta'):
    #         self.meta = self.init_meta(self.meta)

    @property
    def utterance_ids(self):
        return self.fields['utterance_ids']

    @utterance_ids.setter
    def utterance_ids(self, new_utterance_ids):
        self.fields['utterance_ids'] = new_utterance_ids

    @property
    def speaker_ids(self):
        return self.fields['speaker_ids']

    @speaker_ids.setter
    def speaker_ids(self, new_speaker_ids):
        self.fields['speaker_ids'] = new_speaker_ids

    @property
    def conversation_ids(self):
        return self.fields['conversation_ids']

    @conversation_ids.setter
    def conversation_ids(self, new_conversation_ids):
        self.fields['conversation_ids'] = new_conversation_ids

    @property
    def obj_type(self):
        return self.fields['obj_type']

    @obj_type.setter
    def obj_type(self, new_obj_type):
        self.fields['obj_type'] = new_obj_type

    @property
    def id(self):
        return self.fields.__getitem__('id')

    @id.setter
    def id(self, new_id):
        if not isinstance(new_id, str) and new_id is not None:
            self.fields.__setitem__('id', str(new_id))
            warn(
                "{} id must be a string. ID input has been casted to a string."
                .format(self.obj_type))
        else:
            self.fields.__setitem__('id', new_id)

    @property
    def vectors(self):
        return self.fields.__getitem__('vectors')

    @vectors.setter
    def vectors(self, new_vectors):
        self.fields.__setitem__('vectors', new_vectors)

    def init_meta(self, meta):
        if self.owner is None:
            index = ConvoKitIndex(None)
        else:
            index = self.owner.meta_index
        ck_meta = ConvoKitMeta(index, self.obj_type)
        if meta is not None:
            for key, value in meta.items():
                ck_meta[key] = value
        return ck_meta

    def _add_utterance(self, utt):
        if self.utterance_ids is None:
            self.utterance_ids = [utt.id]
        else:
            self.utterance_ids.append(utt.id)

        self.utterances[utt.id] = utt

    def _add_conversation(self, convo):
        if self.conversation_ids is None:
            self.conversation_ids = [convo.id]
        else:
            self.conversation_ids.append(convo.id)

        self.conversations[convo.id] = convo

    # def __eq__(self, other):
    #     if type(self) != type(other): return False
    #     # do not compare 'utterances' and 'conversations' in Speaker.__dict__. recursion loop will occur.
    #     self_keys = set(self.__dict__).difference(['_owner', 'meta', 'utterances', 'conversations'])
    #     other_keys = set(other.__dict__).difference(['_owner', 'meta', 'utterances', 'conversations'])
    #     return self_keys == other_keys and all([self.__dict__[k] == other.__dict__[k] for k in self_keys])

    def retrieve_meta(self, key: str):
        """
        Retrieves a value stored under the key of the metadata of corpus object

        :param key: name of metadata attribute
        :return: value
        """
        return self.meta.get(key, None)

    def add_meta(self, key: str, value) -> None:
        """
        Adds a key-value pair to the metadata of the corpus object

        :param key: name of metadata attribute
        :param value: value of metadata attribute
        :return: None
        """
        self.meta[key] = value

    def get_info(self, key):
        """
        Gets attribute <key> of the corpus object. Returns None if the corpus object does not have this attribute.

        :param key: name of attribute
        :return: attribute <key>
        """
        deprecation("get_info()", "retrieve_meta()")
        return self.meta.get(key, None)

    def set_info(self, key, value):
        """
        Sets attribute <key> of the corpus object to <value>.

        :param key: name of attribute
        :param value: value to set
        :return: None
        """
        deprecation("set_info()", "add_meta()")
        self.meta[key] = value

    def get_vector(self,
                   vector_name: str,
                   as_dataframe: bool = False,
                   columns: Optional[List[str]] = None):
        """
        Get the vector stored as `vector_name` for this object.

        :param vector_name: name of vector
        :param as_dataframe: whether to return the vector as a dataframe (True) or in its raw array form (False). False
            by default.
        :param columns: optional list of named columns of the vector to include. All columns returned otherwise. This
            parameter is only used if as_dataframe is set to True
        :return: a numpy / scipy array
        """
        if vector_name not in self.vectors:
            raise ValueError("This {} has no vector stored as '{}'.".format(
                self.obj_type, vector_name))

        return self.owner.get_vector_matrix(vector_name).get_vectors(
            ids=[self.id], as_dataframe=as_dataframe, columns=columns)

    def add_vector(self, vector_name: str):
        """
        Logs in the Corpus component object's internal vectors list that the component object has a vector row
        associated with it in the vector matrix named `vector_name`.

        Transformers that add vectors to the Corpus should use this to update the relevant component objects during
        the transform() step.

        :param vector_name: name of vector matrix
        :return: None
        """
        if vector_name not in self.vectors:
            self.vectors.append(vector_name)

    def has_vector(self, vector_name: str):
        return vector_name in self.vectors

    def delete_vector(self, vector_name: str):
        """
        Delete a vector associated with this Corpus component object.

        :param vector_name:
        :return: None
        """
        self.vectors.remove(vector_name)

    def __str__(self):
        return "{}(id: {}, vectors: {}, meta: {})".format(
            self.obj_type.capitalize(), self.id, self.vectors, self.meta)

    def __hash__(self):
        return hash(self.obj_type + str(self.id))

    def __repr__(self):
        copy = self.__dict__.copy()
        deleted_keys = [
            'utterances', 'conversations', 'user', '_root', '_utterance_ids',
            '_speaker_ids'
        ]
        for k in deleted_keys:
            if k in copy:
                del copy[k]

        to_delete = [k for k in copy if k.startswith('_')]
        to_add = {k[1:]: copy[k] for k in copy if k.startswith('_')}

        for k in to_delete:
            del copy[k]

        copy.update(to_add)

        try:
            return self.obj_type.capitalize() + "(" + str(copy) + ")"
        except AttributeError:  # for backwards compatibility when corpus objects are saved as binary data, e.g. wikiconv
            return "(" + str(copy) + ")"

    # @classmethod
    # def from_dbdoc(cls, doc):
    #     raise NotImplementedError()

    @classmethod
    def from_dbdoc(cls, doc: DBDocumentMapping, storage: StorageManager):
        ret = cls(from_db=True, storage=storage)
        ret.fields = doc
        return ret