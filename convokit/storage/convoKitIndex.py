from collections import defaultdict
from typing import Optional, Dict, List
import json


class ConvoKitIndex:
    def __init__(self,
                 storage,
                 utterances_index: Optional[Dict[str, List[str]]] = None,
                 speakers_index: Optional[Dict[str, List[str]]] = None,
                 conversations_index: Optional[Dict[str, List[str]]] = None,
                 overall_index: Optional[Dict[str, List[str]]] = None,
                 vectors: Optional[List[str]] = None,
                 version: Optional[int] = 0):

        self.storage = storage
        self.fields = {}
        self.utterances_index = utterances_index if utterances_index is not None else {}
        self.speakers_index = speakers_index if speakers_index is not None else {}
        self.conversations_index = conversations_index if conversations_index is not None else {}
        self.overall_index = overall_index if overall_index is not None else {}
        self.indices = {
            'utterance': self.utterances_index,
            'conversation': self.conversations_index,
            'speaker': self.speakers_index,
            'corpus': self.overall_index
        }
        self.vectors = list(vectors) if vectors is not None else []
        self.version = version
        self.type_check = True  # toggle-able to enable/disable type checks on metadata additions
        self.lock_metadata_deletion = {
            'utterance': True,
            'conversation': True,
            'speaker': True
        }

    # Defining Properties for abstract storage
    @property
    def utterances_index(self):
        return self.fields.__getitem__('utterances_index')

    @utterances_index.setter
    def utterances_index(self, new_utterances_index):
        self.fields.__setitem__('utterances_index', new_utterances_index)

    @property
    def speakers_index(self):
        return self.fields.__getitem__('speakers_index')

    @speakers_index.setter
    def speakers_index(self, new_speakers_index):
        self.fields.__setitem__('speakers_index', new_speakers_index)

    @property
    def conversations_index(self):
        return self.fields.__getitem__('conversations_index')

    @conversations_index.setter
    def conversations_index(self, new_conversations_index):
        self.fields.__setitem__('conversations_index', new_conversations_index)

    @property
    def overall_index(self):
        return self.fields.__getitem__('overall_index')

    @overall_index.setter
    def overall_index(self, new_overall_index):
        self.fields.__setitem__('overall_index', new_overall_index)

    @property
    def vectors(self):
        return self.fields.__getitem__('vectors')

    @vectors.setter
    def vectors(self, new_vectors):
        self.fields.__setitem__('vectors', new_vectors)

    @property
    def version(self):
        return self.fields.__getitem__('version')

    @version.setter
    def version(self, new_version):
        self.fields.__setitem__('version', new_version)

    @property
    def type_check(self):
        return self.fields.__getitem__('type_check')

    @type_check.setter
    def type_check(self, new_type_check):
        self.fields.__setitem__('type_check', new_type_check)

    @property
    def lock_metadata_deletion(self):
        return self.fields.__getitem__('lock_metadata_deletion')

    @lock_metadata_deletion.setter
    def lock_metadata_deletion(self, new_lock_metadata_deletion):
        self.fields.__setitem__('lock_metadata_deletion',
                                new_lock_metadata_deletion)

    ############

    def update_index(self, obj_type: str, key: str, class_type: str):
        """
        Append the class_type to the index

        :param obj_type: utterance, conversation, or speaker
        :param key: string
        :param class_type: class type
        :return: None
        """
        if type(key) != str:
            raise TypeError(f'key must be a string (got {type(key)} instead)')
        if not('class' in class_type or class_type == 'bin'):
            raise TypeError(f'class_type incorrect (got {class_type} instead)')
        if key not in self.indices[obj_type]:
            self.indices[obj_type][key] = []
        if class_type not in self.indices[obj_type][key]:
            self.indices[obj_type][key].append(class_type)

    def set_index(self, obj_type: str, key: str, class_type: str):
        """
        Set the class_type of the index as [`class_type`].

        :param obj_type: utterance, conversation, or speaker
        :param key: string
        :param class_type: class type
        :return: None
        """
        if type(key) != str:
            raise TypeError(f'key must be a string (got {type(key)} instead)')
        if not('class' in class_type or class_type == 'bin'):
            raise TypeError(f'class_type incorrect (got {class_type} instead)')
        self.indices[obj_type][key] = [class_type]

    def get_index(self, obj_type: str):
        return self.indices[obj_type]

    def del_from_index(self, obj_type: str, key: str):
        if type(key) != str:
            raise TypeError(f'key must be a string (got {type(key)} instead)')
        if key not in self.indices[obj_type]: return
        del self.indices[obj_type][key]
        #
        # corpus = self.owner
        # for corpus_obj in corpus.iter_objs(obj_type):
        #     if key in corpus_obj.meta:
        #         del corpus_obj.meta[key]

    def add_vector(self, vector_name):
        self.vectors.append(vector_name)

    def del_vector(self, vector_name):
        self.vectors.remove(vector_name)

    def update_from_dict(self, meta_index: Dict):
        self.conversations_index.update(meta_index["conversations-index"])
        self.utterances_index.update(meta_index["utterances-index"])
        speaker_index = "speakers-index" if "speakers-index" in meta_index else "users-index"
        self.speakers_index.update(meta_index[speaker_index])
        self.overall_index.update(meta_index["overall-index"])
        self.vectors = list(meta_index.get('vectors', []))
        for index in self.indices.values():
            for k, v in index.items():
                if isinstance(v, str):
                    index[k] = [v]

        self.version = meta_index["version"]

    def to_dict(self, exclude_vectors: List[str] = None, force_version=None):
        retval = dict()
        retval["utterances-index"] = self.utterances_index
        retval["speakers-index"] = self.speakers_index
        retval["conversations-index"] = self.conversations_index
        retval["overall-index"] = self.overall_index

        if force_version is None:
            retval['version'] = self.version + 1
        else:
            retval['version'] = force_version

        if exclude_vectors is not None:
            retval['vectors'] = list(set(self.vectors) - set(exclude_vectors))
        else:
            retval['vectors'] = list(self.vectors)

        return retval

    def enable_type_check(self):
        self.type_check = True

    def disable_type_check(self):
        self.type_check = False

    def __str__(self):
        return str(self.to_dict(force_version=self.version))

    def __repr__(self):
        return str(self)

    def reinitialize_for(self, corpus):
        """
        Reinitialize the Corpus Index from scratch.

        :return: The reinitialized index
        """
        new_index = ConvoKitIndex(corpus)

        new_index._reinitialize_index_helper(corpus, "utterance")
        new_index._reinitialize_index_helper(corpus, "speaker")
        new_index._reinitialize_index_helper(corpus, "conversation")

        for key, value in corpus.meta.items():  # overall
            new_index.update_index('corpus', key, str(type(value)))

        new_index.version = self.version
        return new_index

    def _reinitialize_index_helper(self, corpus, obj_type):
        """
        Helper for reinitializing the index of the different Corpus object types
        :param obj_type: utterance, speaker, or conversation
        :return: None (mutates new_index)
        """
        for obj in corpus.iter_objs(obj_type):
            for key, value in obj.meta.items():
                self._check_type_and_update_index(obj_type, key, value)
            obj.storage.index = self

    def _check_type_and_update_index(self, obj_type, key, value):
        if not isinstance(value,
                          type(None)):  # do nothing to index if value is None
            # print(
            #     f'_check_type_and_update_index: key={key}\nindex.indices[obj_type]={index.indices[obj_type]}'
            # )
            if key not in self.indices[obj_type]:
                type_ = _optimized_type_check(value)
                self.update_index(obj_type, key=key, class_type=type_)
            else:
                # entry exists
                if self.get_index(obj_type)[key] != [
                        "bin"
                ]:  # if "bin" do no further checks
                    if str(type(value)) not in self.get_index(obj_type)[key]:
                        new_type = _optimized_type_check(value)

                        if new_type == "bin":
                            self.set_index(obj_type, key, "bin")
                        else:
                            self.update_index(obj_type, key, new_type)


_basic_types = {type(0), type(1.0), type('str'), type(True)}  # cannot include lists or dicts


def _optimized_type_check(val):
    # if type(obj)
    if type(val) in _basic_types:
        return str(type(val))
    else:
        try:
            json.dumps(val)
            return str(type(val))
        except (TypeError, OverflowError):
            return "bin"
