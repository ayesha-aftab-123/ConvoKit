from collections import MutableMapping
from convokit.util import warn
from .convoKitIndex import ConvoKitIndex
import json

# See reference: https://stackoverflow.com/questions/7760916/correct-usage-of-a-getter-setter-for-dictionary-values


class ConvoKitMeta(MutableMapping):
    """
    ConvoKitMeta is a dictlike object that stores the metadata attributes of a corpus component
    """
    def __init__(self, convokit_index, obj_type):
        self.index: ConvoKitIndex = convokit_index
        self.storage = self.index.storage
        self.obj_type = obj_type
        self.fields = self.storage.ItemMapping(
            self.storage.CollectionMapping('meta'), 'corpus_meta')

    def __getitem__(self, item):
        return self.fields.__getitem__(item)

    @staticmethod
    def _check_type_and_update_index(index, obj_type, key, value):
        if not isinstance(value,
                          type(None)):  # do nothing to index if value is None
            # print(
            #     f'_check_type_and_update_index: key={key}\nindex.indices[obj_type]={index.indices[obj_type]}'
            # )
            if key not in index.indices[obj_type]:
                type_ = _optimized_type_check(value)
                index.update_index(obj_type, key=key, class_type=type_)
            else:
                # entry exists
                if index.get_index(obj_type)[key] != [
                        "bin"
                ]:  # if "bin" do no further checks
                    if str(type(value)) not in index.get_index(obj_type)[key]:
                        new_type = _optimized_type_check(value)

                        if new_type == "bin":
                            index.set_index(obj_type, key, "bin")
                        else:
                            index.update_index(obj_type, key, new_type)

    def __setitem__(self, key, value):
        # print(f'META: Setting meta[{key}] to {value}')
        if not isinstance(key, str):
            warn(
                "Metadata attribute keys must be strings. Input key has been casted to a string."
            )
            key = str(key)

        if True:  # self.index.type_check:
            ConvoKitMeta._check_type_and_update_index(self.index,
                                                      self.obj_type, key,
                                                      value)
        # self.index.update_index(self.obj_type, key, repr(
        #     type(value)))  # Todo: Should this be here?
        self.fields[key] = value

    def __delitem__(self, key):
        if self.obj_type == 'corpus':
            self.fields.__delitem__(key)
            self.index.del_from_index(self.obj_type, key)
        else:
            if self.index.lock_metadata_deletion[self.obj_type]:
                warn(
                    "For consistency in metadata attributes in Corpus component objects, deleting metadata attributes "
                    "from component objects individually is not allowed. "
                    "To delete this metadata attribute from all Corpus components of this type, "
                    "use corpus.delete_metadata(obj_type='{}', attribute='{}') instead."
                    .format(self.obj_type, key))
            else:
                self.fields.__delitem__(key)

    def __iter__(self):
        return self.fields.__iter__()

    def __len__(self):
        return self.fields.__len__()

    def __contains__(self, x):
        return self.fields.__contains__(x)

    def to_dict(self):
        return self.fields.__dict__

    def __eq__(self, o: object) -> bool:
        if isinstance(o, ConvoKitMeta):
            return self.fields.dict() == o.fields.dict()
        elif isinstance(o, dict):
            return self.fields.dict() == o
        else:
            return False

    def __repr__(self):
        return str(self.__dict__)


_basic_types = {type(0), type(1.0),
                type('str'), type(True)}  # cannot include lists or dicts


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
