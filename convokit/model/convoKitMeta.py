try:
    from collections.abc import MutableMapping
except:
    from collections import MutableMapping
from convokit.util import warn
from .convoKitIndex import ConvoKitIndex
import json

# See reference: https://stackoverflow.com/questions/7760916/correct-usage-of-a-getter-setter-for-dictionary-values


class ConvoKitMeta(MutableMapping, dict):
    """
    ConvoKitMeta is a dictlike object that stores the metadata attributes of a corpus component
    """

    def __init__(self, owner, convokit_index, obj_type):
        self.owner = owner  # Corpus or CorpusComponent
        self.index: ConvoKitIndex = convokit_index
        self.obj_type = obj_type

        self._get_storage().initialize_data_for_component("meta", self.storage_key)

    @property
    def storage_key(self) -> str:
        return f"{self.obj_type}_{self.owner.id}"

    def __getitem__(self, item):
        item_type = self.index.get_index(self.obj_type).get(item, None)
        return self._get_storage().get_data("meta", self.storage_key, item, item_type)

    def _get_storage(self):
        # special case for Corpus meta since that's the only time owner is not a CorpusComponent
        # since cannot directly import Corpus to check the type (circular import), as a proxy we
        # check for the obj_type attribute which is common to all CorpusComponent but not
        # present in Corpus
        if not hasattr(self.owner, "obj_type"):
            return self.owner.storage
        # self.owner -> CorpusComponent
        # self.owner.owner -> Corpus that owns the CorpusComponent (only Corpus has direct pointer to storage)
        return self.owner.owner.storage

    @staticmethod
    def _check_type_and_update_index(index, obj_type, key, value):
        if not isinstance(value, type(None)):  # do nothing to index if value is None
            if key not in index.indices[obj_type]:
                type_ = _optimized_type_check(value)
                index.update_index(obj_type, key=key, class_type=type_)
            else:
                # entry exists
                if index.get_index(obj_type)[key] != ["bin"]:  # if "bin" do no further checks
                    if str(type(value)) not in index.get_index(obj_type)[key]:
                        new_type = _optimized_type_check(value)

                        if new_type == "bin":
                            index.set_index(obj_type, key, "bin")
                        else:
                            index.update_index(obj_type, key, new_type)

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            warn("Metadata attribute keys must be strings. Input key has been casted to a string.")
            key = str(key)

        if self.index.type_check:
            ConvoKitMeta._check_type_and_update_index(self.index, self.obj_type, key, value)
        item_type = self.index.get_index(self.obj_type).get(key, None)
        self._get_storage().update_data("meta", self.storage_key, key, value, item_type)

    def __delitem__(self, key):
        if self.obj_type == "corpus":
            self.index.del_from_index(self.obj_type, key)
            self._get_storage().delete_data("meta", self.storage_key, key)
        else:
            if self.index.lock_metadata_deletion[self.obj_type]:
                warn(
                    "For consistency in metadata attributes in Corpus component objects, deleting metadata attributes "
                    "from component objects individually is not allowed. "
                    "To delete this metadata attribute from all Corpus components of this type, "
                    "use corpus.delete_metadata(obj_type='{}', attribute='{}') instead.".format(
                        self.obj_type, key
                    )
                )
            else:
                self._get_storage().delete_data("meta", self.storage_key, key)

    def __iter__(self):
        return self._get_storage().get_data("meta", self.storage_key).__iter__()

    def __len__(self):
        return self._get_storage().get_data("meta", self.storage_key).__len__()

    def __contains__(self, x):
        return self._get_storage().get_data("meta", self.storage_key).__contains__(x)

    def __repr__(self) -> str:
        return "ConvoKitMeta(" + self.to_dict().__repr__() + ")"

    def to_dict(self):
        return dict(self._get_storage().get_data("meta", self.storage_key))

    def reinitialize_from_other(self, other):
        """
        Reinitialize this ConvoKitMeta instance with the data from other
        """
        if type(other) == ConvoKitMeta:
            other = {k: v for k, v in other.to_dict().items()}
        self._get_storage().initialize_data_for_component(
            "meta", self.storage_key, overwrite=True, initial_value=other
        )


_basic_types = {type(0), type(1.0), type("str"), type(True)}  # cannot include lists or dicts


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
