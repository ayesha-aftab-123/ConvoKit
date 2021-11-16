from collections import MutableMapping
from convokit.util import warn
from convokit.storage import DBDocumentMapping, StorageManager

# See reference: https://stackoverflow.com/questions/7760916/correct-usage-of-a-getter-setter-for-dictionary-values


class ConvoKitMeta(MutableMapping):
    """
    ConvoKitMeta is a dictlike object that stores the metadata attributes of a corpus component
    """
    def __init__(self, obj_type=None, id=None, storage=None, from_db=False):

        self.storage = storage
        self.obj_type = obj_type

        if from_db:
            return
        self.fields = self.storage.ItemMapping(self.storage._metas, id)

    def __getitem__(self, item):
        if item in self.fields:
            return self.fields.__getitem__(item)
        else:
            raise KeyError(item)

    def __setitem__(self, key, value):
        # print(f'META: Setting meta[{key}] to {value}')
        if not isinstance(key, str):
            warn(
                "Metadata attribute keys must be strings. Input key has been casted to a string."
            )
            key = str(key)

        if value is not None:
            if self.storage.index.type_check:
                self.storage.index._check_type_and_update_index(
                    self.obj_type, key, value)
            self.storage.index.update_index(self.obj_type, key,
                                            repr(type(value)))
        # print(f'meta: setting {key} -> {value}')
        # print(f'meta: setting {type(key)} -> {type(value)}')
        self.fields[key] = value

    def __delitem__(self, key):
        if self.obj_type == 'corpus':
            self.fields.__delitem__(key)
            self.storage.index.del_from_index(self.obj_type, key)
        else:
            if self.storage.index.lock_metadata_deletion[self.obj_type]:
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
            theirs = o.fields.dict(with_id=False)
        elif isinstance(o, dict):
            theirs = o
        else:
            return False

        mine = self.fields.dict(with_id=False)
        return theirs == mine

    def __repr__(self):
        return f'{self.fields.collection_mapping.name}.{self.fields.id}: {str(self.fields.dict(with_id=False))}'

    def __str__(self):
        return str(self.fields.dict(with_id=False))

    def update(self, other=(), **kwds):
        ''' D.update([E, ]**F) -> None.  Update D from mapping/iterable E and F.
            If E present and has a .keys() method, does:     for k in E: D[k] = E[k]
            If E present and lacks .keys() method, does:     for (k, v) in E: D[k] = v
            In either case, this is followed by: for k, v in F.items(): D[k] = v
        '''
        if isinstance(other, ConvoKitMeta) or isinstance(other, dict):
            for key in other:
                if key != '_id':
                    self[key] = other[key]
        else:
            super().update(other=other, **kwds)

    @classmethod
    def from_dbdoc(cls, doc: DBDocumentMapping):
        """
        Initilize a corpusComponent object with data contained in the DB document 
        represented by doc. 

        :param cls: class to initilize: Utterance, Conversation, or Speaker
        :param doc: DB document to initilize the corpusComponent from
        :return: the initilized corpusComponent object
        """
        type_id = doc.id.split('_')
        obj_type = type_id[0]

        ret = cls(from_db=True,
                  id=doc.id,
                  obj_type=obj_type,
                  storage=doc.collection_mapping.storage)
        ret.fields = doc
        return ret