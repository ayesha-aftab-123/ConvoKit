Storage Options
=================

Convokit naturally uses in-memory storage. However, we provide an alternative to using a database for storage instead.

Alternative Storage Option: Database Storage
--------------------------------------------

ConvoKit uses the Primary Data Format described above for distributing datasets. 
The Primary Data Format is also the best choice for long term storage of static 
datasets, which can be loaded into memory for active computation. 
On the other hand, Convokit provides an alternative storage option designed to 
support datasets that require both persistant storage and active computation 
over long time periods: database storage.

ConvoKit creates a ``convokit`` database within the MongoDB server specified using the 
``db_host`` option in ~/.convokit/config.yml (see :doc:`db_setup` for instructions
on setting up a MongoDB server for ConvoKit). 
A corpus using DB storage can be initialized with:

::

 corpus = Corpus(corpus_id='example_id', storage_type='db')
::

If there is no corpus with the id ``'example_id'`` in the database, this will 
intilize an empty corpus, which will be stored accross these database collections:

::

 convokit
       |-- example_id_v0_utterances
       |-- example_id_v0_speakers
       |-- example_id_v0_conversations
       |-- example_id_v0_metas
       |-- example_id_v0_speakers

::

To connect directly to an existing version of a corpus (or create an 
empty corpus if there is no existing version), ``in_place=True`` is by default:  

::

 corpus = Corpus(corpus_id='example_id', storage_type='db')
::


On the other hand, if you want to create a copy of corpus with the id ``'example_id'`` already existed in 
the database (assuming the existing corpus is version 0 and stored in the collections listed above)
the contents of the existing database collections will be copied into the following new collections
to create a unique copy of the corpus which can be modified without affecting the original version:

::

 convokit
       |-- example_id_v0.1_utterances
       |-- example_id_v0.1_speakers
       |-- example_id_v0.1_conversations
       |-- example_id_v0.1_metas
       |-- example_id_v0.1_speakers

::

To do so, specify ``in_place=False``:

::

 corpus = Corpus(corpus_id='example_id', storage_type='db', in_place=False)
:: 

To access the particular version, specify ``'version'`` parameter if you know which version
of a corpus you want to connect to (in place, or in the default as a copy): 

::

 corpus = Corpus(corpus_id='example_id', storage_type='db', version='0.1')
::


Details of database collections
-------------------------------

Each database collection contains a database document for each item of the type
suggested by the collection name. These database documents store all the data for 
the cooresponding ConvoKit object. Each time an object is requested from a Corpus, e.g.
``utt = corpus.get_utterance('0')``, a new ConvoKit object of the desired type is 
initialized with a connection to the cooresponding database document. When data is accessed
from the object, e.g. ``txt = utt.text`` the live data is pulled from the database, ensuring 
the most recent and currently accurate version of any attribute is always returned. 
Moreover, when an attribute is modified, e.g. ``utt.text = 'I actually said *this*'``,
the change is automatically written back to the database for persistant storage. 