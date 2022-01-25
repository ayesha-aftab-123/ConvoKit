Data Format and Storage Options
===============================

Primary Data Format
-------------------

ConvoKit expects and saves each corpus with the following basic structure, which mirrors closely with the intuitions behind the design of Corpus (see :doc:`architecture`). 

::

 corpus_directory
       |-- utterances.jsonl
       |-- speakers.json
       |-- conversations.json
       |-- corpus.json
       |-- index.json

::

This corpus can be loaded with:

::

 corpus = Corpus(filename="corpus_directory")
::


Note that the end speakers do not need to manually create these files. ConvoKit provides the functionality to dump a Corpus object to save it with the required format:

At a high level, a custom dataset can be converted to a list of utterances (custom_utterance_list), and saved with ConvoKit format for reuse by: 

>>> corpus = Corpus(utterances = custom_utterance_list) 
>>> corpus.dump("custom_dataset", base_path="./") # dump to local directory

A more detailed example of how the `Cornell Movie--Dialogs Corpus <https://www.cs.cornell.edu/~cristian/Chameleons_in_imagined_conversations.html>`_. may be converted from its original release form to ConvoKit format can be found `here <https://github.com/CornellNLP/Cornell-Conversational-Analysis-Toolkit/tree/master/examples/converting_movie_corpus.ipynb>`_.  


Details of component files
--------------------------

utterances.jsonl
^^^^^^^^^^^^^^^^

Each utterance is stored on its own line and represented as a json object, with six mandatory fields:

* id: index of the utterance
* speaker: the speaker who authored the utterance
* conversation_id: id of the first utterance in the conversation this utterance belongs to
* reply_to: index of the utterance to which this utterance replies to (None if the utterance is not a reply)
* timestamp: time of the utterance
* text: textual content of the utterance

Additional information can be added optionally, depending on characteristics of the dataset and intended use, as:

* meta: dictionary of utterance metadata

utterances.jsonl contains a list of such utterances. An example utterance is shown below, drawn from the Supreme Court corpus:

::

 {'id': '200', 'speaker': 'mr. srinivasan', 'conversation_id': '145', 'reply_to': '199', 'timestamp': None, 'text': 'It -- it does.', 'meta': {'case': '02-1472', 'side': 'respondent'}}
::


speakers.json
^^^^^^^^^^^^^

speakers are identified by speaker names. speakers.json keeps a dictionary, where the keys are speaker names, and values are metadata associated with the speakers. Provision of speaker metadata is optional.

An example speaker-metadata pair is shown below, again, drawn from the Supreme Court corpus:

::

'mr. srinivasan': {'is-justice': False, 'gender': 'male'}

::


conversation.json 
^^^^^^^^^^^^^^^^^

Similarly, conversation.json also keeps a dictionary where keys are conversation index, and values are conversational-level metadata (i.e., additional information that stay invariant throughout the conversation). 

An example conversation index-metadata pair is shown below, adapted from the conversations gone awry corpus: 

::

"236755381.13326.13326": {"page_title": "speaker talk: Entropy", "conversation_has_personal_attack": true}

::

Provision of conversational-level metadata is optional. In case no information is provided, the file could simply contain an empty dictionary.  


corpus.json
^^^^^^^^^^^

Metadata of the corpus is saved in corpus.json, as a dictionary where keys are names of the metadata, and values are the actual content of such metadata. 

The contents of the corpus.json file for the Reddit corpus (small) is as follows: 

::

 {"subreddit": "reddit-corpus-small", "num_posts": 8286, "num_comments": 288846, "num_speaker": 119889}

::


index.json 
^^^^^^^^^^

To allow speakers the option of previewing available information in the corpus without loading it entirely, ConvoKit requires an index.json file that contains information about all available metadata and their expected types.

There are five mandatory fields: 

* utterances-index: information of utterance-level metadata
* speakers-index: information of speaker-level metadata
* conversations-index: information of conversation-level metadata
* overall-index: information of corpus-level metadata
* version: version number of the corpus

As an example, the corpus-level metadata for the Reddit corpus (small) is shown below: 

::

"overall-index": {"subreddit": "<class 'str'>", "num_posts": "<class 'int'>", "num_comments": "<class 'int'>", "num_speakers": "<class 'int'>"}
:: 
 

While not necessary, speakers experienced with handling json files can choose to convert their custom datasets directly based on the expected data format specifications.


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
A corpus using DB storage can be initilized with:

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

On the other hand, if a corpus with the id ``'example_id'`` already existed in 
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

To connect directly to an existing version of a corpus (or create an 
empty corpus if there is no existing version), specify ``in_place=True``:  

::

 corpus = Corpus(corpus_id='example_id', storage_type='db', in_place=True)
::

Finally, specify the ``version`` parameter if you know which version
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


