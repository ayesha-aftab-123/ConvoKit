SPOLIN Corpus
============================

**Selected Pairs of Learnable ImprovisatioN (SPOLIN)** is a collection of more than 68,000 "Yes, and” type utterance pairs extracted from the long-form improvisation podcast *Spontaneanation* by Paul F. Tompkins, the Cornell Movie-Dialogs Corpus, and the SubTle corpus.

Dataset details
---------------

Speaker-level information
^^^^^^^^^^^^^^^^^^^^^^^^^


Utterance-level information
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Every conversation is labeled with its source (Spontaneantion, Cornell Movie-Dialogs Corpus, or the SubTle corpus) and whether it abides by the “Yes, and” principle or not.
The “Yes, and” principle is a rule-of-thumb of improvisational theater that suggests that a participant should accept the reality of what the other participant has said (“Yes”) and expand or refine that reality with additional information ("and").
It does not require the response to explicitly contain the phrase "Yes, and".

Metadata for each utterance:

* split: whether it belongs to the original dataset’s train or validation set
* label: 1 if it is part of a “yes-and” pair or 0 otherwise
* source: whether it comes from Spontaneantion (“spont”), Cornell Movie-Dialogs Corpus (“cornell”), or the SubTle corpus (“subtle”)


Conversational-level information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


Corpus-level information
^^^^^^^^^^^^^^^^^^^^^^^^

The metadata includes a summarized version of the information contained in this documentation.

Usage
-----

To download directly with ConvoKit:

>>> from convokit import Corpus, download
>>> corpus = Corpus(filename=download("spolin-corpus"))


For some quick stats:

>>> corpus.print_summary_stats()
Number of Speakers: 225194
Number of Utterances: 225194
Number of Conversations: 112597


Number of yesands / non-yesands:

* Total: 68,188 / 44,409
* Spontaneanation: 10,959 / 6,087
* Cornell: 16,926 / 18,810
* SubTle: 40,303 / 19,512


Additional notes
----------------

More details about the SPOLIN project can be found on: https://justin-cho.com/spolin

License
^^^^^^^
This dataset is shared under the `Creative Commons Attribution-NonCommercial 4.0 International License <https://creativecommons.org/licenses/by-nc/4.0/>`_.

Contact
^^^^^^^

Please email any questions to Hyundong Justin Cho (jcho@isi.edu), Information Sciences Institute, University of Southern California
