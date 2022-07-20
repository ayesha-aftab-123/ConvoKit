import spacy
import sys

from .transformer import Transformer

class ArcExtractor(Transformer):
    """
    Transformer that annotates each Utterance in a Corpus with dependency parse-based
    sentence-level root token arcs. Because these are parse-based, this requires that
    the Corpus has been previously processed with the Parser transformer.
    """

    @classmethod
    def is_noun_ish(word):
        """
            True if the word is a noun, pronoun or determiner
        """
        return (word.dep in NP_LABELS) or (word.tag_.startswith('NN') or word.tag_.startswith('PRP')) or (word.tag_.endswith('DT'))

    @classmethod
    def has_w_det(token):
        """
            Returns the tokens text if it has a W determiner, False otherwise
        """
        if token.tag_.startswith('W'): return token.text
        first_tok = next(token.subtree)
        if (first_tok.tag_.startswith('W')): return first_tok.text
        return False

    @classmethod
    def get_tok(token):
        """
            TODO
        """
        if ArcExtractor.is_noun_ish(token):
            has_w = ArcExtractor.has_w_det(token)
            if has_w:
                return has_w.lower(), True
            else:
                return 'NN', True
        else:
            return token.text.lower(), False

    @classmethod
    def get_clean_tok(tok):
        """
            Removes dashes from the tokens text
        """
        out_tok, is_noun = ArcExtractor.get_tok(tok)
        return out_tok.replace('--','').strip(), is_noun

    @classmethod
    def get_arcs(root, follow_conj):
        """
            Helper to extract_arcs.
            Returns the arcs in a given question
            follow_conj is whether conjunctions and their children should be included
            in the returned arc or not
        """
        # todo: could imagine version where nouns allowed
        arcs = set()
        root_tok, _ = ArcExtractor.get_clean_tok(root)
        if not MotifsExtractor.is_usable(root_tok): return arcs

        arcs.add(root_tok + '_*')
        conj_elems = []
        for idx, kid in enumerate(root.children):
            if kid.dep_ in ['punct','cc']:
                continue
            elif kid.dep_ == 'conj':
                if follow_conj:
                    conj_elems.append(kid)
            else:
                kid_tok, _ = MotifsExtractor.get_clean_tok(kid)
                if MotifsExtractor.is_usable(kid_tok):
                    arcs.add(root_tok + '_' + kid_tok)

        first_elem = next(root.subtree)
        first_tok, _ = MotifsExtractor.get_clean_tok(first_elem)
        if MotifsExtractor.is_usable(first_tok):
            arcs.add(first_tok + '>*')
            try:
                second_elem = first_elem.nbor()
                second_tok, _ = MotifsExtractor.get_clean_tok(second_elem)
                if MotifsExtractor.is_usable(second_tok):
                    arcs.add(first_tok + '>' + second_tok)
            except:
                pass

        for conj_elem in conj_elems:
            arcs.update(MotifsExtractor.get_arcs(conj_elem, follow_conj))
        return arcs

    def transform(self, corpus):
        for utterance in corpus.iter_utterances():
            arcs = [] # arcs indexed by span
            for span in utterance.meta["parsed"]:
