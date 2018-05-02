import numpy as np
import spacy

nlp = spacy.load('en_core_web_md')


stopwords = set()
for line in open('data/stopwords_ace.txt'):
    stopwords.add(line.replace('\n', '').replace('\r', ''))

class Paper:
    def __init__(self, paper_id, title, year, venue_id, affiliation_id, coauthors, author_id):
        self.paper_id = paper_id
        self.title = title
        self.year = year
        self.venue_id = venue_id
        self.affiliation_id = affiliation_id
        self.coauthors = coauthors
        self.author_id = author_id

        title_nlp = nlp(title)

        title_vector_sum = np.zeros(title_nlp[0].vector.shape)
        word_count = 0
        self.title_vector = np.zeros(title_nlp[0].vector.shape)

        for word in title_nlp:
            if str(word) not in stopwords and len(str(word)) > 1:
                title_vector_sum += word.vector
                word_count += 1
        if word_count != 0:
            self.title_vector = title_vector_sum / word_count
        
class Cluster:
    def __init__(self, paper, paper_idx, affiliation_id, year):
        # paper = papers[idx]
        self.author_id = None
        self.papers = list()
        self.papers.append(paper)
        self.paper_idx_list = list()
        self.paper_idx_list.append(paper_idx)
        self.cluster_id = paper_idx
        self.affiliations = set()
        self.affiliations.add(affiliation_id)
        self.year_2_affiliations = dict()
        if affiliation_id is not None and year is not None:
            self.year_2_affiliations[year] = set()
            self.year_2_affiliations[year].add(affiliation_id)

        self.link_type_2_ngbrs = dict()
        self.ngbrs = set()

    def unit(self, other, paper_idx_2_cluster_id):
        for paper_idx in other.paper_idx_list:
            paper_idx_2_cluster_id[paper_idx] = self.cluster_id
        self.papers.extend(other.papers)
        self.paper_idx_list.extend(other.paper_idx_list)
        self.affiliations |= other.affiliations

        for k, v in other.year_2_affiliations.items():
            if k in self.year_2_affiliations.keys():
                self.year_2_affiliations[k] |= v
            else:
                self.year_2_affiliations[k] = v

    def has_no_conflict(self, other, paper_final_edges, strict_mode):
        connected_edges = 0
        for paper_idx in other.paper_idx_list:
            connected_edges += len(np.nonzero(paper_final_edges[paper_idx, self.paper_idx_list])[0])

        if strict_mode and float(connected_edges) < 0.01 * (len(self.papers) * len(other.papers)):
            return False

        if len(self.affiliations | other.affiliations) > 20:
            return False

        for k, v in self.year_2_affiliations.items():
            if k in other.year_2_affiliations.keys():
                if len(v | other.year_2_affiliations[k]) > 3:
                    return False

        return True
