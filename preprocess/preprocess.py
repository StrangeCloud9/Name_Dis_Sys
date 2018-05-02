import os
import pickle

from lib.DatabaseClient import DatabaseClient
from lib.DisData import Paper, Cluster


def add_in_inverted_indices(inverted_indices, paper_idx, feature_uni_id):
    if feature_uni_id not in inverted_indices:
        inverted_indices[feature_uni_id] = list()
    inverted_indices[feature_uni_id].append(paper_idx)  # papers about this unit


def analyze_papers_and_init_clusters(author_name):
    db = DatabaseClient()

    paper_affiliations = db.get_paper_affiliations_by_author_name(author_name)

    if len(paper_affiliations) < 300:
        return None, None, None, None, None
    # elif len(paper_affiliations) > 15000:
    #     f_big = open('./big_name', 'a')
    #     f_big.write(author_name + "\n")
    #     f_big.close()
    #     return None, None, None, None, None

    process_count = 0
    papers = list()
    clusters = dict()
    paper_idx_2_cluster_id = dict()
    inverted_indices = dict()
    author_id_set = set()

    uni_id_generator = 0
    coauthor_2_uni_id = dict()
    affiliation_2_uni_id = dict()
    venue_2_uni_id = dict()

    for i, paper_affiliation in enumerate(paper_affiliations):
        print (i, '/', len(paper_affiliations))

        paper_id = paper_affiliation[0]
        original_author_id = paper_affiliation[2]
        author_id_set.add(original_author_id)

        # get coauthors
        authors = db.get_coauthors_by_paper_id(paper_id)
        if authors is None:
            continue

        paper_idx = process_count

        # coauthors = set()
        coauthors = [x for x in authors if x[0] != author_name]
        for author in authors:
            coauthor_name = author[0]
            if coauthor_name != author_name:
                if coauthor_name not in coauthor_2_uni_id:
                    coauthor_2_uni_id[coauthor_name] = 'a' + str(uni_id_generator)
                    uni_id_generator += 1
                coauthor_uni_id = coauthor_2_uni_id[coauthor_name]
                # coauthors.add(coauthor_uni_id)

                add_in_inverted_indices(inverted_indices, paper_idx, coauthor_uni_id)

        # get affiliation
        affiliation_id = paper_affiliation[1]
        if affiliation_id is not None:
            if affiliation_id not in affiliation_2_uni_id:
                affiliation_2_uni_id[affiliation_id] = 'o' + str(uni_id_generator)
                uni_id_generator += 1
            affiliation_id = affiliation_2_uni_id[affiliation_id]

            add_in_inverted_indices(inverted_indices, paper_idx, affiliation_id)

        # get venue, title and year
        venue_id = None
        title = None
        year = None
        title_venue_year = db.get_title_venue_year_by_paper_id(paper_id)
        if len(title_venue_year) != 0:
            # fill in paper_venue_dict
            if title_venue_year[0][1] is not None:
                venue_id = title_venue_year[0][1]
            elif title_venue_year[0][2] is not None:
                venue_id = title_venue_year[0][2]

            if venue_id is not None:
                if venue_id not in venue_2_uni_id:
                    venue_2_uni_id[venue_id] = 'v' + str(uni_id_generator)
                    uni_id_generator += 1
                venue_id = venue_2_uni_id[venue_id]

                add_in_inverted_indices(inverted_indices, paper_idx, venue_id)

            title = title_venue_year[0][0]
            year = title_venue_year[0][3]

        paper_instance = Paper(paper_id, title, year, venue_id, affiliation_id, coauthors, original_author_id)
        papers.append(paper_instance)

        # initially each paper is used as a cluster
        new_cluster = Cluster(paper_instance, paper_idx, affiliation_id, year)
        clusters[paper_idx] = new_cluster
        paper_idx_2_cluster_id[paper_idx] = paper_idx
        process_count += 1

    if len(clusters) == 0:
        print ("")
        return None, None, None, None, None

    return papers, clusters, paper_idx_2_cluster_id, inverted_indices, author_id_set


def preprocess_author(author_name, papers_out, clusters_out, paper_cluster_mapping_out, invid_out, author_ids_out):
    p, c, pcm, iid, aid = analyze_papers_and_init_clusters(author_name)
    pickle.dump(p, papers_out)
    pickle.dump(c, clusters_out)
    pickle.dump(pcm, paper_cluster_mapping_out)
    pickle.dump(iid, invid_out)
    pickle.dump(aid, author_ids_out)


PREPPED_PAPERS_FNFMT = 'papers_{author}'
PREPPED_CLUSTERS_FNFMT = 'clusters_{author}'
PREPPED_PAPER_INDEX_TO_CLUSTER_ID_FNFMT = 'paper_idx_2_cluster_id_{author}'
PREPPED_INVERTED_INDICES_FNFMT = 'inverted_indices_{author}'
PREPPED_AUTHOR_ID_SET_FNFMT = 'author_id_set_{author}'

def preprocess_author_to_files(author_name, directory):
    try:
        os.mkdir(directory)
    except OSError:
        pass
    preprocess_author(
        author_name,
        open(os.path.join(directory, PREPPED_PAPERS_FNFMT.format(author=author_name)), 'wb'),
        open(os.path.join(directory, PREPPED_CLUSTERS_FNFMT.format(author=author_name)), 'wb'),
        open(os.path.join(directory, PREPPED_PAPER_INDEX_TO_CLUSTER_ID_FNFMT.format(author=author_name)), 'wb'),
        open(os.path.join(directory, PREPPED_INVERTED_INDICES_FNFMT.format(author=author_name)), 'wb'),
        open(os.path.join(directory, PREPPED_AUTHOR_ID_SET_FNFMT.format(author=author_name)), 'wb')
    )

def load_preprocessed_author_local(author_name, directory):
    papers = pickle.load(open(os.path.join(directory, PREPPED_PAPERS_FNFMT.format(author=author_name)), 'rb'))
    clusters = pickle.load(open(os.path.join(directory, PREPPED_CLUSTERS_FNFMT.format(author=author_name)), 'rb'))
    paper_cluster_mapping = pickle.load(
        open(os.path.join(directory, PREPPED_PAPER_INDEX_TO_CLUSTER_ID_FNFMT.format(author=author_name)), 'rb'))
    inverted_ids = pickle.load(
        open(os.path.join(directory, PREPPED_INVERTED_INDICES_FNFMT.format(author=author_name)), 'rb'))
    author_id_set = pickle.load(
        open(os.path.join(directory, PREPPED_AUTHOR_ID_SET_FNFMT.format(author=author_name)), 'rb'))
    return papers, clusters, paper_cluster_mapping, inverted_ids, author_id_set
