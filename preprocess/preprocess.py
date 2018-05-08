import os
import pickle

from lib.DatabaseClient import *
from lib.DisData import *

import sys
def add_in_inverted_indices(inverted_indices, paper_id, feature_uni_id):
    if feature_uni_id not in inverted_indices:
        inverted_indices[feature_uni_id] = list()
    inverted_indices[feature_uni_id].append(paper_id)  # papers about this unit


def analyze_papers_and_init_clusters(author_name):
    db = DatabaseClient()

    paper_affiliations = db.get_paper_affiliations_by_author_name(author_name)


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


#paper_affiliations,authors_dic,title_venue_year_dic
PREPPED_PAPER_AFFI_FNFMT = 'paper_affiliations_{author}'
PREPPED_AUTHOR_DIC_FNFMT = 'author_dic_{author}'
PREPPED_T_V_Y_DIC_FNFMT = 'title_venue_year_dic_{author}'

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
def get_name2aid_from_file(aims,fileName = '/tmp/aname2aid.txt'):
    #id,name
        name2aid = {}
        aid2name = {}
        COUNT = 0

        for line in open(fileName):
            line = line[:-1]
            line = line.split('$$$')
            COUNT+=1
            if COUNT % 1000000 == 0:
                print(len(name2aid))
            if line[1] in aims:
                if(line[1] not in name2aid):
                    name2aid[line[1]] = ""
                name2aid[line[1]]+=line[0]+" "
                aid2name[line[0]] = line[1]
        print('load name2aid done')
        sys.stdout.flush()
        return name2aid,aid2name

def get_aid2pid_affid_from_file(aims,fileName = '/tmp/aid2pid_affid.txt'):
    #aid,affid,pid
    aid2pid_affid = {}
    pid2aid = {}
    COUNT = 0
    for line in open(fileName):
        line = line[:-1]
        line = line.split('\t')
        COUNT+=1
        if COUNT % 1000000 == 0:
            print(len(aid2pid_affid))
        if line[0] in aims:
            if(line[0] not in aid2pid_affid):
                aid2pid_affid[line[0]] = ""
            if(line[2] not in pid2aid):
                pid2aid[line[2]] = ""

            aid2pid_affid[line[0]] += line[1]+" "+line[2]+"$$$"
            pid2aid[line[2]] += line[0]+ " "
    print('load aid2pid_affid done')
    sys.stdout.flush()
    return aid2pid_affid,pid2aid


def get_pid2ptitle_cid_jid_year_from_file(aims,fileName = '/tmp/pid2ptitle_cid_jid_year.txt'):
    pid2ptitle_cid_jid_year = {}
    COUNT = 0
    for line in open(fileName):
        line = line[:-1]
        line = line.split('$$$')
        COUNT+=1
        if COUNT % 1000000 == 0:
            print(len(pid2ptitle_cid_jid_year))
        if line[0] in aims:
            if(line[0] not in pid2ptitle_cid_jid_year):
                pid2ptitle_cid_jid_year[line[0]] = ""
            pid2ptitle_cid_jid_year[line[0]] +=line[1]+"$$$"+line[2]+ "$$$"+ line[3] +"$$$"+line[4]
    

    print('load pid2ptitle_cid_jid_year done')
    sys.stdout.flush()
    return pid2ptitle_cid_jid_year
def batch_dic_extract(author_name,name2aid,aid2name,aid2pid_affid,pid2aid,pid2ptitle_cid_jid_year):
    paper_affiliations = []

    aids = name2aid[author_name][:-1].split(" ")
    for aid in aids:
        pid_affid = aid2pid_affid[aid]
        for item in pid_affid.split('$$$')[:-1]:
            paper_affiliations.append([ item.split(' ')[1],item.split(' ')[0],aid ])
    
    authors_dic = {}# paper_id->authors name
    title_venue_year_dic = {} # paper_id ->title_venue_year

    for paper_affiliation in paper_affiliations:
        paper_id = paper_affiliation[0]

        authors_dic[paper_id] = []

        aids = pid2aid[paper_id].split(' ')[:-1]
        for aid in aids:
            authors_dic[paper_id].append(aid2name[aid])

        title_venue_year_dic[paper_id] = [pid2ptitle_cid_jid_year[paper_id].split('$$$')]

    return paper_affiliations,authors_dic,title_venue_year_dic


def batch_analyze_papers_and_init_clusters(author_name,paper_affiliations,authors_dic,title_venue_year_dic):
    
    #name - >PaperID,AffiliationID,AuthorID

    #------paper_affiliations = db.get_paper_affiliations_by_author_name(author_name)
    

    # elif len(paper_affiliations) > 15000:
    #     f_big = open('./big_name', 'a')
    #     f_big.write(author_name + "\n")
    #     f_big.close()
    #     return None, None, None, None, None

    process_count = 0
    papers = list()
    clusters = dict()
    paper_id_2_cluster_id = dict()
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
        authors = authors_dic[paper_id]


        #-----authors = db.get_coauthors_by_paper_id(paper_id)
        if authors is None:
            continue

        paper_idx = process_count

        # coauthors = set()
        coauthors = [x for x in authors if x[0] != author_name]
        for author in authors:
            coauthor_name = author[0]
            if coauthor_name != author_name:
                if coauthor_name not in coauthor_2_uni_id:
                    coauthor_2_uni_id[coauthor_name] = 'a' + str(coauthor_name)
                coauthor_uni_id = coauthor_2_uni_id[coauthor_name]
                # coauthors.add(coauthor_uni_id)

                add_in_inverted_indices(inverted_indices, paper_idx, coauthor_uni_id)

        # get affiliation
        affiliation_id = paper_affiliation[1]
        if affiliation_id is not None:
            if affiliation_id not in affiliation_2_uni_id:
                affiliation_2_uni_id[affiliation_id] = 'o' + str(affiliation_id)
            affiliation_id = affiliation_2_uni_id[affiliation_id]

            add_in_inverted_indices(inverted_indices, paper_idx, affiliation_id)

        # get venue, title and year
        venue_id = None
        title = None
        year = None
        
        title_venue_year = title_venue_year_dic[paper_id]

         #-----db.get_title_venue_year_by_paper_id(paper_id)


        if len(title_venue_year) != 0:
            # fill in paper_venue_dict
            if title_venue_year[0][1] is not None:
                venue_id = title_venue_year[0][1]
            elif title_venue_year[0][2] is not None:
                venue_id = title_venue_year[0][2]

            if venue_id is not None:
                if venue_id not in venue_2_uni_id:
                    venue_2_uni_id[venue_id] = 'v' + str(venue_id)
                venue_id = venue_2_uni_id[venue_id]

                add_in_inverted_indices(inverted_indices, paper_idx, venue_id)

            title = title_venue_year[0][0]
            year = title_venue_year[0][3]

        paper_instance = Paper(paper_id, title, year, venue_id, affiliation_id, coauthors, original_author_id,paper_idx)
        papers.append(paper_instance)

        # initially each paper is used as a cluster
        new_cluster = Cluster(paper_instance, process_count, affiliation_id, year)
        clusters[process_count] = new_cluster
        paper_idx_2_cluster_id[paper_idx] = process_count
        process_count += 1

    if len(clusters) == 0:
        print ("")
        return None, None, None, None, None

    return papers, clusters, paper_idx_2_cluster_id, inverted_indices, author_id_set
#PREPPED_PAPERS_FNFMT = 'papers_{author}'
#PREPPED_CLUSTERS_FNFMT = 'clusters_{author}'
#PREPPED_PAPER_INDEX_TO_CLUSTER_ID_FNFMT = 'paper_idx_2_cluster_id_{author}'
#PREPPED_INVERTED_INDICES_FNFMT = 'inverted_indices_{author}'
#PREPPED_AUTHOR_ID_SET_FNFMT = 'author_id_set_{author}'


#paper_affiliations,authors_dic,title_venue_year_dic
#PREPPED_PAPER_AFFI_FNFMT = 'paper_affiliations_{author}'
#PREPPED_AUTHOR_DIC_FNFMT = 'author_dic_{author}'
#PREPPED_T_V_Y_DIC_FNFMT = 'title_venue_year_dic_{author}'

def batch_preprocess(author_names,directory):
    if(os.path.exists(directory) == False):
        os.mkdir(directory)

    db = DatabaseClient()
    db.get_aname2aid_from_db()
    db.get_aid2pid_affid_from_db()
    db.get_pid2ptitle_cid_jid_year()

    name2aid,aid2name = get_name2aid_from_file(aims = author_names)
    aid2pid_affid,pid2aid = get_aid2pid_affid_from_file(aims = aid2name)
    pid2ptitle_cid_jid_year = get_pid2ptitle_cid_jid_year_from_file(aims = pid2aid)

    for author_name in author_names:
        paper_affiliations,authors_dic,title_venue_year_dic = batch_dic_extract(author_name,name2aid,aid2name,aid2pid_affid,pid2aid,pid2ptitle_cid_jid_year)
        papers, clusters, paper_idx_2_cluster_id, inverted_indices, author_id_set = batch_analyze_papers_and_init_clusters(author_name,paper_affiliations,authors_dic,title_venue_year_dic)

        pickle.dump(papers, open(os.path.join(directory,PREPPED_PAPERS_FNFMT.format(author=author_name)),'wb')   )
        pickle.dump(clusters, open(os.path.join(directory,PREPPED_CLUSTERS_FNFMT.format(author=author_name)),'wb')   )
        pickle.dump(paper_idx_2_cluster_id, open(os.path.join(directory,PREPPED_PAPER_INDEX_TO_CLUSTER_ID_FNFMT.format(author=author_name)),'wb')   )
        pickle.dump(inverted_indices, open(os.path.join(directory,PREPPED_INVERTED_INDICES_FNFMT.format(author=author_name)),'wb')   )
        pickle.dump(author_id_set, open(os.path.join(directory,PREPPED_AUTHOR_ID_SET_FNFMT.format(author=author_name)),'wb')   )
        
        pickle.dump(paper_affiliations, open(os.path.join(directory,PREPPED_PAPER_AFFI_FNFMT.format(author=author_name)),'wb')   )
        pickle.dump(authors_dic, open(os.path.join(directory,PREPPED_AUTHOR_DIC_FNFMT.format(author=author_name)),'wb')   )
        pickle.dump(title_venue_year_dic, open(os.path.join(directory,PREPPED_T_V_Y_DIC_FNFMT.format(author=author_name)),'wb')   )
  