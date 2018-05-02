import cProfile
import os
from lxml import html
import re
import pymysql as MySQLdb
#import spacy
import numpy as np
import datetime
import math
from algorithm.name_disambiguation_local import *
from lib.DatabaseClient import *
from lib.DisData import *
from preprocess import *
from lib.Logger import *

conn = MySQLdb.connect(host='202.120.36.29', port=3306, user='groupleader', passwd='onlyleaders', db='mag-new-160205',
                       charset="utf8")
cursor = conn.cursor()

def add_in_inverted_indices(inverted_indices, paper_idx, feature_uni_id):
    if feature_uni_id not in inverted_indices:
        inverted_indices[feature_uni_id] = list()
    inverted_indices[feature_uni_id].append(paper_idx)

def analyze_papers_and_init_clusters(file):
    author_name = file.split('/')[-1].replace('.xml', '')
    author_name = author_name.lower()
    author_name = re.sub('[^A-Za-z0-9]', ' ', author_name)
    author_name = re.sub('\s{2,}', ' ', author_name)

    print (author_name,)

    tree = html.parse(file)
    root = tree.getroot()  # 获取根节点

    process_count = 0
    papers = list()
    clusters = dict()
    paper_idx_2_cluster_id = dict()
    inverted_indices = dict()

    uni_id_generator = 0
    coauthor_2_uni_id = dict()
    affiliation_2_uni_id = dict()
    venue_2_uni_id = dict()

    # # remove authors who have only one paper
    # label_2_papers = dict()
    # for node in root.xpath('//publication'):
    #     label = node.xpath('label')[0].text
    #     paper_id = node.xpath('id')[0].text

    #     if label not in label_2_papers:
    #         label_2_papers[label] = set()
    #     label_2_papers[label].add(paper_id)

    # papers_reserved = set()
    # for label, label_papers in label_2_papers.items():
    #     if len(label_papers) > 1:
    #         for paper_id in label_papers:
    #             papers_reserved.add(paper_id)


    for node in root.xpath('//publication'):
        original_paper_id = node.xpath('id')[0].text
        # if original_paper_id not in papers_reserved:
        #     continue

        label = node.xpath('label')[0].text
        title = node.xpath('title')[0].text

        title = title.lower()
        if title[-1] == '.':
            title = title[:-1]
        title = re.sub('[^A-Za-z0-9]', ' ', title)
        title = re.sub('\s{2,}', ' ', title)
        quest_paper_by_title = 'SELECT PaperID FROM Papers WHERE NormalizedPaperTitle="%s"'
        cursor.execute(quest_paper_by_title % title)
        ps = cursor.fetchall()

        paper_ids = list()
        if len(ps) == 1:
            paper_ids.append(ps[0][0])
        if len(ps) >= 2:
            for p in ps:
                quest_author_by_paper = 'SELECT AuthorName FROM Authors INNER JOIN' \
                                        '   (SELECT AuthorID FROM PaperAuthorAffiliations AS PAA  WHERE PaperID="%s") AS TB2' \
                                        '   ON Authors.AuthorID = TB2.AuthorID'
                cursor.execute(quest_author_by_paper % p[0])
                authors = cursor.fetchall()
                for author in authors:
                    if author[0] == author_name.lower():
                        paper_ids.append(p[0])

        for paper_id in paper_ids:
            paper_idx = process_count

            # get affiliation and coauthors
            quest_affiliation = 'SELECT AuthorName,AffiliationID FROM Authors INNER JOIN' \
                                '   (SELECT AuthorID,AffiliationID FROM PaperAuthorAffiliations WHERE PaperID="%s") AS TB ' \
                                'ON Authors.AuthorID = TB.AuthorID'
            cursor.execute(quest_affiliation % paper_id)
            author_affiliations = cursor.fetchall()

            himself = None
            for ai in range(len(author_affiliations)):
                if author_affiliations[ai][0] == author_name.lower():
                    himself = ai
                    break

            if himself is None:
                tmp1 = author_name.split()
                count = 0
                for ai in range(len(author_affiliations)):
                    tmp2 = author_affiliations[ai][0].split()
                    if tmp1[-1] == tmp2[-1] and tmp1[0][0] == tmp2[0][0]:
                        himself = ai
                        break
                    elif tmp1[-1] == tmp2[0] and tmp1[0][0] == tmp2[-1][0]:
                        himself = ai
                        break

            # get affiliation
            if himself is None:
                #                 affiliation_id = author_affiliations[-1][1]
                affiliation_id = None
            else:
                affiliation_id = author_affiliations[himself][1]
            if affiliation_id is not None:
                if affiliation_id not in affiliation_2_uni_id:
                    affiliation_2_uni_id[affiliation_id] = 'o' + str(uni_id_generator)
                    uni_id_generator += 1
                affiliation_id = affiliation_2_uni_id[affiliation_id]

                add_in_inverted_indices(inverted_indices, paper_idx, affiliation_id)

            # get coauthors
            coauthors = set()
            for ai in range(len(author_affiliations)):
                if ai != himself:
                    coauthor_name = author_affiliations[ai][0]
                    if coauthor_name not in coauthor_2_uni_id:
                        coauthor_2_uni_id[coauthor_name] = 'a' + str(uni_id_generator)
                        uni_id_generator += 1
                    # coauthors.add(coauthor_2_uni_id[coauthor_name])
                    add_in_inverted_indices(inverted_indices, paper_idx, coauthor_2_uni_id[coauthor_name])

            # get venue, title and year
            venue_id = None
            year = None
            quest_info_by_paper = 'SELECT NormalizedPaperTitle, ConferenceSeriesIDMappedToVenueName, ' \
                                  'JournalIDMappedToVenueName, PaperPublishYear FROM Papers WHERE PaperID = "%s"'
            cursor.execute(quest_info_by_paper % paper_id)
            rs = cursor.fetchall()
            if len(rs) != 0:
                # fill in paper_venue_dict
                if rs[0][1] is not None:
                    venue_id = rs[0][1]
                elif rs[0][2] is not None:
                    venue_id = rs[0][2]

                if venue_id is not None:
                    if venue_id not in venue_2_uni_id:
                        venue_2_uni_id[venue_id] = 'v' + str(uni_id_generator)
                        uni_id_generator += 1
                    venue_id = venue_2_uni_id[venue_id]
                    add_in_inverted_indices(inverted_indices, paper_idx, venue_id)

                year = rs[0][3]

            paper_instance = Paper(paper_id, title, label)
            papers.append(paper_instance)

            # initially each paper is used as a cluster
            new_cluster = Cluster(paper_instance, paper_idx, affiliation_id, year)
            clusters[paper_idx] = new_cluster
            paper_idx_2_cluster_id[paper_idx] = paper_idx
            process_count += 1

    print ('\t' + str(len(papers)),)
    return papers, clusters, paper_idx_2_cluster_id, inverted_indices


def name_disambiguation(file):
    # analyze papers and initialize clusters
    papers, clusters, paper_idx_2_cluster_id, inverted_indices = analyze_papers_and_init_clusters(file)

    # initialize papers' edges and ngbrs
    paper_full_edges, paper_all_ngbrs, paper_weak_type_ngbrs, \
    cluster_merge_pairs, title_sim_matrix = init_paper_edges_and_ngbrs(papers, inverted_indices)

    # merge strong connected papers
    clusters = merge_strong_connected_papers(clusters, paper_idx_2_cluster_id, cluster_merge_pairs)

    # generate cluster edges
    clusters, cluster_edges = generate_cluster_edges(clusters, papers, paper_full_edges, paper_weak_type_ngbrs, paper_idx_2_cluster_id)

    # generate paper similarity dict
    paper_similarity_dict, paper_final_edges \
        = generate_paper_similarity_dict(papers, paper_idx_2_cluster_id, paper_weak_type_ngbrs, cluster_edges)

    # hierarchical clustering
    clusters = hierarchical_clustering(paper_similarity_dict, paper_final_edges, clusters, paper_idx_2_cluster_id)

    # merge scattered papers
    clusters = merge_scattered_papers(clusters, paper_idx_2_cluster_id, title_sim_matrix, paper_all_ngbrs,
                                      paper_final_edges)

    return papers, clusters
def get_file_list(dir, file_list):
    newDir = dir
    if os.path.isfile(dir):
        file_list.append(dir)#.decode('gbk'))
    elif os.path.isdir(dir):
        for s in os.listdir(dir):
            # if s == "xxx":
            # continue
            newDir = os.path.join(dir, s)
            get_file_list(newDir, file_list)
    return file_list
def test(file):    
    total_papers_count = 0
    avg_pairwise_precision = 0.0
    avg_pairwise_recall = 0.0
    avg_pairwise_f1 = 0.0
    papers, clusters = name_disambiguation(file)
    print ("clustering over")
    total_papers_count += len(papers)

    cluster_id = 0
    for cluster_id, cluster in clusters.items():
        for paper in cluster.papers:
            paper.label_predicted = cluster_id
        cluster_id += 1

    TP = 0.0  # Pairs Correctly Predicted To SameAuthor
    TP_FP = 0.0  # Total Pairs Predicted To SameAuthor
    TP_FN = 0.0  # Total Pairs To SameAuthor

    for i in range(len(papers)):
        for j in range(i + 1, len(papers)):
            if papers[i].label == papers[j].label:
                TP_FN += 1
            if papers[i].label_predicted == papers[j].label_predicted:
                TP_FP += 1
            if (papers[i].label == papers[j].label) \
                    and (papers[i].label_predicted == papers[j].label_predicted):
                TP += 1
    if TP_FP == 0:
        pairwise_precision = 0
    else:
        pairwise_precision = TP / TP_FP
    pairwise_recall = TP / TP_FN
    if pairwise_precision + pairwise_recall == 0:
        pairwise_f1 = 0
    else:
        pairwise_f1 = (2 * pairwise_precision * pairwise_recall) / (pairwise_precision + pairwise_recall)
    avg_pairwise_precision += pairwise_precision
    avg_pairwise_recall += pairwise_recall
    avg_pairwise_f1 += pairwise_f1
    print ("print result now")
    print ('\t %f' % pairwise_precision,)
    print ('\t %f' % pairwise_recall,)
    print ('\t %f' % pairwise_f1)



def name_disambiguation_test(folder):
    file_list = get_file_list(folder, [])

    total_papers_count = 0
    avg_pairwise_precision = 0.0
    avg_pairwise_recall = 0.0
    avg_pairwise_f1 = 0.0
    #     file_list = ['./dataset/J. Guo.xml']
    # print file_list
    for file in file_list[1:]:
        test(file)

        