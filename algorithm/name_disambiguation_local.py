# -*- coding: UTF-8 -*-

import datetime

import numpy as np
import os
import _pickle as cpickle

from preprocess import preprocess

'''
stopwords = set()
for line in open('./stopwords_ace.txt'):
    stopwords.add(line.replace('\n', '').replace('\r', ''))
# print stopwords
'''
'''
nlp = spacy.load('en_core_web_md')
print ('spacy.load finished')
'''

'''
conn = MySQLdb.connect(host='202.120.36.29', port=3306, user='groupleader', passwd='onlyleaders', db='mag-new-160205',
                       charset="utf8")
cursor = conn.cursor()
'''
f = open('./disambiguation_finished_authors', 'a')




def compute_title_similarity(paper_A, paper_B):
    vector_A = paper_A.title_vector
    vector_B = paper_B.title_vector

    if len(np.nonzero(vector_A)[0]) == 0 or len(np.nonzero(vector_B)[0]) == 0:
        return 0
    cos_sim = np.dot(vector_A, vector_B) / (np.linalg.norm(vector_A) * np.linalg.norm(vector_B))
    return cos_sim


def link_type_2_weight(link_type, except_type):
    if link_type == 's':  # strong connection
        return 0

    if link_type[-1] == 'w' and link_type[:-1] == except_type:
        # the same type as 'except_type' but with strong content relevancy
        if link_type[0] == 'a':  # co-author
            return 4
        elif link_type[0] == 'o':  # co-affiliation
            return 5
        elif link_type[0] == 'v':  # co-venue
            return 6
    else:
        if link_type[0] == 'a':  # co-author
            return 1
        elif link_type[0] == 'o':  # co-affiliation
            return 2
        elif link_type[0] == 'v':  # co-venue
            return 3


def init_paper_edges_and_ngbrs(papers, inverted_indices):
    paper_count = len(papers)

    title_sim_matrix = np.zeros((paper_count, paper_count))
    paper_full_edges = [[0 for col in range(paper_count)] for row in range(paper_count)]
    paper_all_ngbrs = [set() for i in range(paper_count)]
    paper_weak_type_ngbrs = [dict() for i in range(paper_count)]
    cluster_merge_pairs = list()

    paper_weak_edges = set()
    paper_strong_edges = set()
    paper_tmp_edges = [[0 for col in range(paper_count)] for row in range(paper_count)]

    for i in range(paper_count):#improve: compute it when use it; title_sim could be replaced by a dict()
        for j in range(i + 1, paper_count):
            title_sim = compute_title_similarity(papers[i], papers[j])
            title_sim_matrix[i, j] = title_sim
            title_sim_matrix[j, i] = title_sim

    for link_type, paper_list in inverted_indices.items():
        for i in range(len(paper_list)):
            paper_i = paper_list[i]
            for j in range(i + 1, len(paper_list)):
                paper_j = paper_list[j]

                if paper_tmp_edges[paper_i][paper_j] == 0:
                    paper_tmp_edges[paper_i][paper_j] = link_type
                    paper_tmp_edges[paper_j][paper_i] = link_type
                    paper_weak_edges.add((paper_i, paper_j))
                    paper_full_edges[paper_i][paper_j] = link_type
                    paper_full_edges[paper_j][paper_i] = link_type
                    paper_all_ngbrs[paper_i].add(paper_j)
                    paper_all_ngbrs[paper_j].add(paper_i)

                elif paper_tmp_edges[paper_i][paper_j] != -1:  # strong connection
                    cluster_merge_pairs.append(set((paper_i, paper_j)))
                    paper_strong_edges.add((paper_i, paper_j))
                    paper_tmp_edges[paper_i][paper_j] = -1
                    paper_tmp_edges[paper_j][paper_i] = -1
                    paper_full_edges[paper_i][paper_j] = 's'
                    paper_full_edges[paper_j][paper_i] = 's'

    paper_weak_edges = paper_weak_edges - paper_strong_edges
    for weak_edge in paper_weak_edges:
        i = weak_edge[0]
        j = weak_edge[1]
        link_type = paper_full_edges[i][j]

        if link_type not in paper_weak_type_ngbrs[i].keys():
            paper_weak_type_ngbrs[i][link_type] = set()
        paper_weak_type_ngbrs[i][link_type].add(j)
        if title_sim_matrix[i, j] > 0.7:
            paper_full_edges[i][j] = link_type + 'w'
            paper_full_edges[j][i] = link_type + 'w'

    return paper_full_edges, paper_all_ngbrs, paper_weak_type_ngbrs, \
           cluster_merge_pairs, title_sim_matrix

def merge_strong_connected_papers(clusters, paper_idx_2_cluster_id, cluster_merge_pairs):
    has_changed = True
    while has_changed:
        has_changed = False
        pair_num = len(cluster_merge_pairs)
        i = 0
        while i < pair_num:
            j = i + 1
            while j < pair_num:
                if len(cluster_merge_pairs[i] & cluster_merge_pairs[j]):
                    cluster_merge_pairs[i] = cluster_merge_pairs[i] | cluster_merge_pairs[j]
                    cluster_merge_pairs.remove(cluster_merge_pairs[j])
                    pair_num -= 1
                    j = i + 1
                    has_changed = True
                else:
                    j += 1
            i += 1

    for merge in cluster_merge_pairs:
        A = merge.pop()
        for B in merge:
            clusters[A].unit(clusters[B], paper_idx_2_cluster_id)
            del clusters[B]
    return clusters


def generate_cluster_edges(clusters, papers, paper_full_edges, paper_weak_type_ngbrs, paper_idx_2_cluster_id):
    # sort cluster by number of papers
    sorted_clusters = sorted(clusters.items(), key=lambda d: len(d[1].papers), reverse=True)

    # change clusters' type(dict) to list
    clusters = list()
    for c in range(len(sorted_clusters)):
        cluster = sorted_clusters[c][1]
        cluster.cluster_id = c
        for paper_idx in cluster.paper_idx_list:
            paper_idx_2_cluster_id[paper_idx] = c
        clusters.append(cluster)

    # initialize cluster edges
    cluster_initial_edges = [[set() for col in range(len(clusters))] for row in range(len(clusters))]

    for i in range(len(papers)):
        cluster_i = paper_idx_2_cluster_id[i]
        for i_link_type, i_ngbrs in paper_weak_type_ngbrs[i].items():
            papers_in_same_cluster = set(clusters[paper_idx_2_cluster_id[i]].paper_idx_list)  # including itself
            i_ngbrs -= papers_in_same_cluster
            if len(i_ngbrs) == 0:
                continue

            for j in i_ngbrs:
                cluster_j = paper_idx_2_cluster_id[j]
                cluster_initial_edges[cluster_i][cluster_j].add(paper_full_edges[i][j])
                cluster_initial_edges[cluster_j][cluster_i].add(paper_full_edges[i][j])
                clusters[cluster_i].ngbrs.add(cluster_j)
                clusters[cluster_j].ngbrs.add(cluster_i)

    # generate clusters' link_type_2_ngbrs
    #improve: merge those in one step
    for i in range(len(clusters)):
        for j in range(i + 1, len(clusters)):#improve: i.ngbrs
            if len(cluster_initial_edges[i][j]) != 0:
                for link_type in cluster_initial_edges[i][j]:
                    if link_type[-1] == 'w':
                        link_type = link_type[:-1]
                    if link_type not in clusters[i].link_type_2_ngbrs:
                        clusters[i].link_type_2_ngbrs[link_type] = set()
                    clusters[i].link_type_2_ngbrs[link_type].add(j)

    cluster_final_edges = [[dict() for col in range(len(clusters))] for row in range(len(clusters))]

    INFINITY = 9999



    for i in range(len(clusters)):

        for i_link_type, i_ngbrs in clusters[i].link_type_2_ngbrs.items():

            # Dijkstra's algorithm
            #improve: use dfs or floyd. in small graph dfs is effective
            S = set()  # visited
            Q = set(range(len(clusters)))  # unvisited

            dist = np.array([INFINITY] * len(papers))
            dist_final = np.array([INFINITY] * len(papers))

            S.add(i)
            dist[i] = 0

            while len(Q) != 0:

                u = dist.argmin()

                if dist[u] > 18:
                    break

                Q.remove(u)
                S.add(u)
                dist_final[u] = dist[u]

                if len(i_ngbrs - S) == 0:
                    break

                for v in clusters[u].ngbrs:
                    if v in Q:
                        uv_link_types = cluster_initial_edges[u][v]
                        for uv_link_type in uv_link_types:
                            if uv_link_type == i_link_type:
                                continue
                            alt = dist[u] + link_type_2_weight(uv_link_type, i_link_type)#improve: change weight
                            if alt < dist[v]:
                                dist[v] = alt
                dist[u] = INFINITY

            for i_ngbr in i_ngbrs:
                if dist_final[i_ngbr] != INFINITY and dist_final[i_ngbr] != 0:
                    weight = 1.0 / dist_final[i_ngbr]
                    cluster_final_edges[i][i_ngbr][i_link_type] = weight
                    cluster_final_edges[i_ngbr][i][i_link_type] = weight
                    # cluster_similarity_dict[tuple((i, i_ngbr, i_link_type))] = weight

    # change 'cluster'(list) to dict
    tmp_clusters = dict()
    for i in range(len(clusters)):
        tmp_clusters[i] = clusters[i]
    clusters = tmp_clusters

    return clusters, cluster_final_edges


def generate_paper_similarity_dict(papers, paper_idx_2_cluster_id, paper_weak_type_ngbrs, cluster_edges):
    paper_similarity_dict = dict()

    paper_final_edges = np.zeros((len(papers), len(papers)))

    for i in range(len(papers)):

        cluster_i_id = paper_idx_2_cluster_id[i]

        for i_link_type, i_ngbrs in paper_weak_type_ngbrs[i].items():
            for j in i_ngbrs:
                cluster_j_id = paper_idx_2_cluster_id[j]
                if cluster_i_id == cluster_j_id:
                    continue
                if i_link_type in cluster_edges[cluster_i_id][cluster_j_id]:
                    weight = cluster_edges[cluster_i_id][cluster_j_id][i_link_type]
                    paper_final_edges[i, j] = weight
                    paper_final_edges[j, i] = weight
                    paper_similarity_dict[tuple((i, j))] = weight
    return paper_similarity_dict, paper_final_edges


def hierarchical_clustering(paper_similarity_dict, paper_final_edges, clusters, paper_idx_2_cluster_id):
    sorted_similarity_pairs = sorted(paper_similarity_dict.items(), key=lambda d: d[1], reverse=True)

    for pair in sorted_similarity_pairs:
        paper_A_idx = pair[0][0]
        paper_B_idx = pair[0][1]
        # similarity = pair[1]

        cluster_A_id = paper_idx_2_cluster_id[paper_A_idx]
        cluster_B_id = paper_idx_2_cluster_id[paper_B_idx]
        if cluster_A_id == cluster_B_id:
            continue

        cluster_A = clusters[cluster_A_id]
        cluster_B = clusters[cluster_B_id]

        if cluster_A != cluster_B:
            cluster_small = cluster_A
            cluster_big = cluster_B
            cluster_small_id = cluster_A_id
            if len(cluster_A.papers) > len(cluster_B.papers):
                cluster_small = cluster_B
                cluster_big = cluster_A
                cluster_small_id = cluster_B_id

            if cluster_big.has_no_conflict(cluster_small, paper_final_edges, True):
                cluster_big.unit(cluster_small, paper_idx_2_cluster_id)
                del clusters[cluster_small_id]
    return clusters


def merge_scattered_papers(clusters, paper_idx_2_cluster_id, title_sim_matrix, paper_all_ngbrs, paper_final_edges):
    cluster_merge_pairs = list()
    for cluster_id, cluster in clusters.items():
        if len(cluster.papers) == 1:
            paper_idx = cluster.paper_idx_list[0]
            top_indices = np.argsort(-title_sim_matrix[paper_idx, :])
            for i in top_indices:
                if title_sim_matrix[paper_idx, i] > 0.8 or (
                                title_sim_matrix[paper_idx, i] > 0.6 and i in paper_all_ngbrs[paper_idx]):
                    cluster_small_id = paper_idx_2_cluster_id[paper_idx]
                    cluster_big_id = paper_idx_2_cluster_id[i]
                    if len(clusters[cluster_big_id].papers) > 5 \
                            and clusters[cluster_big_id].has_no_conflict(clusters[cluster_small_id],
                                                                         paper_final_edges,
                                                                         False):
                        cluster_merge_pairs.append((cluster_small_id, cluster_big_id))
                        break

    for merge in cluster_merge_pairs:
        clusters[merge[1]].unit(clusters[merge[0]], paper_idx_2_cluster_id)
        del clusters[merge[0]]

    return clusters


def clustering(author_name, save_path):
    if len(author_name.split()) < 2:
        return 0, None, None, 0, 0
    #print ('start...',author_name)
    starttime = datetime.datetime.now()

    # analyze papers and initialize clusters
    papers, clusters, paper_idx_2_cluster_id, inverted_indices, author_id_set = \
        preprocess.load_preprocessed_author_local(author_name, save_path)

    db_endtime = datetime.datetime.now()

    if papers is None:
        return 0, None, None, 0, 0

    '''
    global conn
    global cursor
    cursor.close()
    conn.close()
'''

    # initialize papers' edges and ngbrs
    paper_full_edges, paper_all_ngbrs, paper_weak_type_ngbrs, \
    cluster_merge_pairs, title_sim_matrix = init_paper_edges_and_ngbrs(papers, inverted_indices)

    # merge strong connected papers
    clusters = merge_strong_connected_papers(clusters, paper_idx_2_cluster_id, cluster_merge_pairs)

    # generate cluster edges
    clusters, cluster_edges = generate_cluster_edges(clusters, papers, paper_full_edges, paper_weak_type_ngbrs,
                                                     paper_idx_2_cluster_id)

    # generate paper similarity dict
    paper_similarity_dict, paper_final_edges \
        = generate_paper_similarity_dict(papers, paper_idx_2_cluster_id, paper_weak_type_ngbrs, cluster_edges)
    clusters = hierarchical_clustering(paper_similarity_dict, paper_final_edges, clusters, paper_idx_2_cluster_id)

    # merge scattered papers
    clusters = merge_scattered_papers(clusters, paper_idx_2_cluster_id, title_sim_matrix, paper_all_ngbrs,
                                      paper_final_edges)

    cl_endtime = datetime.datetime.now()

    return len(papers), clusters, author_id_set, (db_endtime - starttime).seconds / 60.0, (
    cl_endtime - db_endtime).seconds / 60.0

def name_disambiguation_local(author_name, save_path,logger):
    paper_count, clusters, author_id_set, db_time, cl_time = clustering(author_name,save_path)
    
    cpickle.dump(clusters,open(os.path.join(save_path,"result_cluster_%s"%(author_name)),'wb'))
    #logger.info("saved in "+os.path.join(save_path,"result_cluster_%s"%(author_name)))



def generate_new_id(id_gen, process_id):
    # global id_gen
    id_gen = hex(int(id_gen, 16) + 1).replace('0x', '').upper()
    new_id = process_id + id_gen[1:]
    return new_id, id_gen
