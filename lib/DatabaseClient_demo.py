from DatabaseClient import *

if __name__ == '__main__':

    d=DatabaseClient(host = '202.120.36.29',port = 3306, user = 'readonly', password = 'readonly', database = 'mag-new-160205')
    ret = d.get_paper_affiliations_by_author_name('xueqing wang')
    print (ret)
    ret = d.get_coauthors_by_paper_id('7664ED89')
    print (ret)
    ret = d.get_title_venue_year_by_paper_id('7664ED89')
    print (ret)