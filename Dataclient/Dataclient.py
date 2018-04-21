# -*- coding: UTF-8 -*-
import pymysql as MySQLdb
class Dataclient:
    def __init__(self,host1,port1,user1,passwd1,db1,charset1):
        self.host=host1
        self.port=port1
        self.user=user1
        self.passwd=passwd1
        self.db=db1
        self.charset=charset1
        conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db,
                       charset=self.charset)
        #conn = MySQLdb.connect(host='202.120.36.29', port=3306, user='groupleader', passwd='onlyleaders', db='mag-new-160205',charset="utf8")
        self.cursor = conn.cursor()


    def get_paper_affiliations_by_author_name(self,author_name):
        #select stuname as '姓名',classname as '班级' from student inner join c lass on student.stuid=class.stuid
        #select stuname as '姓名',classname as '班级'
        #from student,class
        #where student.stuid=class.stuid
        quest_paper_by_author_name = 'SELECT PaperID,AffiliationID,A.AuthorID FROM PaperAuthorAffiliations AS P INNER JOIN ' \
                                     '(SELECT AuthorID FROM Authors WHERE AuthorName ="%s") AS A ' \
                                     'ON P.AuthorID = A.AuthorID'
        self.cursor.execute(quest_paper_by_author_name % author_name)
        paper_affiliations = self.cursor.fetchall()
        return paper_affiliations


    def get_coauthors_by_paper_id(self,paper_id):
        quest_author_by_paper = 'SELECT AuthorID FROM PaperAuthorAffiliations WHERE PaperID = "%s"'
        self.cursor.execute(quest_author_by_paper % paper_id)
        author_ids = self.cursor.fetchall()
        if len(author_ids) > 20:
            return None

        quest_author_by_paper = 'SELECT AuthorName FROM Authors INNER JOIN ' \
                                '(SELECT AuthorID FROM PaperAuthorAffiliations WHERE PaperID = "%s") AS TB ' \
                                'ON Authors.AuthorID = TB.AuthorID'
        self.cursor.execute(quest_author_by_paper % paper_id)
        authors = self.cursor.fetchall()
        return authors


    def get_title_venue_year_by_paper_id(self,paper_id):
        quest_info_by_paper = 'SELECT NormalizedPaperTitle, ConferenceSeriesIDMappedToVenueName, ' \
                              'JournalIDMappedToVenueName, PaperPublishYear FROM Papers WHERE PaperID = "%s"'
        self.cursor.execute(quest_info_by_paper % paper_id)
        rs = self.cursor.fetchall()
        return rs

        
