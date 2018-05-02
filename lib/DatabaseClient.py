# -*- coding: UTF-8 -*-
import pymysql as MySQLdb
class DatabaseClient:
    def __init__(self,**kw):
        #host1,port1,user1,passwd1,db1,charset1):
        self.host=kw.get('host','202.120.36.29')
        self.port=kw.get('port',3306)
        self.user=kw.get('user','readonly')
        self.passwd=kw.get('password','readonly')
        self.db=kw.get('database','mag-new-160205')

        conn = MySQLdb.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db,
                       charset='utf8')
        #conn = MySQLdb.connect(host='202.120.36.29', port=3306, user='groupleader', passwd='onlyleaders', db='mag-new-160205',charset="utf8")
        self.cursor = conn.cursor()


    def get_paper_affiliations_by_author_name(self,author_name):
        #select stuname as '姓名',classname as '班级' from student inner join c lass on student.stuid=class.stuid
        #select stuname as '姓名',classname as '班级'
        #from student,class
        #where student.stuid=class.stuid
        quest_paper_by_author_name = 'SELECT PaperID,AffiliationID,A.AuthorID FROM PaperAuthorAffiliations AS P INNER JOIN ' \
                                     '(SELECT AuthorID FROM Authors WHERE AuthorName ="%s") AS A ' \
                                     'ON P.AuthorID = A.AuthorID'#Author name->AUTHRO ID , AUTHOR ID->PAPER AFFILIA id
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

        
    def get_aname2aid_from_db(self):
        #aname 2 aid
        if os.path.exists('/tmp/aname2aid.txt'):
            os.remove('/tmp/aname2aid.txt')
        cursor.execute("SELECT AuthorID,AuthorName  \
            INTO OUTFILE '/tmp/aname2aid.txt' FIELDS TERMINATED BY '$$$' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '\n' FROM Authors where AuthorName is not null")

    def get_aid2pid_affid_from_db(self):
        if os.path.exists('/tmp/aid2pid_affid.txt'):
            os.remove('/tmp/aid2pid_affid.txt')
        cursor.execute("SELECT AuthorID,AffiliationID,PaperID  \
            INTO OUTFILE '/tmp/aid2pid_affid.txt' FIELDS TERMINATED BY '\t' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '\n' FROM PaperAuthorAffiliations")

    def get_pid2ptitle_cid_jid_year(self):
        if os.path.exists('/tmp/pid2ptitle_cid_jid_year.txt'):
            os.remove('/tmp/pid2ptitle_cid_jid_year.txt')
        cursor.execute("SELECT PaperID, NormalizedPaperTitle, ConferenceSeriesIDMappedToVenueName, ' \
                              'JournalIDMappedToVenueName, PaperPublishYear \
                        INTO OUTFILE '/tmp/pid2ptitle_cid_jid_year.txt' FIELDS TERMINATED BY '$$$' OPTIONALLY ENCLOSED BY '' LINES TERMINATED BY '\n' FROM Papers")  

    def get_name2aid_from_file(self,fileName = '/tmp/aname2aid.txt',caids):
        aid2aname = {}
        COUNT = 0
        for line in open(fileName):
            line = line.split('$$$')
            count()
            if COUNT % 1000000 == 0:
                print(len(aid2aname))
            if line[0] in caids:
                aid2aname[line[0]] = line[1]
        print('load aid2aname done')
        sys.stdout.flush()
        return aid2aname

    def get_aid2pid_affid_from_file(self,fileName = '/tmp/aid2pid_affid.txt',caids):
        aid2pid_affid = {}
        COUNT = 0
        for line in open(fileName):
            line = line.split('\t')
            count()
            if COUNT % 1000000 == 0:
                print(len(aid2pid_affid))
            if line[0] in caids:
                if(line[0] not in aid2pid_affid):
                    aid2pid_affid[line[0]] = {}
                aid2pid_affid[line[0]]['affid'] = line[1]
                aid2pid_affid[line[0]]['pid'] = line[2]
        print('load aid2pid_affid done')
        sys.stdout.flush()
        return aid2pid_affid


    def get_name2aid_from_file(self,fileName = '/tmp/pid2ptitle_cid_jid_year.txt',caids):
        pid2ptitle_cid_jid_year = {}
        COUNT = 0
        for line in open(fileName):
            line = line.split('$$$')
            count()
            if COUNT % 1000000 == 0:
                print(len(pid2ptitle_cid_jid_year))
            if line[0] in caids:
                if(line[0] not in pid2ptitle_cid_jid_year):
                    pid2ptitle_cid_jid_year[line[0]] = {}
                pid2ptitle_cid_jid_year['ptitle'] = line[1]
                pid2ptitle_cid_jid_year['cid'] = line[2]
                pid2ptitle_cid_jid_year['jid'] = line[3]
                pid2ptitle_cid_jid_year['year'] = line[4]

        print('load pid2ptitle_cid_jid_year done')
        sys.stdout.flush()
        return pid2ptitle_cid_jid_year