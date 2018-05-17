from algorithm.name_disambiguation_local import *
from algorithm.name_disambiguation_test import *
from lib.DatabaseClient import *
from lib.DisData import *
from preprocess.preprocess import *
from lib.Logger import *



if __name__ == '__main__':

    logger = get_logger("main_log.txt")
    preprocess_author_to_files('issa yavari', 'data/preprocess_test') #下载数据到一个文件夹
    name_disambiguation_local('issa yavari','data/preprocess_test',logger) #从这个文件夹读数据 运行算法
    