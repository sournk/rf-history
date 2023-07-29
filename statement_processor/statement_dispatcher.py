from redis import Redis
from rq import Queue
import logging
import sys
import sqlite3

import statement_processor
import ftp_scanner

sys.path.append("..")
import config
from config import statement_dispatcher_logger as statement_dispatcher_logger

def enqueue_process_statement_file():
    ftp_user_name = '123456789'
    src_file_name = f'{config.SRC_FTP_DIR}/{ftp_user_name}/{config.STATEMENT_FILENAME}'
    
    q = Queue(connection=Redis(), name=config.STATEMENT_PROCESSING_QUEUE_NAME)
    job = q.enqueue(statement_processor.process_statement_file, 
                    'sournkz',
                    ftp_user_name,
                    src_file_name,
                    config.PROCESSING_DIR)
    
    statement_dispatcher_logger.info(f'Enqueue statement processing {job}')
    
def enqueue_scan_ftp_for_updates():
    ftp_user_name = '123456789'
    src_file_name = f'{config.SRC_FTP_DIR}/{ftp_user_name}/{config.STATEMENT_FILENAME}'
    
    q = Queue(connection=Redis(), name=config.FTP_SCAN_QUEUE_NAME)
    job = q.enqueue(ftp_scanner.scan_ftp_for_updates)
    
    statement_dispatcher_logger.info(f'Enqueue scan FTP for updates {job}')


if __name__ == '__main__':
    enqueue_scan_ftp_for_updates()