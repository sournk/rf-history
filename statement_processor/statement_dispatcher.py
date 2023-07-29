from redis import Redis
from rq import Queue
import logging
import sys

import statement_processor

sys.path.append("..")
import config
from config import statement_dispatcher_logger as statement_dispatcher_logger

if __name__ == '__main__':
    ftp_user_name = '123456789'
    src_file_name = f'{config.SRC_FTP_DIR}/{ftp_user_name}/{config.STATEMENT_FILENAME}'
    
    q = Queue(connection=Redis(), name=config.STATEMENT_PROCESSING_QUEUE_NAME)
    job = q.enqueue(statement_processor.process_statement_file, 
                    'sournkz',
                    ftp_user_name,
                    src_file_name,
                    config.PROCESSING_DIR)
    
    statement_dispatcher_logger.info(f'Enqueue statement processing {job}')
    

    # 1. Проверить dt обновления файла. Обработать только свежие файлы.