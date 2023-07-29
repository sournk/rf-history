import sys
import os
import logging


TELEGRAM_API = os.getenv('RF_HISTORY_TELEGRAM_API')

# DB settings
DATABASE_CONNECTION_STRING = '/Users/sournk/dev/rf-history/db.sqlite'
TRANS_TABLE_NAME = 'Trans'

# Processing files and dirs settings
UPLOAD_FILES_PATH = 'files_received' # todo replace by PROCESSING_DIR
SEND_FILES_PATH = 'files_sent'
SRC_FTP_DIR = '/Users/sournk/dev/rf-history/research/ftp'
PROCESSING_DIR = '/Users/sournk/dev/rf-history/files_received'
STATEMENT_FILENAME = 'statement.htm'

# Redis
STATEMENT_PROCESSING_QUEUE_NAME = 'STATEMENT_PROCESSING_QUEUE'

# Logging
STATEMENT_DISPATCHER_LOG_LEVEL = logging.DEBUG


statement_dispatcher_logger = logging.getLogger('statement_dispatcher_logger')
statement_dispatcher_logger.setLevel(STATEMENT_DISPATCHER_LOG_LEVEL)

ch = logging.StreamHandler()
ch.setLevel(STATEMENT_DISPATCHER_LOG_LEVEL)
ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
statement_dispatcher_logger.addHandler(ch)

