import os
import logging


TELEGRAM_API = os.getenv('RF_HISTORY_TELEGRAM_API')
TRANS_TABLE_NAME = 'Trans'
UPLOAD_FILES_PATH = 'files_received'
SEND_FILES_PATH = 'files_sent'
LOG_LEVEL = logging.INFO
