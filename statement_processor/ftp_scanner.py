import sys
import sqlite3
import pandas as pd
import datetime
import pathlib

sys.path.append("..")
import config
from config import statement_dispatcher_logger as statement_dispatcher_logger


def scan_ftp_for_updates() -> None:
    conn = sqlite3.connect(config.DATABASE_CONNECTION_STRING)
    df_accounts = pd.read_sql('SELECT * FROM ACCOUNTS', 
                              con=conn)
    
    df_accounts = df_accounts[df_accounts['ACTIVE'] == 1]
    df_accounts = df_accounts.sort_values(by='LAST_CHECK_DT')
    statement_dispatcher_logger.debug(f'Got total list account accounts {len(df_accounts.shape)}')
    
    for idx, row in df_accounts.iterrows():
        src_ftp_file = pathlib.Path(f'{config.SRC_FTP_DIR}/{row["FTP_USER_ID"]}/{config.STATEMENT_FILENAME}')
        statement_dispatcher_logger.debug(f'Start update scan for {str(src_ftp_file)}')
        
        if src_ftp_file.exists():
            file_modified_dt = datetime.datetime.fromtimestamp(src_ftp_file.lstat().st_mtime)
            last_check_dt = datetime.datetime.fromisoformat(row["LAST_CHECK_DT"]) if row["LAST_CHECK_DT"] else None
            
            if (not last_check_dt) or (file_modified_dt > last_check_dt):
                cur = conn.cursor()
                cur.execute('UPDATE ACCOUNTS SET LAST_CHECK_DT = ? WHERE FTP_USER_ID = ?', (datetime.datetime.now(), row["FTP_USER_ID"]))
                conn.commit()
                
                statement_dispatcher_logger.debug(f'Enqueue processing modified file {src_ftp_file}')
            else:
                statement_dispatcher_logger.debug(f'Skipping not modified file {file_modified_dt} {src_ftp_file}')
        else:
            statement_dispatcher_logger.debug(f'File is not exists {src_ftp_file}')
            
        statement_dispatcher_logger.info(f'File scan finished {src_ftp_file}')


