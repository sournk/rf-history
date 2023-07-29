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
    
    df_accounts['LAST_CHECK_DT'] = pd.to_datetime(df_accounts['LAST_CHECK_DT'], errors='ignore')
    df_accounts['LAST_UPDATE_DT'] = pd.to_datetime(df_accounts['LAST_UPDATE_DT'], errors='ignore')
    
    df_accounts = df_accounts[df_accounts['ACTIVE'] == 1]
    df_accounts = df_accounts.sort_values(by='LAST_CHECK_DT')
    
    for idx, row in df_accounts.iterrows():
        src_ftp_file = pathlib.Path(f'{config.SRC_FTP_DIR}/{row["FTP_USER_ID"]}/{config.STATEMENT_FILENAME}')
        
        if src_ftp_file.exists():
            # src_ftp_file.lstat().st_mtime #Output: 1496134873.8279443
            modified_dt = datetime.datetime.fromtimestamp(src_ftp_file.lstat().st_mtime)
            
            if (row['LAST_CHECK_DT']) or (modified_dt > row['LAST_CHECK_DT']):
                cur = conn.cursor()
                cur.execute('UPDATE ACCOUNTS SET LAST_CHECK_DT = "123" WHERE FTP_USER_ID = "123456789"')
                conn.commit()
                statement_dispatcher_logger.debug(f'Enqueue !!! {src_ftp_file} - {modified_dt=} {row["LAST_CHECK_DT"]=}')
            else:
                statement_dispatcher_logger.debug(f'Skip {src_ftp_file} - {modified_dt}')
        else:
            statement_dispatcher_logger.debug(f'File is not exists: {src_ftp_file}')

        # modification dt of src_ftp_file > LAST_CHECK_DT
            # update check dt in DB 
            # enqueue file processing
        # else skip + log

