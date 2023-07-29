import sqlite3
import unittest
import tempfile

import statement_processor.statement_processor as sp

class TestStatementProcessor(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(':memory:')
        self.telegram_user_id = 'test_telegram_user_name'
        self.ftp_user_id = 'test_ftp_user_id'
        self.account_number = '47072270'
        self.account_name = 'Fake Account Name'

        # Mock ACCOUNTS table with fake user
        cur = self.conn.cursor()
        cur.execute('''
                    CREATE TABLE ACCOUNTS (
                        TELEGRAM_USER_ID	TEXT,
                        FTP_USER_ID	TEXT,
                        ACCOUNT_NUMBER	TEXT,
                        ACCOUNT_NAME	TEXT);
                    ''')
        
        cur.execute(f'INSERT INTO ACCOUNTS VALUES ("{self.telegram_user_id}", "{self.ftp_user_id}", "{self.account_number}", "{self.account_name}")')
        
        return super().setUp()
    
    def test_processing_mt4_template(self):
        with tempfile.TemporaryDirectory(dir='tests/statement_processor_test') as dst_temp_processing_dir:
            df = sp.process_statement_file(telegram_user_id=self.telegram_user_id,
                                    ftp_user_id=self.ftp_user_id,
                                    src_file_name='tests/statement_processor_test/statement_mt4_full.htm',
                                    dst_processing_dir=dst_temp_processing_dir,
                                    conn=self.conn)
        
        # Check full loaded row count
        cur = self.conn.cursor()
        res = cur.execute("SELECT COUNT(ORDER_ID) FROM Trans")
        assert(res.fetchone()[0] == 2085) # statement_mt4_full.htm has 2085 rows
        
        # Check created comments for trading trans
        res = cur.execute("SELECT COMMENT FROM Trans WHERE ORDER_ID='71409437'")
        assert(res.fetchone()[0] == 'Start BUY')
        
        # Check sum(PROFIT)
        res = cur.execute("SELECT SUM(PROFIT) FROM Trans")
        assert(abs(46684-res.fetchone()[0]) < 0.1)
        
    def test_processing_roboforex_template(self):
        with tempfile.TemporaryDirectory(dir='tests/statement_processor_test') as dst_temp_processing_dir:
            df = sp.process_statement_file(telegram_user_id=self.telegram_user_id,
                                    ftp_user_id=self.ftp_user_id,
                                    src_file_name='tests/statement_processor_test/statement_roboforex_june.html',
                                    dst_processing_dir=dst_temp_processing_dir,
                                    conn=self.conn)
        
        # Check full loaded row count
        cur = self.conn.cursor()
        res = cur.execute("SELECT COUNT(ORDER_ID) FROM Trans")
        assert(res.fetchone()[0] == 1350) # statement_roboforex_june.html has 1350 rows
        
        # Check created comments for trading trans
        res = cur.execute("SELECT COMMENT FROM Trans WHERE ORDER_ID='71409437'")
        assert(res.fetchone()[0] == 'Start BUY')        
        
        # Check sum(PROFIT)
        res = cur.execute("SELECT SUM(PROFIT) FROM Trans")
        assert(abs(1443066-res.fetchone()[0]) < 0.1)
        
    def test_processing_multi_files_with_intersection_trans(self):
        with tempfile.TemporaryDirectory(dir='tests/statement_processor_test') as dst_temp_processing_dir:
            df = sp.process_statement_file(telegram_user_id=self.telegram_user_id,
                                    ftp_user_id=self.ftp_user_id,
                                    src_file_name='tests/statement_processor_test/statement_roboforex_june.html',
                                    dst_processing_dir=dst_temp_processing_dir,
                                    conn=self.conn)
            df = sp.process_statement_file(telegram_user_id=self.telegram_user_id,
                                    ftp_user_id=self.ftp_user_id,
                                    src_file_name='tests/statement_processor_test/statement_roboforex_july.html',
                                    dst_processing_dir=dst_temp_processing_dir,
                                    conn=self.conn)            
            df = sp.process_statement_file(telegram_user_id=self.telegram_user_id,
                                    ftp_user_id=self.ftp_user_id,
                                    src_file_name='tests/statement_processor_test/statement_mt4_full.htm',
                                    dst_processing_dir=dst_temp_processing_dir,
                                    conn=self.conn)                        
        
        # Check full loaded row count
        cur = self.conn.cursor()
        res = cur.execute("SELECT COUNT(ORDER_ID) FROM Trans")
        assert(res.fetchone()[0] == 2254) # statement_roboforex_june.html has 1350 rows
        
        # Check created comments for trading trans
        res = cur.execute("SELECT COMMENT FROM Trans WHERE ORDER_ID='71409437'")
        assert(res.fetchone()[0] == 'Start BUY')        
        
        # Check sum(PROFIT)
        res = cur.execute("SELECT SUM(PROFIT) FROM Trans")
        assert(abs(2150517.33-res.fetchone()[0]) < 0.1)

        
if __name__ == '__main__':
    unittest.main()