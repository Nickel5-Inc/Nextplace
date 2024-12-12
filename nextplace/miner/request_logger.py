import sqlite3
import json
from datetime import datetime
import bittensor as bt

class RequestLogger:
    def __init__(self, db_path="/home/ubuntu/Nextplace/requests_log.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """初始化數據庫表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 創建請求日誌表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS request_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                hotkey TEXT,
                validator_uid INTEGER,
                validator_stake REAL,
                request_type TEXT,
                request_data TEXT,
                response_data TEXT,
                processing_time REAL
            )
        ''')
        
        # 創建預測詳情表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS prediction_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER,
                nextplace_id TEXT,
                property_id TEXT,
                listing_id TEXT,
                address TEXT,
                price REAL,
                predicted_price REAL,
                predicted_date TEXT,
                market TEXT,
                property_details TEXT,
                FOREIGN KEY (request_id) REFERENCES request_logs(id)
            )
        ''')
        
        conn.commit()
        conn.close()

    def log_request(self, hotkey: str, request_data: str, validator_uid: int, validator_stake: float):
        """記錄請求到數據庫"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            timestamp = datetime.now().isoformat()
            
            # 插入基本請求信息
            cursor.execute('''
                INSERT INTO request_logs (timestamp, hotkey, validator_uid, validator_stake, request_type, request_data)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (timestamp, hotkey, validator_uid, validator_stake, 'prediction_request', request_data))
            
            request_id = cursor.lastrowid
            
            # 如果是預測請求，解析並存儲詳細信息
            try:
                request_obj = json.loads(request_data)
                if 'predictions' in request_obj:
                    for pred in request_obj['predictions']:
                        cursor.execute('''
                            INSERT INTO prediction_details (
                                request_id, nextplace_id, property_id, listing_id,
                                address, price, market, property_details
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            request_id,
                            pred.get('nextplace_id'),
                            pred.get('property_id'),
                            pred.get('listing_id'),
                            pred.get('address'),
                            pred.get('price'),
                            pred.get('market'),
                            json.dumps(pred)
                        ))
            except json.JSONDecodeError as e:
                bt.logging.error(f"JSON 解析錯誤: {e}")
                bt.logging.error(f"原始數據: {request_data}")
            except Exception as e:
                bt.logging.error(f"處理預測數據時發生錯誤: {e}")
            
            conn.commit()
            return request_id
        except Exception as e:
            bt.logging.error(f"數據庫操作錯誤: {e}")
            return None
        finally:
            conn.close()

    def log_response(self, request_id: int, response_data: str, processing_time: float):
        """記錄響應到數據庫"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE request_logs
            SET response_data = ?, processing_time = ?
            WHERE id = ?
        ''', (response_data, processing_time, request_id))
        
        conn.commit()
        conn.close()

    def get_request_stats(self, days=1):
        """獲取請求統計信息"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        stats = {
            'total_requests': 0,
            'avg_processing_time': 0,
            'requests_by_validator': {},
            'predictions_by_market': {}
        }
        
        # 實現統計查詢...
        
        return stats