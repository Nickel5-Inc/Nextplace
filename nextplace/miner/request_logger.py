import sqlite3
import json
from datetime import datetime, timedelta
import bittensor as bt

class RequestLogger:
    def __init__(self, db_path="/home/ubuntu/Nextplace/requests_log.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """初始化數據庫表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 創建請求日誌主表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS request_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                hotkey TEXT,
                validator_uid INTEGER,
                validator_stake REAL,
                request_type TEXT,
                processing_time REAL,
                response_data TEXT
            )
        ''')
        
        # 創建請求詳情表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS request_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER,
                nextplace_id TEXT,
                market TEXT,
                request_timestamp TEXT,
                request_data TEXT,
                response_data TEXT,
                predicted_sale_price REAL,
                predicted_sale_date TEXT,
                FOREIGN KEY (request_id) REFERENCES request_logs(id)
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
                city TEXT,
                state TEXT,
                zip_code TEXT,
                price REAL,
                beds INTEGER,
                baths REAL,
                sqft INTEGER,
                lot_size INTEGER,
                year_built INTEGER,
                days_on_market INTEGER,
                latitude REAL,
                longitude REAL,
                property_type TEXT,
                last_sale_date TEXT,
                hoa_dues REAL,
                query_date TEXT,
                market TEXT,
                predicted_price REAL,
                predicted_date TEXT,
                FOREIGN KEY (request_id) REFERENCES request_logs(id)
            )
        ''')
        
        conn.commit()
        conn.close()

    def log_request(self, hotkey: str, request_data: str, validator_uid: int, validator_stake: float):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            request_obj = json.loads(request_data)
            predictions = request_obj.get('predictions', [])
            
            # 插入主請求記錄
            cursor.execute('''
                INSERT INTO request_logs (
                    timestamp, hotkey, validator_uid, validator_stake, request_type
                ) VALUES (?, ?, ?, ?, ?)
            ''', (
                datetime.now().isoformat(),
                hotkey,
                validator_uid,
                validator_stake,
                'prediction_request'
            ))
            
            request_id = cursor.lastrowid
            
            # 插入每個請求的詳細信息
            for pred in predictions:
                # 計算預測日期
                query_date = pred.get('query_date')
                if query_date:
                    try:
                        base_date = datetime.strptime(query_date, "%Y-%m-%d")
                        predicted_date = (base_date + timedelta(days=30)).strftime("%Y-%m-%d")
                    except (ValueError, TypeError):
                        predicted_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
                else:
                    predicted_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")

                # 獲取預測價格
                predicted_price = float(pred.get('price', 0.0))
                
                cursor.execute('''
                    INSERT INTO prediction_details (
                        request_id, nextplace_id, property_id, listing_id,
                        address, city, state, zip_code, price, beds,
                        baths, sqft, lot_size, year_built, days_on_market,
                        latitude, longitude, property_type, last_sale_date,
                        hoa_dues, query_date, market, predicted_price, predicted_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    request_id,
                    pred.get('nextplace_id'),
                    pred.get('property_id'),
                    pred.get('listing_id'),
                    pred.get('address'),
                    pred.get('city'),
                    pred.get('state'),
                    pred.get('zip_code'),
                    pred.get('price'),
                    pred.get('beds'),
                    pred.get('baths'),
                    pred.get('sqft'),
                    pred.get('lot_size'),
                    pred.get('year_built'),
                    pred.get('days_on_market'),
                    pred.get('latitude'),
                    pred.get('longitude'),
                    pred.get('property_type'),
                    pred.get('last_sale_date'),
                    pred.get('hoa_dues'),
                    pred.get('query_date'),
                    pred.get('market'),
                    predicted_price,
                    predicted_date
                ))
            
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