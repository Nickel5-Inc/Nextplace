import logging
import bittensor as bt
from datetime import datetime

class RequestLogger:
    """用於記錄 validator 請求的日誌記錄器"""
    
    def __init__(self):
        self.logger = logging.getLogger('request_logger')
        self._setup_logger()
    
    def _setup_logger(self):
        """設置日誌格式和級別"""
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
    
    def log_request(self, hotkey: str, request_data: str):
        """記錄接收到的請求"""
        bt.logging.info(f"📝 收到請求:")
        bt.logging.info(f"  - 時間: {datetime.now()}")
        bt.logging.info(f"  - Hotkey: {hotkey}")
        bt.logging.info(f"  - 請求內容: {request_data}")
    
    def log_response(self, response_data: str):
        """記錄響應內容"""
        bt.logging.info(f"📤 發送響應:")
        bt.logging.info(f"  - 時間: {datetime.now()}")
        bt.logging.info(f"  - 響應內容: {response_data}")