import akshare as ak
import sqlite3
from datetime import datetime
import pandas as pd
import os

class StockDataModel:
    def __init__(self, db_path='data/SH/SH_data.db'):
        # 确保目录存在
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """初始化数据库表结构"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sh_index (
                date TEXT PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
        ''')
        conn.commit()
        conn.close()

    def fetch_sh_index_data(self):
        """从akshare获取上证指数数据"""
        try:
            df = ak.stock_zh_index_daily(symbol="sh000001")
            return df
        except Exception as e:
            print(f"获取数据失败: {e}")
            return None

    def save_to_db(self, df):
        """将数据保存到数据库"""
        if df is None or df.empty:
            return False

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 准备数据
        data = df[['date', 'open', 'high', 'low', 'close', 'volume']].values.tolist()

        # 使用批量插入
        cursor.executemany('''
            INSERT OR REPLACE INTO sh_index (date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', data)

        conn.commit()
        conn.close()
        return True

    def get_latest_data(self, limit=10):
        """获取最新的数据"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM sh_index
            ORDER BY date DESC
            LIMIT ?
        ''', (limit,))
        data = cursor.fetchall()
        conn.close()
        return data 