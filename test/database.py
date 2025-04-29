import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

class StockDatabase:
    def __init__(self, db_path='data/stock_data.db'):
        self.db_path = db_path
        self._init_db()
        
    def _init_db(self):
        """初始化数据库，创建必要的表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建股票列表表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_list (
            code TEXT PRIMARY KEY,
            name TEXT,
            update_time TIMESTAMP
        )
        ''')
        
        # 创建股票数据表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_data (
            code TEXT,
            date DATE,
            开盘 REAL,
            收盘 REAL,
            最高 REAL,
            最低 REAL,
            成交量 REAL,
            成交额 REAL,
            update_time TIMESTAMP,
            PRIMARY KEY (code, date)
        )
        ''')
        
        conn.commit()
        conn.close()
        
    def save_stock_list(self, stock_list):
        """保存股票列表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 清空现有数据
        cursor.execute('DELETE FROM stock_list')
        
        # 插入新数据
        current_time = datetime.now()
        for _, row in stock_list.iterrows():
            cursor.execute('''
            INSERT OR REPLACE INTO stock_list (code, name, update_time)
            VALUES (?, ?, ?)
            ''', (row['code'], row['name'], current_time))
            
        conn.commit()
        conn.close()
        
    def get_stock_list(self):
        """获取股票列表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT code, name FROM stock_list')
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            return pd.DataFrame(columns=['code', 'name'])
            
        return pd.DataFrame(rows, columns=['code', 'name'])
        
    def save_stock_data(self, code, data):
        """保存单只股票的数据"""
        conn = sqlite3.connect(self.db_path)
        current_time = datetime.now()
        
        # 将数据转换为适合插入的格式
        data = data.copy()
        
        # 确保数据有正确的列名
        if '日期' in data.columns:
            data = data.rename(columns={'日期': 'date'})
        
        # 添加必要的列
        data['code'] = code
        data['update_time'] = current_time
        
        # 确保所有必要的列都存在
        required_columns = ['date', '开盘', '收盘', '最高', '最低', '成交量', '成交额', 'code', 'update_time']
        for col in required_columns:
            if col not in data.columns:
                print(f"警告: 数据中缺少列 '{col}'，将使用默认值")
                if col == 'date':
                    data[col] = data.index
                else:
                    data[col] = 0
        
        try:
            # 使用事务来确保数据完整性
            cursor = conn.cursor()
            
            # 获取数据库中该股票的最新日期
            cursor.execute('SELECT MAX(date) FROM stock_data WHERE code = ?', (code,))
            last_date = cursor.fetchone()[0]
            
            # 确保日期列是日期时间类型
            data['date'] = pd.to_datetime(data['date'])
            
            if last_date:
                # 如果数据库中有数据，只插入新数据
                last_date = pd.to_datetime(last_date)
                new_data = data[data['date'] > last_date]
                if not new_data.empty:
                    new_data.to_sql('stock_data', conn, if_exists='append', index=False)
                    print(f"成功更新股票 {code} 的数据，新增 {len(new_data)} 条记录")
                else:
                    print(f"股票 {code} 没有新数据需要更新")
            else:
                # 如果数据库中没有数据，插入所有数据
                data.to_sql('stock_data', conn, if_exists='append', index=False)
                print(f"成功保存股票 {code} 的数据，共 {len(data)} 条记录")
                
            conn.commit()
            
        except Exception as e:
            print(f"保存股票 {code} 数据时出错: {str(e)}")
            print("数据预览:")
            print(data.head())
            conn.rollback()
        finally:
            conn.close()
        
    def get_stock_data(self, code, start_date=None, end_date=None):
        """获取股票数据"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 构建查询条件
            query = 'SELECT * FROM stock_data WHERE code = ?'
            params = [code]
            
            if start_date:
                query += ' AND date >= ?'
                params.append(start_date)
            if end_date:
                query += ' AND date <= ?'
                params.append(end_date)
                
            query += ' ORDER BY date'
            
            # 执行查询
            cursor.execute(query, params)
            rows = cursor.fetchall()
            conn.close()
            
            if not rows:
                print(f"警告: 股票 {code} 在数据库中没有数据")
                return None
                
            # 获取列名
            columns = ['code', 'date', '开盘', '收盘', '最高', '最低', '成交量', '成交额', 'update_time']
            
            # 创建DataFrame
            df = pd.DataFrame(rows, columns=columns)
            # 使用更灵活的日期解析格式
            df['date'] = pd.to_datetime(df['date'], format='mixed')
            df.set_index('date', inplace=True)
            
            # 删除不需要的列
            df = df.drop(['code', 'update_time'], axis=1)
            
            print(f"成功从数据库获取股票 {code} 的数据，共 {len(df)} 条记录")
            return df
            
        except Exception as e:
            print(f"从数据库获取股票 {code} 数据时出错: {str(e)}")
            return None
        
    def is_data_available(self, code, start_date, end_date):
        """检查指定时间段的数据是否已存在"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT COUNT(*) FROM stock_data 
        WHERE code = ? AND date BETWEEN ? AND ?
        ''', (code, start_date, end_date))
        
        count = cursor.fetchone()[0]
        conn.close()
        
        return count > 0
        
    def get_data_date_range(self, code):
        """获取股票数据的日期范围"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT MIN(date), MAX(date), COUNT(*) 
        FROM stock_data 
        WHERE code = ?
        ''', (code,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0] and result[1]:
            return {
                'start_date': result[0],
                'end_date': result[1],
                'count': result[2]
            }
        return None 