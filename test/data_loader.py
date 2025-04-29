import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import tqdm
import sys
import os
# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import StockDatabase

class StockDataLoader:
    def __init__(self):
        self.stock_list = None
        self.db = StockDatabase()
        
    def get_stock_list(self, force_update=False):
        """获取沪深300指数成分股列表"""
        if not force_update:
            # 尝试从数据库获取
            self.stock_list = self.db.get_stock_list()
            if not self.stock_list.empty:
                print("从数据库获取股票列表")
                return self.stock_list
                
        print("从网络获取股票列表")
        # 从akshare获取数据
        self.stock_list = ak.index_stock_cons_csindex(symbol="000300")
        print("原始列名:", self.stock_list.columns.tolist())
        
        # 重命名列名以保持一致性
        self.stock_list = self.stock_list.rename(columns={
            '成分券代码': 'code', 
            '成分券名称': 'name'
        })
        print("重命名后的列名:", self.stock_list.columns.tolist())
        
        # 保存到数据库
        self.db.save_stock_list(self.stock_list)
        
        return self.stock_list
    
    def get_stock_data(self, start_date, end_date, factors=None, force_update=False):
        """获取股票数据"""
        if factors is None:
            factors = ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额']
            
        if self.stock_list is None:
            self.get_stock_list()
            
        stock_data = {}
        
        # 检查每只股票的数据情况
        for code in tqdm.tqdm(self.stock_list['code']):
            try:
                print(f"\n处理股票 {code}...")
                
                # 从网络获取数据
                print(f"从网络获取股票 {code} 的数据，日期范围：{start_date} 到 {end_date}")
                df = ak.stock_zh_a_hist(symbol=code, period="daily", 
                                      start_date=start_date, end_date=end_date,
                                      adjust="qfq")
                
                if df is not None and not df.empty:
                    print(f"获取到 {len(df)} 条数据")
                    # 只保留需要的因子
                    df = df[factors]
                    stock_data[code] = df
                    
                    # 保存到数据库
                    self.db.save_stock_data(code, df)
                else:
                    print(f"股票 {code} 没有获取到数据")
                    
            except Exception as e:
                print(f"获取股票 {code} 数据失败: {str(e)}")
                
        return stock_data 