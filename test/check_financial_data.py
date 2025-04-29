import pandas as pd
import sqlite3
from datetime import datetime
import os
import sys

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from test.financial_database import FinancialDatabase

def check_database_content():
    """检查财务报表数据库的内容"""
    print("开始检查财务报表数据库内容...")
    
    # 连接数据库
    db = FinancialDatabase('data/financial_data.db')
    
    try:
        # 检查资产负债表
        print("\n=== 资产负债表数据 ===")
        df_bs = db.get_balance_sheet()
        print(f"记录数量: {len(df_bs)}")
        print("\n数据预览:")
        print(df_bs.head())
        print("\n数据统计信息:")
        print(df_bs.describe())
        
        # 检查利润表
        print("\n=== 利润表数据 ===")
        df_is = db.get_income_statement()
        print(f"记录数量: {len(df_is)}")
        print("\n数据预览:")
        print(df_is.head())
        print("\n数据统计信息:")
        print(df_is.describe())
        
        # 检查现金流量表
        print("\n=== 现金流量表数据 ===")
        df_cf = db.get_cash_flow()
        print(f"记录数量: {len(df_cf)}")
        print("\n数据预览:")
        print(df_cf.head())
        print("\n数据统计信息:")
        print(df_cf.describe())
        
        # 检查数据完整性
        print("\n=== 数据完整性检查 ===")
        print(f"资产负债表记录数: {len(df_bs)}")
        print(f"利润表记录数: {len(df_is)}")
        print(f"现金流量表记录数: {len(df_cf)}")
        
        # 检查是否有重复数据
        print("\n=== 重复数据检查 ===")
        for table_name, df in [('资产负债表', df_bs), ('利润表', df_is), ('现金流量表', df_cf)]:
            duplicates = df.duplicated(subset=['股票代码', '公告日期'], keep=False)
            if duplicates.any():
                print(f"{table_name}中存在重复数据:")
                print(df[duplicates].sort_values(['股票代码', '公告日期']))
            else:
                print(f"{table_name}中没有重复数据")
        
    except Exception as e:
        print(f"检查数据库时出错: {str(e)}")
    finally:
        db.close()

if __name__ == "__main__":
    check_database_content() 