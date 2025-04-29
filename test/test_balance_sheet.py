import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import sys
import os
import logging
from financial_database import FinancialDatabase

# 设置日志
def setup_logging():
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'financial_statements_{timestamp}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logging.info(f'日志文件保存在: {log_file}')

def get_quarter_end_date():
    today = datetime.now()
    if today.month <= 3:
        return f"{today.year-2}1231"
    elif today.month <= 6:
        return f"{today.year-1}0331"
    elif today.month <= 9:
        return f"{today.year-1}0630"
    else:
        return f"{today.year-1}0930"

def test_balance_sheet(db):
    """测试资产负债表数据获取和保存"""
    date = get_quarter_end_date()
    print(f"\n获取日期 {date} 的资产负债表数据")
    
    try:
        # 获取资产负债表数据
        df = ak.stock_zcfz_em(date=date)
        print("\n获取资产负债表数据成功！")
        
        print("\n原始数据预览：")
        print(df.head())
        
        print("\n数据列名：")
        print(df.columns.tolist())
        
        print("\n数据形状：")
        print(df.shape)
        
        # 转换列名
        column_mapping = {
            '资产-货币资金': '资产货币资金',
            '资产-应收账款': '资产应收账款',
            '资产-存货': '资产存货',
            '资产-总资产': '资产总资产',
            '资产-总资产同比': '资产总资产同比',
            '负债-应付账款': '负债应付账款',
            '负债-预收账款': '负债预收账款',
            '负债-总负债': '负债总负债',
            '负债-总负债同比': '负债总负债同比'
        }
        df = df.rename(columns=column_mapping)
        
        # 保存数据到数据库
        db.save_balance_sheet(df)
        
    except Exception as e:
        print(f"获取资产负债表数据时出错: {str(e)}")

def test_income_statement(db):
    """测试利润表数据获取和保存"""
    date = get_quarter_end_date()
    logging.info(f"开始获取日期 {date} 的利润表数据")
    
    try:
        # 获取利润表数据
        df = ak.stock_lrb_em(date=date)
        if df is None or df.empty:
            logging.error("获取的利润表数据为空")
            return
            
        logging.info(f"成功获取利润表数据，包含 {len(df)} 条记录")
        
        logging.info("\n原始数据预览：")
        logging.info(df.head())
        
        logging.info("\n数据列名：")
        logging.info(df.columns.tolist())
        
        logging.info("\n数据形状：")
        logging.info(df.shape)
        
        # 检查必要列是否存在
        required_columns = ['股票代码', '股票简称', '净利润', '营业总收入']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.error(f"利润表数据缺少必要列: {missing_columns}")
            return
            
        # 转换列名
        column_mapping = {
            '营业总支出-营业支出': '营业总支出营业支出',
            '营业总支出-销售费用': '营业总支出销售费用',
            '营业总支出-管理费用': '营业总支出管理费用',
            '营业总支出-财务费用': '营业总支出财务费用',
            '营业总支出-营业总支出': '营业总支出营业总支出'
        }
        df = df.rename(columns=column_mapping)
        
        # 检查数据质量
        if df['股票代码'].isnull().any():
            logging.warning("存在股票代码为空的数据")
            df = df.dropna(subset=['股票代码'])
            
        if df['公告日期'].isnull().any():
            logging.warning("存在公告日期为空的数据")
            df = df.dropna(subset=['公告日期'])
            
        # 保存数据到数据库
        db.save_income_statement(df)
        logging.info("成功保存利润表数据到数据库")
        
        # 验证数据是否成功保存
        saved_data = db.get_income_statement()
        if saved_data is None or saved_data.empty:
            logging.error("从数据库读取的利润表数据为空")
        else:
            logging.info(f"从数据库成功读取 {len(saved_data)} 条利润表记录")
            
    except Exception as e:
        logging.error(f"获取利润表数据时出错: {str(e)}")
        raise

def test_cash_flow(db):
    """测试现金流量表数据获取和保存"""
    date = get_quarter_end_date()
    print(f"\n获取日期 {date} 的现金流量表数据")
    
    try:
        # 获取现金流量表数据
        df = ak.stock_xjll_em(date=date)
        print("\n获取现金流量表数据成功！")
        
        print("\n原始数据预览：")
        print(df.head())
        
        print("\n数据列名：")
        print(df.columns.tolist())
        
        print("\n数据形状：")
        print(df.shape)
        
        # 转换列名
        column_mapping = {
            '净现金流-净现金流': '净现金流净现金流',
            '净现金流-同比增长': '净现金流同比增长',
            '经营性现金流-现金流量净额': '经营性现金流现金流量净额',
            '经营性现金流-净现金流占比': '经营性现金流净现金流占比',
            '投资性现金流-现金流量净额': '投资性现金流现金流量净额',
            '投资性现金流-净现金流占比': '投资性现金流净现金流占比',
            '融资性现金流-现金流量净额': '融资性现金流现金流量净额',
            '融资性现金流-净现金流占比': '融资性现金流净现金流占比'
        }
        df = df.rename(columns=column_mapping)
        
        # 保存数据到数据库
        db.save_cash_flow(df)
        
    except Exception as e:
        print(f"获取现金流量表数据时出错: {str(e)}")

def test_data_retrieval(db):
    """测试从数据库读取保存的数据"""
    print("\n从数据库读取保存的数据：")
    
    print("\n资产负债表数据：")
    df_bs = db.get_balance_sheet()
    print(df_bs)
    
    print("\n利润表数据：")
    df_is = db.get_income_statement()
    print(df_is)
    
    print("\n现金流量表数据：")
    df_cf = db.get_cash_flow()
    print(df_cf)

def main():
    setup_logging()
    print("开始测试财务报表数据获取...")
    
    # 连接数据库
    db = FinancialDatabase('data/financial_data.db')
    
    # 测试资产负债表数据
    test_balance_sheet(db)
    
    # 测试利润表数据
    test_income_statement(db)
    
    # 测试现金流量表数据
    test_cash_flow(db)
    
    # 测试数据检索
    test_data_retrieval(db)
    
    # 关闭数据库连接
    db.close()

if __name__ == "__main__":
    main() 