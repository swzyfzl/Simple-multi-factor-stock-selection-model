import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from scipy import stats
from typing import Dict, List, Tuple
from financial_database import FinancialDatabase
import logging
import os

class FactorModel:
    def __init__(self):
        self.factors = {}  # 存储因子及其权重
        self.factor_scores = {}  # 存储每个因子的得分
        self.final_scores = None  # 存储最终的综合得分
        self.scaler = StandardScaler()
        
        # 确保data/SH目录存在
        db_dir = 'data/SH'
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
            
        self.db = FinancialDatabase(os.path.join(db_dir, 'financial_data.db'))
        logging.basicConfig(level=logging.INFO)
        
    def add_factor(self, factor_name: str, weight: float):
        """添加因子及其权重"""
        if weight < 0 or weight > 1:
            raise ValueError("权重必须在0到1之间")
        self.factors[factor_name] = weight
        
    def calculate_rsi(self, data: pd.DataFrame, period: int = 14) -> float:
        """计算RSI因子"""
        try:
            if not isinstance(data, pd.DataFrame) or '收盘' not in data.columns:
                return 0.0
                
            close_prices = data['收盘'].astype(float)
            delta = close_prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return float(rsi.iloc[-1]) if not rsi.empty else 0.0
        except Exception as e:
            return 0.0
        
    def calculate_volatility(self, data: pd.DataFrame, period: int = 20) -> float:
        """计算波动率因子"""
        try:
            if not isinstance(data, pd.DataFrame) or '收盘' not in data.columns:
                return 0.0
                
            close_prices = data['收盘'].astype(float)
            returns = close_prices.pct_change()
            volatility = returns.rolling(window=period).std()
            return float(volatility.iloc[-1]) if not volatility.empty else 0.0
        except Exception as e:
            return 0.0
        
    def calculate_momentum(self, data: pd.DataFrame, period: int = 20) -> float:
        """计算动量因子"""
        try:
            if not isinstance(data, pd.DataFrame) or '收盘' not in data.columns:
                return 0.0
                
            close_prices = data['收盘'].astype(float)
            momentum = close_prices.pct_change(periods=period)
            return float(momentum.iloc[-1]) if not momentum.empty else 0.0
        except Exception as e:
            return 0.0
        
    def calculate_technical_factors(self, stock_data: Dict[str, pd.DataFrame], date: str = None):
        """计算所有技术因子"""
        try:
            self.factor_scores = {}
            
            # 首先计算所有股票的每个因子
            for code, data in stock_data.items():
                if not isinstance(data, pd.DataFrame) or data.empty:
                    continue
                    
                self.factor_scores[code] = {}
                
                # 计算RSI（使用14天周期）
                if 'rsi' in self.factors:
                    self.factor_scores[code]['rsi'] = self.calculate_rsi(data, period=14)
                
                # 计算波动率（使用20天周期）
                if 'volatility' in self.factors:
                    self.factor_scores[code]['volatility'] = self.calculate_volatility(data, period=20)
                
                # 计算动量（使用20天周期）
                if 'momentum' in self.factors:
                    self.factor_scores[code]['momentum'] = self.calculate_momentum(data, period=20)
            
            # 对每个因子进行标准化
            for factor in self.factors.keys():
                if factor in ['rsi', 'volatility', 'momentum']:
                    # 获取所有股票的该因子得分
                    scores = {}
                    for code in self.factor_scores.keys():
                        if factor in self.factor_scores[code]:
                            scores[code] = self.factor_scores[code][factor]
                    
                    if not scores:
                        continue
                        
                    # 转换为Series
                    scores_series = pd.Series(scores)
                    
                    # 处理无效值
                    valid_scores = scores_series[pd.notnull(scores_series) & 
                                               (scores_series != float('inf')) & 
                                               (scores_series != float('-inf'))]
                    
                    if len(valid_scores) > 0:
                        # 标准化
                        mean = valid_scores.mean()
                        std = valid_scores.std()
                        
                        if std != 0:
                            # 对有效值进行标准化
                            normalized_scores = (scores_series - mean) / std
                            # 将无效值设置为最小值
                            normalized_scores[pd.isnull(scores_series) | 
                                           (scores_series == float('inf')) | 
                                           (scores_series == float('-inf'))] = -3.0
                        else:
                            normalized_scores = scores_series - mean
                    else:
                        # 如果所有值都无效，则全部设为0
                        normalized_scores = pd.Series(0.0, index=scores_series.index)
                    
                    # 更新标准化后的得分
                    for code in self.factor_scores.keys():
                        if code in normalized_scores.index:
                            self.factor_scores[code][factor] = float(normalized_scores[code])
                        else:
                            self.factor_scores[code][factor] = 0.0
                    
        except Exception as e:
            raise
        
    def calculate_final_score(self):
        """计算最终的综合得分"""
        if not self.factor_scores:
            raise ValueError("请先计算技术因子")
            
        final_scores = {}
        for code in self.factor_scores.keys():
            score = 0
            for factor, weight in self.factors.items():
                if factor in self.factor_scores[code]:
                    score += self.factor_scores[code][factor] * weight
            final_scores[code] = score
            
        self.final_scores = pd.Series(final_scores)
        return self.final_scores
        
    def select_top_stocks(self, n: int) -> List[Tuple[str, float]]:
        """选择得分最高的n只股票"""
        if self.final_scores is None:
            raise ValueError("请先计算最终得分")
            
        return list(self.final_scores.nlargest(n).items())
        
    def get_factor_exposure(self, code: str) -> Dict[str, float]:
        """获取某只股票的因子暴露"""
        if code not in self.factor_scores:
            raise ValueError(f"股票 {code} 没有因子数据")
            
        return {factor: score for factor, score in self.factor_scores[code].items()}

    def calculate_market_factor(self, returns, market_returns):
        """计算市场因子"""
        return market_returns
        
    def calculate_size_factor(self, stock_data):
        """计算规模因子"""
        market_cap = stock_data['close'] * stock_data['volume']
        size_factor = np.log(market_cap)
        return (size_factor - size_factor.mean()) / size_factor.std()
        
    def calculate_momentum_factor(self, returns, lookback_period=252):
        """计算动量因子"""
        momentum = returns.rolling(window=lookback_period).mean()
        return (momentum - momentum.mean()) / momentum.std()
        
    def calculate_volatility_factor(self, returns, lookback_period=60):
        """计算波动率因子"""
        volatility = returns.rolling(window=lookback_period).std()
        return (volatility - volatility.mean()) / volatility.std()

    def calculate_all_factors(self, stock_data: Dict[str, pd.DataFrame], market_data: Dict[str, pd.DataFrame], date: str):
        """计算所有因子"""
        try:
            # 初始化factor_scores
            self.factor_scores = {}
            for code in stock_data.keys():
                self.factor_scores[code] = {}
            
            # 计算并存储市场因子
            market_returns = {}
            for code, data in market_data.items():
                if not data.empty:
                    market_returns[code] = data['收盘'].pct_change().iloc[-1] if len(data) > 1 else 0
                else:
                    market_returns[code] = 0
            
            for code in stock_data.keys():
                self.factor_scores[code]['market'] = market_returns.get(code, 0)
            
            # 计算并存储规模因子
            market_caps = {}
            for code, data in stock_data.items():
                if not data.empty:
                    market_caps[code] = data['收盘'].iloc[-1] * data['成交量'].iloc[-1]
                else:
                    market_caps[code] = 0
            
            size_factors = {code: np.log(cap) if cap > 0 else 0 for code, cap in market_caps.items()}
            size_mean = np.mean(list(size_factors.values()))
            size_std = np.std(list(size_factors.values()))
            for code in stock_data.keys():
                self.factor_scores[code]['size'] = (size_factors[code] - size_mean) / size_std if size_std != 0 else 0
            
            # 计算并存储动量因子
            momentum = {}
            for code, data in stock_data.items():
                if not data.empty and len(data) > 252:
                    returns = data['收盘'].pct_change()
                    momentum[code] = returns.rolling(window=252).mean().iloc[-1]
                else:
                    momentum[code] = 0
            
            momentum_mean = np.mean(list(momentum.values()))
            momentum_std = np.std(list(momentum.values()))
            for code in stock_data.keys():
                self.factor_scores[code]['momentum'] = (momentum[code] - momentum_mean) / momentum_std if momentum_std != 0 else 0
            
            # 计算并存储波动率因子
            volatility = {}
            for code, data in stock_data.items():
                if not data.empty and len(data) > 60:
                    returns = data['收盘'].pct_change()
                    volatility[code] = returns.rolling(window=60).std().iloc[-1]
                else:
                    volatility[code] = 0
            
            vol_mean = np.mean(list(volatility.values()))
            vol_std = np.std(list(volatility.values()))
            for code in stock_data.keys():
                self.factor_scores[code]['volatility'] = (volatility[code] - vol_mean) / vol_std if vol_std != 0 else 0
            
            return self.factor_scores
            
        except Exception as e:
            print(f"计算因子时出错: {str(e)}")
            return None

    def close(self):
        """关闭数据库连接"""
        if hasattr(self, 'db'):
            self.db.close() 