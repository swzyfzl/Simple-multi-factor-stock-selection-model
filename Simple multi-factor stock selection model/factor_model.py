import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from scipy import stats
from typing import Dict, List, Tuple

class FactorModel:
    def __init__(self):
        self.factors = {}  # 存储因子及其权重
        self.factor_scores = {}  # 存储每个因子的得分
        self.final_scores = None  # 存储最终的综合得分
        self.scaler = StandardScaler()
        
    def add_factor(self, factor_name: str, weight: float):
        """添加因子及其权重"""
        if weight < 0 or weight > 1:
            raise ValueError("权重必须在0到1之间")
        self.factors[factor_name] = weight
        
    def calculate_rsi(self, data: pd.DataFrame, period: int = 14) -> pd.Series:
        """计算RSI因子"""
        delta = data['收盘'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
        
    def calculate_volatility(self, data: pd.DataFrame, period: int = 20) -> pd.Series:
        """计算波动率因子"""
        returns = data['收盘'].pct_change()
        return returns.rolling(window=period).std()
        
    def calculate_momentum(self, data: pd.DataFrame, period: int = 20) -> pd.Series:
        """计算动量因子"""
        return data['收盘'].pct_change(periods=period)
        
    def calculate_technical_factors(self, stock_data: Dict[str, pd.DataFrame]):
        """计算所有技术因子"""
        self.factor_scores = {}
        
        for code, data in stock_data.items():
            self.factor_scores[code] = {}
            
            # 计算RSI
            if 'rsi' in self.factors:
                self.factor_scores[code]['rsi'] = self.calculate_rsi(data)
                
            # 计算波动率
            if 'volatility' in self.factors:
                self.factor_scores[code]['volatility'] = self.calculate_volatility(data)
                
            # 计算动量
            if 'momentum' in self.factors:
                self.factor_scores[code]['momentum'] = self.calculate_momentum(data)
                
    def normalize_factor_scores(self):
        """对因子得分进行标准化"""
        for factor in self.factors.keys():
            scores = pd.Series([scores[factor].iloc[-1] for scores in self.factor_scores.values()])
            scores = (scores - scores.mean()) / scores.std()
            for i, code in enumerate(self.factor_scores.keys()):
                self.factor_scores[code][factor] = scores[i]
                
    def calculate_final_score(self):
        """计算最终的综合得分"""
        if not self.factor_scores:
            raise ValueError("请先计算技术因子")
            
        self.normalize_factor_scores()
        
        final_scores = {}
        for code in self.factor_scores.keys():
            score = 0
            for factor, weight in self.factors.items():
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
        
    def calculate_all_factors(self, stock_data, market_data):
        """计算所有因子"""
        returns = stock_data['close'].pct_change()
        market_returns = market_data['close'].pct_change()
        
        self.factors['market'] = self.calculate_market_factor(returns, market_returns)
        self.factors['size'] = self.calculate_size_factor(stock_data)
        self.factors['momentum'] = self.calculate_momentum_factor(returns)
        self.factors['volatility'] = self.calculate_volatility_factor(returns)
        
        return self.factors 