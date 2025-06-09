import pandas as pd
import numpy as np

class PatternAnalyzer:
    def __init__(self):
        """패턴 분석기 초기화"""
        pass
    
    def detect_vcp(self, df, window=60, std_threshold=0.8, min_periods=20):
        """Volatility Contraction Pattern (VCP) 감지
        
        Args:
            df: 주가 데이터프레임
            window: 변동성 분석 기간
            std_threshold: 변동성 수축 조건 
            min_periods: 최소 필요 기간
            
        Returns:
            vcp_found: VCP 패턴 존재 여부
            vcp_data: VCP 관련 데이터
        """
        if df.empty or len(df) < window:
            return False, {}
        
        # 최근 데이터 슬라이싱 (최신 데이터 우선)
        recent_df = df.tail(window).copy()
        
        # 고가-저가 범위 계산
        recent_df['Range'] = recent_df['High'] - recent_df['Low']
        
        # 변동성 측정 (고가-저가 범위의 이동 표준편차)
        recent_df['Volatility'] = recent_df['Range'].rolling(window=20, min_periods=min_periods).std()
        
        # 최근 20일 변동성과 이전 30일 변동성 비교
        if len(recent_df) < 50:  # 충분한 데이터가 없으면
            return False, {}
            
        recent_volatility = recent_df['Volatility'].iloc[-20:].mean()
        previous_volatility = recent_df['Volatility'].iloc[-50:-20].mean()
        
        # VCP 조건: 최근 변동성이 이전 변동성보다 유의미하게 낮음
        vcp_found = (recent_volatility < previous_volatility * std_threshold)
        
        # 주가 흐름이 상승 추세인지 확인 (이동평균선 정렬)
        ma_aligned = False
        if 'MA20' in df.columns and 'MA60' in df.columns and 'MA120' in df.columns:
            latest_values = df.iloc[-1]
            ma_aligned = (
                latest_values['Close'] > latest_values['MA20'] and
                latest_values['MA20'] > latest_values['MA60'] and
                latest_values['MA60'] > latest_values['MA120']
            )
        
        # VCP 패턴이고 이동평균선이 정렬되어 있어야 최종적으로 VCP로 인정
        vcp_found = vcp_found and ma_aligned
        
        vcp_data = {
            'recent_volatility': recent_volatility,
            'previous_volatility': previous_volatility,
            'volatility_ratio': recent_volatility / previous_volatility if previous_volatility > 0 else 0,
            'ma_aligned': ma_aligned
        }
        
        return vcp_found, vcp_data
    
    def detect_pocket_pivot(self, df, window=50, min_range=10, volume_factor=1.5):
        """Pocket Pivot 패턴 감지
        
        Args:
            df: 주가 데이터프레임
            window: 분석 기간
            min_range: 최소 필요 기간
            volume_factor: 거래량 급증 기준 (평균 대비)
            
        Returns:
            pivot_found: Pocket Pivot 패턴 존재 여부
            pivot_data: 패턴 관련 데이터
        """
        if df.empty or len(df) < window:
            return False, {}
        
        # 최근 데이터 슬라이싱
        recent_df = df.tail(window).copy()
        
        # Pocket Pivot 조건
        # 1. 거래량 급증 (이동평균의 1.5배 이상)
        # 2. 주가 상승 (+3% 이상)
        # 3. 이동평균선 위에 위치
        
        latest_idx = recent_df.index[-1]
        latest = recent_df.iloc[-1]
        prev = recent_df.iloc[-2]
        
        # 거래량 조건 확인
        volume_surge = False
        if 'Volume_MA20' in recent_df.columns:
            volume_surge = latest['Volume'] > latest['Volume_MA20'] * volume_factor
        
        # 가격 상승 조건 확인
        price_up = latest['Close'] > prev['Close'] * 1.03  # 3% 이상 상승
        
        # 이동평균선 위에 위치하는지 확인
        above_ma = False
        if 'MA20' in recent_df.columns and 'MA60' in recent_df.columns:
            above_ma = latest['Close'] > latest['MA20'] and latest['MA20'] > latest['MA60']
        
        # 최종 판정
        pivot_found = volume_surge and price_up and above_ma
        
        pivot_data = {
            'volume_ratio': latest['Volume'] / latest['Volume_MA20'] if 'Volume_MA20' in recent_df.columns and latest['Volume_MA20'] > 0 else 0,
            'price_change_pct': (latest['Close'] / prev['Close'] - 1) * 100,
            'above_ma': above_ma
        }
        
        return pivot_found, pivot_data
    
    def detect_breakout(self, df, window=60, consolidation_days=20, breakout_pct=3.0):
        """저항선 돌파 패턴 감지
        
        Args:
            df: 주가 데이터프레임
            window: 분석 기간
            consolidation_days: 횡보 기간
            breakout_pct: 돌파 기준 퍼센트
            
        Returns:
            breakout_found: 저항선 돌파 여부
            breakout_data: 패턴 관련 데이터
        """
        if df.empty or len(df) < window:
            return False, {}
        
        # 최근 데이터
        recent_df = df.tail(window).copy()
        
        # 횡보 구간 식별 (최근 consolidation_days 기간 동안의 최고가)
        if len(recent_df) < consolidation_days + 5:  # 충분한 데이터 확인
            return False, {}
            
        # 최근 횡보 구간의 최고가 저항선
        resistance = recent_df['High'].iloc[-(consolidation_days+5):-5].max()
        
        # 최근 5일 내에 저항선 돌파 여부
        latest = recent_df.iloc[-1]
        breakout_price = resistance * (1 + breakout_pct/100)
        
        # 돌파 조건
        close_breakout = latest['Close'] > resistance
        volume_surge = False
        
        if 'Volume_MA20' in recent_df.columns:
            volume_surge = latest['Volume'] > latest['Volume_MA20'] * 1.3
        
        # 최종 판정 (가격 돌파 + 거래량 증가)
        breakout_found = close_breakout and volume_surge
        
        breakout_data = {
            'resistance_level': resistance,
            'breakout_level': breakout_price,
            'price_to_resistance_ratio': latest['Close'] / resistance if resistance > 0 else 0,
            'volume_surge': volume_surge
        }
        
        return breakout_found, breakout_data 