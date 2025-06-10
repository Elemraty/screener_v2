import pandas as pd
import numpy as np

class ScoreCalculator:
    def __init__(self):
        """SEPA 점수 계산기 초기화"""
        pass
    
    def calculate_trend_score(self, stock_data):
        """주가 추세 점수 계산 (25점 만점)
        
        Args:
            stock_data: 기술 지표가 계산된 주가 데이터프레임
            
        Returns:
            trend_score: 추세 점수 (0-25점)
            trend_details: 세부 점수 항목
        """
        if stock_data.empty:
            return 0, {}
        
        # 가장 최근 데이터
        latest = stock_data.iloc[-1]
        
        # 초기 점수 설정
        score = 0.0
        details = {}
        
        # 1. 단기 이동평균선 정렬 (현재가 > MA20 > MA60) - 6점
        ma_alignment_score = 0
        if 'MA20' in stock_data.columns and 'MA60' in stock_data.columns:
            if latest['Close'] > latest['MA20']:
                ma_alignment_score += 3  # 현재가가 MA20 위에 있음
            if latest['MA20'] > latest['MA60']:
                ma_alignment_score += 3  # MA20이 MA60 위에 있음
            details['ma_alignment_score'] = ma_alignment_score
            score += ma_alignment_score / 6 * 6  # 6점 만점
        
        # 2. 중기 이동평균선 추세 (MA20, MA60, MA120 상승) - 6점
        ma_trend_score = 0
        if len(stock_data) >= 20:
            try:
                # 20일 전 대비 이동평균선 기울기
                if 'MA20' in stock_data.columns:
                    ma20_slope = (latest['MA20'] / stock_data['MA20'].iloc[-20] - 1) * 100
                    if ma20_slope > 0:
                        ma_trend_score += 2
                
                if 'MA60' in stock_data.columns and len(stock_data) >= 60:
                    ma60_slope = (latest['MA60'] / stock_data['MA60'].iloc[-min(60, len(stock_data)-1)] - 1) * 100
                    if ma60_slope > 0:
                        ma_trend_score += 2
                
                if 'MA120' in stock_data.columns and len(stock_data) >= 120:
                    ma120_slope = (latest['MA120'] / stock_data['MA120'].iloc[-min(120, len(stock_data)-1)] - 1) * 100
                    if ma120_slope > 0:
                        ma_trend_score += 2
                
                details['ma_trend_score'] = ma_trend_score
                score += ma_trend_score / 6 * 6  # 6점 만점
            except:
                details['ma_trend_score'] = 0
        
        # 3. 장기 추세 (52주 저가 대비 위치) - 6점
        long_trend_score = 0
        if '52W_Low' in stock_data.columns and latest['52W_Low'] > 0:
            price_to_low_ratio = latest['Close'] / latest['52W_Low']
            if price_to_low_ratio >= 1.5:  # 52주 저가 대비 50% 이상
                long_trend_score = 6
            elif price_to_low_ratio >= 1.3:  # 30% 이상
                long_trend_score = 4
            elif price_to_low_ratio >= 1.2:  # 20% 이상
                long_trend_score = 2
            
            details['price_to_52w_low'] = round(price_to_low_ratio, 2)
            details['long_trend_score'] = long_trend_score
            score += long_trend_score / 6 * 6  # 6점 만점
        
        # 4. 거래량 확인 (최근 거래량 vs 평균 거래량) - 7점
        volume_score = 0
        if 'Volume' in stock_data.columns and len(stock_data) >= 20:
            try:
                # 최근 5일 평균 거래량
                recent_5d_volume = stock_data['Volume'].tail(5).mean()
                # 20일 평균 거래량
                avg_volume_20 = stock_data['Volume'].tail(20).mean()
                # 60일 평균 거래량 (장기 평균)
                avg_volume_60 = stock_data['Volume'].tail(min(60, len(stock_data))).mean()
                
                # 거래량 증가율 계산
                volume_ratio_20d = recent_5d_volume / avg_volume_20 if avg_volume_20 > 0 else 0
                volume_ratio_60d = recent_5d_volume / avg_volume_60 if avg_volume_60 > 0 else 0
                
                # 거래량 점수 계산 (더 정교한 기준)
                if volume_ratio_20d >= 2.0:  # 20일 평균 대비 200% 이상
                    volume_score = 7
                elif volume_ratio_20d >= 1.5:  # 150% 이상
                    volume_score = 6
                elif volume_ratio_20d >= 1.3:  # 130% 이상  
                    volume_score = 5
                elif volume_ratio_20d >= 1.1:  # 110% 이상
                    volume_score = 4
                elif volume_ratio_20d >= 1.0:  # 평균 이상
                    volume_score = 3
                elif volume_ratio_20d >= 0.8:  # 80% 이상
                    volume_score = 2
                elif volume_ratio_20d >= 0.5:  # 50% 이상
                    volume_score = 1
                # 50% 미만은 0점
                
                # 장기 거래량 대비 보너스
                if volume_ratio_60d >= 1.5:
                    volume_score = min(volume_score + 1, 7)  # 최대 1점 보너스
                
                details['recent_5d_volume'] = int(recent_5d_volume)
                details['avg_volume_20d'] = int(avg_volume_20)
                details['avg_volume_60d'] = int(avg_volume_60)
                details['volume_ratio_20d'] = round(volume_ratio_20d, 2)
                details['volume_ratio_60d'] = round(volume_ratio_60d, 2)
                details['volume_score'] = volume_score
                score += volume_score
            except:
                details['volume_score'] = 0
        
        # 최종 점수 (25점 만점)
        trend_score = round(score, 1)
        
        return trend_score, details
    
    def calculate_pattern_score(self, vcp_result, pivot_result, breakout_result):
        """패턴 분석 점수 계산 (20점 만점)
        
        Args:
            vcp_result: VCP 분석 결과 (bool, dict)
            pivot_result: Pocket Pivot 분석 결과 (bool, dict)
            breakout_result: Breakout 분석 결과 (bool, dict)
            
        Returns:
            pattern_score: 패턴 점수 (0-20점)
            pattern_details: 세부 점수 항목
        """
        # 각 패턴별 가중치
        weights = {
            'vcp': 8,        # VCP 패턴 (8점)
            'pivot': 7,      # Pocket Pivot (7점)
            'breakout': 5    # Breakout (5점)
        }
        
        # 초기 점수 설정
        score = 0.0
        details = {}
        
        # VCP 패턴 (8점)
        vcp_found, vcp_data = vcp_result
        if vcp_found:
            score += weights['vcp']
            details['vcp_found'] = True
        else:
            details['vcp_found'] = False
        
        # 추가 VCP 데이터 저장
        if vcp_data:
            details['vcp_data'] = vcp_data
        
        # Pocket Pivot (7점)
        pivot_found, pivot_data = pivot_result
        if pivot_found:
            score += weights['pivot']
            details['pivot_found'] = True
        else:
            details['pivot_found'] = False
            
        # 추가 Pivot 데이터 저장
        if pivot_data:
            details['pivot_data'] = pivot_data
        
        # Breakout (5점)
        breakout_found, breakout_data = breakout_result
        if breakout_found:
            score += weights['breakout']
            details['breakout_found'] = True
        else:
            details['breakout_found'] = False
            
        # 추가 Breakout 데이터 저장
        if breakout_data:
            details['breakout_data'] = breakout_data
        
        # 최종 점수 (20점 만점)
        pattern_score = round(score, 1)
        
        return pattern_score, details
    
    def calculate_rs_score(self, stock_data, market_data, max_points=25):
        """상대강도(RS) 점수 계산 (25점 만점)
        
        Args:
            stock_data: 주가 데이터
            market_data: 시장 데이터 (코스피/코스닥 ETF)
            max_points: 최대 점수
            
        Returns:
            rs_score: 상대강도 점수 (0-25점)
            rs_details: 세부 점수 항목
        """
        if stock_data.empty or market_data.empty:
            return 0, {}
        
        # 초기값 설정
        score = 0.0
        details = {}
        
        # 최근 데이터
        stock_latest = stock_data.iloc[-1]
        market_latest = market_data.iloc[-1]
        
        # 13주 상대 수익률
        rs_13w = 0
        if 'Return_13W' in stock_latest and 'Return_13W' in market_latest:
            stock_return_13w = stock_latest['Return_13W']
            market_return_13w = market_latest['Return_13W']
            
            if not np.isnan(stock_return_13w) and not np.isnan(market_return_13w):
                rs_13w = stock_return_13w - market_return_13w
                
                # 13주 상대 수익률 점수 (최대 12.5점)
                rs_13w_score = min(max(rs_13w / 0.2, 0), 1) * 12.5  # 20% 이상이면 최고점
                score += rs_13w_score
                
                details['stock_return_13w'] = round(stock_return_13w * 100, 2)
                details['market_return_13w'] = round(market_return_13w * 100, 2)
                details['rs_13w'] = round(rs_13w * 100, 2)
                details['rs_13w_score'] = round(rs_13w_score, 1)
        
        # 26주 상대 수익률
        rs_26w = 0
        if 'Return_26W' in stock_latest and 'Return_26W' in market_latest:
            stock_return_26w = stock_latest['Return_26W']
            market_return_26w = market_latest['Return_26W']
            
            if not np.isnan(stock_return_26w) and not np.isnan(market_return_26w):
                rs_26w = stock_return_26w - market_return_26w
                
                # 26주 상대 수익률 점수 (최대 12.5점)
                rs_26w_score = min(max(rs_26w / 0.3, 0), 1) * 12.5  # 30% 이상이면 최고점
                score += rs_26w_score
                
                details['stock_return_26w'] = round(stock_return_26w * 100, 2)
                details['market_return_26w'] = round(market_return_26w * 100, 2)
                details['rs_26w'] = round(rs_26w * 100, 2)
                details['rs_26w_score'] = round(rs_26w_score, 1)
        
        # 최종 점수 (25점 만점)
        rs_score = round(score, 1)
        
        return rs_score, details
    
    def calculate_investor_score(self, investor_data, max_points=12):
        """외국인/기관 수급 점수 계산 (12점 만점, 공매도 정보 포함)
        
        Args:
            investor_data: 수급 데이터 딕셔너리 (공매도 정보 포함)
            max_points: 최대 점수
            
        Returns:
            investor_score: 수급 점수 (0-12점)
            investor_details: 세부 점수 항목
        """
        if not investor_data:
            return 0, {}
        
        # 데이터 소스 확인 - default인 경우 최소 점수만 부여
        data_source = investor_data.get('data_source', 'unknown')
        if data_source == 'default':
            # 기본값인 경우 최소 점수만 부여 (데이터 수집 실패를 의미)
            return 0.0, {
                'foreign_net_buy': 0,
                'foreign_ratio': 0,
                'institution_net_buy': 0,
                'institution_ratio': 0,
                'net_buy_days': 0,
                'short_selling_volume': 0,
                'short_selling_ratio': 0,
                'short_selling_balance': 0,
                'short_selling_days': 0,
                'data_source': data_source,
                'message': '투자자 데이터 수집 실패'
            }
        
        score = 0.0
        details = {}
        
        # 1. 외국인 순매수 점수 (최대 4점)
        foreign_net = investor_data.get('foreign_net_buy', 0)
        foreign_ratio = investor_data.get('foreign_ratio', 0)
        
        if foreign_net > 0:
            # 외국인 순매수 시 점수 부여
            if foreign_ratio >= 5:  # 5% 이상 순매수
                score += 4
            elif foreign_ratio >= 2:  # 2% 이상 순매수
                score += 3
            elif foreign_ratio >= 1:  # 1% 이상 순매수
                score += 2
            elif foreign_ratio > 0:  # 순매수
                score += 1
        
        details['foreign_net_buy'] = foreign_net
        details['foreign_ratio'] = round(foreign_ratio, 2)
        
        # 2. 기관 순매수 점수 (최대 4점)
        institution_net = investor_data.get('institution_net_buy', 0)
        institution_ratio = investor_data.get('institution_ratio', 0)
        
        if institution_net > 0:
            # 기관 순매수 시 점수 부여
            if institution_ratio >= 3:  # 3% 이상 순매수
                score += 4
            elif institution_ratio >= 1.5:  # 1.5% 이상 순매수
                score += 3
            elif institution_ratio >= 0.5:  # 0.5% 이상 순매수
                score += 2
            elif institution_ratio > 0:  # 순매수
                score += 1
        
        details['institution_net_buy'] = institution_net
        details['institution_ratio'] = round(institution_ratio, 2)
        
        # 3. 연속 순매수일 점수 (최대 2점) - 더 정교한 계산
        net_buy_days = investor_data.get('net_buy_days', 0)
        foreign_buy_days = investor_data.get('foreign_buy_days', 0)
        institution_buy_days = investor_data.get('institution_buy_days', 0)
        
        # 연속 매수일 점수 계산
        consecutive_score = 0
        if net_buy_days >= 15:  # 15일 이상 연속 매수
            consecutive_score = 2.0
        elif net_buy_days >= 10:  # 10일 이상
            consecutive_score = 1.5
        elif net_buy_days >= 5:   # 5일 이상
            consecutive_score = 1.0
        elif net_buy_days >= 3:   # 3일 이상
            consecutive_score = 0.5
        
        # 외국인과 기관이 모두 연속 매수 중인 경우 보너스
        if foreign_buy_days >= 3 and institution_buy_days >= 3:
            consecutive_score = min(consecutive_score + 0.5, 2.0)
        
        score += consecutive_score
        
        details['net_buy_days'] = net_buy_days
        details['foreign_buy_days'] = foreign_buy_days
        details['institution_buy_days'] = institution_buy_days
        details['consecutive_score'] = consecutive_score
        
        # 4. 공매도 점수 (최대 2점) - 공매도 비율이 낮을수록 좋음
        short_ratio = investor_data.get('short_selling_ratio', 0)
        short_volume = investor_data.get('short_selling_volume', 0)
        short_balance = investor_data.get('short_selling_balance', 0)
        short_days = investor_data.get('short_selling_days', 0)
        
        # 공매도 비율이 낮을수록 높은 점수
        if short_ratio <= 1:  # 1% 이하
            score += 2
        elif short_ratio <= 3:  # 3% 이하
            score += 1.5
        elif short_ratio <= 5:  # 5% 이하
            score += 1
        elif short_ratio <= 10:  # 10% 이하
            score += 0.5
        # 10% 초과시 0점
        
        details['short_selling_volume'] = short_volume
        details['short_selling_ratio'] = round(short_ratio, 2)
        details['short_selling_balance'] = short_balance
        details['short_selling_days'] = short_days
        details['data_source'] = data_source
        
        # 최종 점수 (12점 만점)
        investor_score = min(round(score, 1), max_points)
        
        return investor_score, details
    
    def calculate_fundamental_score(self, financial_data):
        """펀더멘털 점수 계산 (30점 만점) - f_data 활용 개선 버전
        
        Args:
            financial_data: 재무 데이터 딕셔너리
            
        Returns:
            tuple: (총점, 세부 점수 딕셔너리)
        """
        if not financial_data:
            return 0.0, {
                'profitability': {'roe': 0, 'operating_margin': 0, 'net_margin': 0},
                'growth': {'revenue_growth': 0, 'operating_profit_growth': 0, 'net_income_growth': 0},
                'stability': {'debt_ratio': 0},
                'valuation': {'per': 0, 'pbr': 0, 'dividend_yield': 0},
                'data_source': 'none',
                'message': '재무 데이터 없음'
            }
        
        # 안전한 숫자 변환 함수
        def safe_number(value, default=0.0):
            try:
                if value is None or value == '' or str(value).lower() in ['nan', 'none', 'null']:
                    return default
                return float(value)
            except (ValueError, TypeError):
                return default
        
        # 데이터 소스 확인
        data_source = financial_data.get('source', financial_data.get('data_source', 'unknown'))
        
        # 점수 계산 결과 저장
        scores = {}
        total_score = 0.0
        
        # f_data 소스별 처리
        if data_source == 'f_data_fundamental':
            # f_data fundamental_data (PER, PBR, ROE 등)
            
            # 재무 지표 추출
            roe = safe_number(financial_data.get('roe', 0))
            per = safe_number(financial_data.get('per', 0))
            pbr = safe_number(financial_data.get('pbr', 0))
            eps = safe_number(financial_data.get('eps', 0))
            bps = safe_number(financial_data.get('bps', 0))
            
            # 1. ROE 점수 (12점 만점)
            if roe >= 15:
                roe_score = 12.0
            elif roe >= 10:
                roe_score = 8.0
            elif roe >= 5:
                roe_score = 4.0
            elif roe > 0:
                roe_score = roe * 0.8
            else:
                roe_score = 0.0
            scores['roe'] = min(roe_score, 12.0)
            
            # 2. PER 점수 (9점 만점)
            if 0 < per <= 10:
                per_score = 9.0
            elif 10 < per <= 15:
                per_score = 6.0
            elif 15 < per <= 25:
                per_score = 3.0
            elif per > 25:
                per_score = 1.0
            else:
                per_score = 0.0
            scores['per'] = per_score
            
            # 3. PBR 점수 (9점 만점)
            if 0 < pbr <= 1.0:
                pbr_score = 9.0
            elif 1.0 < pbr <= 1.5:
                pbr_score = 6.0
            elif 1.5 < pbr <= 3.0:
                pbr_score = 3.0
            elif pbr > 3.0:
                pbr_score = 1.0
            else:
                pbr_score = 0.0
            scores['pbr'] = pbr_score
            
            total_score = sum(scores.values())
            
        elif data_source == 'f_data_financial':
            # f_data financial_data (분기별 실적)
            
            # 재무 지표 추출
            roe = safe_number(financial_data.get('roe', 0))
            debt_ratio = safe_number(financial_data.get('debt_ratio', 0))
            operating_margin = safe_number(financial_data.get('operating_margin', 0))
            net_margin = safe_number(financial_data.get('net_margin', 0))
            
            # 성장률 지표
            revenue_yoy = safe_number(financial_data.get('revenue_yoy', 0))
            operating_profit_yoy = safe_number(financial_data.get('operating_profit_yoy', 0))
            net_income_yoy = safe_number(financial_data.get('net_income_yoy', 0))
            
            # 1. ROE 점수 (8점 만점)
            if roe >= 15:
                roe_score = 8.0
            elif roe >= 10:
                roe_score = 6.0
            elif roe >= 5:
                roe_score = 4.0
            elif roe > 0:
                roe_score = roe * 0.8
            else:
                roe_score = 0.0
            scores['roe'] = min(roe_score, 8.0)
            
            # 2. 영업이익률 점수 (7점 만점)
            if operating_margin >= 10:
                op_margin_score = 7.0
            elif operating_margin >= 5:
                op_margin_score = 5.0
            elif operating_margin >= 2:
                op_margin_score = 3.0
            elif operating_margin > 0:
                op_margin_score = operating_margin * 1.5
            else:
                op_margin_score = 0.0
            scores['operating_margin'] = min(op_margin_score, 7.0)
            
            # 3. 매출 성장률 점수 (7점 만점)
            if revenue_yoy >= 20:
                revenue_growth_score = 7.0
            elif revenue_yoy >= 10:
                revenue_growth_score = 5.0
            elif revenue_yoy >= 5:
                revenue_growth_score = 3.0
            elif revenue_yoy > 0:
                revenue_growth_score = revenue_yoy * 0.6
            else:
                revenue_growth_score = 0.0
            scores['revenue_growth'] = min(revenue_growth_score, 7.0)
            
            # 4. 영업이익 성장률 점수 (6점 만점)
            if operating_profit_yoy >= 25:
                op_growth_score = 6.0
            elif operating_profit_yoy >= 15:
                op_growth_score = 4.0
            elif operating_profit_yoy >= 5:
                op_growth_score = 2.0
            elif operating_profit_yoy > 0:
                op_growth_score = operating_profit_yoy * 0.4
            else:
                op_growth_score = 0.0
            scores['operating_profit_growth'] = min(op_growth_score, 6.0)
            
            # 5. 부채비율 점수 (2점 만점)
            if debt_ratio <= 30:
                debt_score = 2.0
            elif debt_ratio <= 50:
                debt_score = 1.5
            elif debt_ratio <= 100:
                debt_score = 1.0
            elif debt_ratio <= 200:
                debt_score = 0.5
            else:
                debt_score = 0.0
            scores['debt_ratio'] = debt_score
            
            total_score = sum(scores.values())
            
        else:
            # 기존 로직 (다른 데이터 소스)
            # 1. 완전한 재무 데이터가 있는 경우 (ROE, 부채비율, 성장률 등 포함)
            if 'roe' in financial_data and 'debt_ratio' in financial_data:
                
                # 재무 지표 추출
                roe = safe_number(financial_data.get('roe', 0))
                debt_ratio = safe_number(financial_data.get('debt_ratio', 0))
                operating_margin = safe_number(financial_data.get('operating_margin', 0))
                net_margin = safe_number(financial_data.get('net_margin', 0))
                
                # 성장률 지표 (YoY - 전년 동기 대비)
                sales_yoy = safe_number(financial_data.get('sales_yoy', 0))
                op_income_yoy = safe_number(financial_data.get('op_income_yoy', 0))
                net_income_yoy = safe_number(financial_data.get('net_income_yoy', 0))
                
                # 성장률 지표 (QoQ - 전분기 대비)
                sales_qoq = safe_number(financial_data.get('sales_qoq', 0))
                op_income_qoq = safe_number(financial_data.get('op_income_qoq', 0))
                net_income_qoq = safe_number(financial_data.get('net_income_qoq', 0))
                
                # 1. 수익성 (ROE) - 8점
                if roe >= 15:  # 15% 이상
                    roe_score = 8.0
                elif roe >= 10:  # 10% 이상
                    roe_score = 6.0
                elif roe >= 5:  # 5% 이상
                    roe_score = 4.0
                elif roe > 0:  # 0% 초과
                    roe_score = roe * 0.8  # 0~5% 구간에서 선형 증가
                else:  # 0% 이하
                    roe_score = 0.0
                scores['roe'] = min(roe_score, 8.0)
                
                # 2. 수익성 (영업이익률) - 7점
                if operating_margin >= 10:  # 10% 이상
                    op_margin_score = 7.0
                elif operating_margin >= 5:  # 5% 이상
                    op_margin_score = 5.0
                elif operating_margin >= 2:  # 2% 이상
                    op_margin_score = 3.0
                elif operating_margin > 0:  # 0% 초과
                    op_margin_score = operating_margin * 1.5  # 0~2% 구간
                else:  # 0% 이하
                    op_margin_score = 0.0
                scores['operating_margin'] = min(op_margin_score, 7.0)
                
                # 3. 성장성 (매출 성장률) - 7점 (YoY 5점 + QoQ 2점)
                # YoY 매출 성장 (5점)
                if sales_yoy >= 20:  # 20% 이상
                    sales_yoy_score = 5.0
                elif sales_yoy >= 10:  # 10% 이상
                    sales_yoy_score = 4.0
                elif sales_yoy >= 5:  # 5% 이상
                    sales_yoy_score = 3.0
                elif sales_yoy > 0:  # 0% 초과
                    sales_yoy_score = sales_yoy * 0.6  # 0~5% 구간
                else:  # 0% 이하
                    sales_yoy_score = 0.0
                
                # QoQ 매출 성장 (2점) - 분기별이므로 기준 완화
                if sales_qoq >= 10:  # 10% 이상
                    sales_qoq_score = 2.0
                elif sales_qoq >= 5:  # 5% 이상
                    sales_qoq_score = 1.5
                elif sales_qoq > 0:  # 0% 초과
                    sales_qoq_score = sales_qoq * 0.3  # 0~5% 구간
                else:  # 0% 이하
                    sales_qoq_score = 0.0
                
                revenue_growth_score = min(sales_yoy_score + sales_qoq_score, 7.0)
                scores['revenue_growth'] = revenue_growth_score
                
                # 4. 성장성 (영업이익 성장률) - 6점 (YoY 4점 + QoQ 2점)
                # YoY 영업이익 성장 (4점)
                if op_income_yoy >= 25:  # 25% 이상
                    op_yoy_score = 4.0
                elif op_income_yoy >= 15:  # 15% 이상
                    op_yoy_score = 3.0
                elif op_income_yoy >= 5:  # 5% 이상
                    op_yoy_score = 2.0
                elif op_income_yoy > 0:  # 0% 초과
                    op_yoy_score = op_income_yoy * 0.4  # 0~5% 구간
                else:  # 0% 이하
                    op_yoy_score = 0.0
                
                # QoQ 영업이익 성장 (2점) - 분기별이므로 기준 완화
                if op_income_qoq >= 15:  # 15% 이상
                    op_qoq_score = 2.0
                elif op_income_qoq >= 5:  # 5% 이상
                    op_qoq_score = 1.0
                elif op_income_qoq > 0:  # 0% 초과
                    op_qoq_score = op_income_qoq * 0.2  # 0~5% 구간
                else:  # 0% 이하
                    op_qoq_score = 0.0
                
                op_growth_score = min(op_yoy_score + op_qoq_score, 6.0)
                scores['operating_profit_growth'] = op_growth_score
                
                # 5. 안정성 (부채비율) - 2점
                if debt_ratio <= 30:  # 30% 이하
                    debt_score = 2.0
                elif debt_ratio <= 50:  # 50% 이하
                    debt_score = 1.5
                elif debt_ratio <= 100:  # 100% 이하
                    debt_score = 1.0
                elif debt_ratio <= 200:  # 200% 이하
                    debt_score = 0.5
                else:  # 200% 초과
                    debt_score = 0.0
                scores['debt_ratio'] = debt_score
                
                # 총점 계산 (30점 만점)
                total_score = sum(scores.values())
                
            # 2. 기본적인 가치평가 지표만 있는 경우 (PER, PBR, 배당수익률)
            elif 'per' in financial_data or 'pbr' in financial_data:
                
                per = safe_number(financial_data.get('per', 0))
                pbr = safe_number(financial_data.get('pbr', 0))
                dividend_yield = safe_number(financial_data.get('dividend_yield', 0))
                
                # 데이터가 모두 0인 경우 최소 점수 부여
                if per == 0 and pbr == 0 and dividend_yield == 0:
                    total_score = 5.0  # 최소 점수
                    scores = {
                        'per': 0, 'pbr': 0, 'dividend_yield': 0,
                        'message': '가치평가 지표 데이터 부족'
                    }
                else:
                    # PER 점수 (12점 만점)
                    if 0 < per <= 10:  # 10배 이하 (저평가)
                        per_score = 12.0
                    elif 10 < per <= 15:  # 15배 이하 (적정)
                        per_score = 8.0
                    elif 15 < per <= 25:  # 25배 이하 (다소 고평가)
                        per_score = 4.0
                    elif per > 25:  # 25배 초과 (고평가)
                        per_score = 1.0
                    else:  # 0 이하 (손실 기업)
                        per_score = 0.0
                    scores['per'] = per_score
                    
                    # PBR 점수 (12점 만점)
                    if 0 < pbr <= 1.0:  # 1배 이하 (저평가)
                        pbr_score = 12.0
                    elif 1.0 < pbr <= 1.5:  # 1.5배 이하 (적정)
                        pbr_score = 8.0
                    elif 1.5 < pbr <= 3.0:  # 3배 이하 (다소 고평가)
                        pbr_score = 4.0
                    elif pbr > 3.0:  # 3배 초과 (고평가)
                        pbr_score = 1.0
                    else:  # 0 이하
                        pbr_score = 0.0
                    scores['pbr'] = pbr_score
                    
                    # 배당수익률 점수 (6점 만점)
                    if dividend_yield >= 4:  # 4% 이상
                        div_score = 6.0
                    elif dividend_yield >= 2:  # 2% 이상
                        div_score = 4.0
                    elif dividend_yield > 0:  # 0% 초과
                        div_score = dividend_yield * 2.0  # 0~2% 구간
                    else:  # 무배당
                        div_score = 0.0
                    scores['dividend_yield'] = div_score
                    
                    # 총점 계산 (30점 만점)
                    total_score = sum(scores.values())
            
            # 2. 기본적인 가치평가 지표만 있는 경우 (PER, PBR, 배당수익률)
            elif 'per' in financial_data or 'pbr' in financial_data:
                
                per = safe_number(financial_data.get('per', 0))
                pbr = safe_number(financial_data.get('pbr', 0))
                dividend_yield = safe_number(financial_data.get('dividend_yield', 0))
                
                # 데이터가 모두 0인 경우 최소 점수 부여
                if per == 0 and pbr == 0 and dividend_yield == 0:
                    total_score = 5.0  # 최소 점수
                    scores = {
                        'per': 0, 'pbr': 0, 'dividend_yield': 0,
                        'message': '가치평가 지표 데이터 부족'
                    }
                else:
                    # PER 점수 (12점 만점)
                    if 0 < per <= 10:  # 10배 이하 (저평가)
                        per_score = 12.0
                    elif 10 < per <= 15:  # 15배 이하 (적정)
                        per_score = 8.0
                    elif 15 < per <= 25:  # 25배 이하 (다소 고평가)
                        per_score = 4.0
                    elif per > 25:  # 25배 초과 (고평가)
                        per_score = 1.0
                    else:  # 0 이하 (손실 기업)
                        per_score = 0.0
                    scores['per'] = per_score
                    
                    # PBR 점수 (12점 만점)
                    if 0 < pbr <= 1.0:  # 1배 이하 (저평가)
                        pbr_score = 12.0
                    elif 1.0 < pbr <= 1.5:  # 1.5배 이하 (적정)
                        pbr_score = 8.0
                    elif 1.5 < pbr <= 3.0:  # 3배 이하 (다소 고평가)
                        pbr_score = 4.0
                    elif pbr > 3.0:  # 3배 초과 (고평가)
                        pbr_score = 1.0
                    else:  # 0 이하
                        pbr_score = 0.0
                    scores['pbr'] = pbr_score
                    
                    # 배당수익률 점수 (6점 만점)
                    if dividend_yield >= 4:  # 4% 이상
                        div_score = 6.0
                    elif dividend_yield >= 2:  # 2% 이상
                        div_score = 4.0
                    elif dividend_yield > 0:  # 0% 초과
                        div_score = dividend_yield * 2.0  # 0~2% 구간
                    else:  # 무배당
                        div_score = 0.0
                    scores['dividend_yield'] = div_score
                    
                    # 총점 계산 (30점 만점)
                    total_score = sum(scores.values())
            
            # 3. 데이터가 전혀 없는 경우
            else:
                total_score = 3.0  # 최소 기본 점수
                scores = {'message': '재무 데이터 부족'}
        
        # 최종 점수 (30점 만점)
        final_score = min(round(total_score, 1), 30.0)
        
        # 세부 점수 정리
        details = {
            'profitability': {
                'roe': scores.get('roe', 0),
                'operating_margin': scores.get('operating_margin', 0),
                'net_margin': scores.get('net_margin', 0)
            },
            'growth': {
                'revenue_growth': scores.get('revenue_growth', 0),
                'operating_profit_growth': scores.get('operating_profit_growth', 0),
                'net_income_growth': scores.get('net_income_growth', 0),
                'revenue_yoy': safe_number(financial_data.get('revenue_yoy', financial_data.get('sales_yoy', 0))),
                'operating_profit_yoy': safe_number(financial_data.get('operating_profit_yoy', financial_data.get('op_income_yoy', 0))),
                'net_income_yoy': safe_number(financial_data.get('net_income_yoy', 0))
            },
            'stability': {
                'debt_ratio': scores.get('debt_ratio', 0)
            },
            'valuation': {
                'per': scores.get('per', 0),
                'pbr': scores.get('pbr', 0),
                'dividend_yield': scores.get('dividend_yield', 0)
            },
            'data_source': data_source,
            'scores_detail': scores
        }
        
        return final_score, details
    
    def calculate_total_score(self, trend_score, pattern_score, rs_score, fundamental_score, investor_score=0):
        """총점 계산 (112점 만점 -> 100점으로 정규화)
        
        Args:
            trend_score: 추세 점수 (0-25점)
            pattern_score: 패턴 점수 (0-20점)
            rs_score: 상대강도 점수 (0-25점)
            fundamental_score: 펀더멘털 점수 (0-30점)
            investor_score: 수급 점수 (0-12점, 공매도 포함)
            
        Returns:
            total_score: 총점 (0-100점)
        """
        # 원점수 합계 (최대 112점)
        raw_total = trend_score + pattern_score + rs_score + fundamental_score + investor_score
        
        # 100점 만점으로 정규화
        total_score = min(round((raw_total / 112) * 100, 1), 100.0)
        
        return total_score 