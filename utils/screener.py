import pandas as pd
import numpy as np
import datetime
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from utils.stock_data import StockDataCollector
from utils.financial_data import FinancialDataCollector, NumpyEncoder
from utils.pattern_analyzer import PatternAnalyzer
from utils.score_calculator import ScoreCalculator

class SEPAScreener:
    def __init__(self, dart_api_key, cache_dir='data', max_workers=2):
        """SEPA 스크리너 초기화
        
        Args:
            dart_api_key: DART API 키
            cache_dir: 캐시 디렉토리
            max_workers: 병렬 처리 워커 수
        """
        self.stock_collector = StockDataCollector(cache_dir=cache_dir)
        self.financial_collector = FinancialDataCollector(api_key=dart_api_key, cache_dir=cache_dir)
        self.pattern_analyzer = PatternAnalyzer()
        self.score_calculator = ScoreCalculator()
        self.cache_dir = cache_dir
        self.max_workers = max_workers
        
        # 결과 저장 디렉토리
        self.results_dir = os.path.join(cache_dir, 'results')
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)
    
    def make_json_safe(self, value):
        """안전한 JSON 변환 헬퍼 함수"""
        if isinstance(value, dict):
            return {k: self.make_json_safe(v) for k, v in value.items()}
        elif isinstance(value, (list, tuple)):
            return [self.make_json_safe(item) for item in value]
        elif isinstance(value, np.ndarray):
            return value.tolist()
        elif isinstance(value, (np.int64, np.int32, np.float64, np.float32)):
            return float(value)
        elif value is None:
            return None
        else:
            return str(value) if not isinstance(value, (int, float, bool, str)) else value
    
    def get_market_data(self):
        """시장 데이터 가져오기"""
        # KOSPI ETF (069500)
        kospi_etf = '069500'
        # KOSDAQ ETF (229200)
        kosdaq_etf = '229200'
        
        try:
            kospi_data = self.stock_collector.get_market_data(kospi_etf, use_cache=False)
            kosdaq_data = self.stock_collector.get_market_data(kosdaq_etf, use_cache=False)
            
            if kospi_data is not None and not kospi_data.empty and kosdaq_data is not None and not kosdaq_data.empty:
                market_data = {
                    'KOSPI': kospi_data,
                    'KOSDAQ': kosdaq_data
                }
                return market_data
            else:
                print("❌ 시장 데이터를 가져올 수 없습니다.")
                return None
            
        except Exception as e:
            print(f"❌ 시장 데이터 가져오기 오류: {str(e)}")
            return None
    
    def process_single_stock(self, stock_code, stock_name, market, market_data, sector=None):
        """단일 종목에 대한 처리"""
        try:
            # 1. 주가 데이터 가져오기 (캐시 사용하지 않음)
            stock_data = self.stock_collector.get_stock_price(stock_code, period='1y', use_cache=False)
            if stock_data is None or stock_data.empty:
                return None
            
            # 2. 기술적 지표 계산
            try:
                stock_data = self.stock_collector.calculate_indicators(stock_data)
            except Exception as e:
                return None
            
            # 3. 패턴 분석
            try:
                vcp_result = self.pattern_analyzer.detect_vcp(stock_data)
                pivot_result = self.pattern_analyzer.detect_pocket_pivot(stock_data)
                breakout_result = self.pattern_analyzer.detect_breakout(stock_data)
                
            except Exception as e:
                # 기본값 설정
                vcp_result = (False, {})
                pivot_result = (False, {})
                breakout_result = (False, {})
            
            # 4. 점수 계산
            try:
                # 4.1 추세 점수
                trend_score, trend_details = self.score_calculator.calculate_trend_score(stock_data)
                
                # 4.2 패턴 점수
                pattern_score, pattern_details = self.score_calculator.calculate_pattern_score(
                    vcp_result, pivot_result, breakout_result
                )
                
                # 4.3 상대강도 점수
                if market not in market_data:
                    rs_score, rs_details = 0, {}
                else:
                    market_df = market_data[market]
                    rs_score, rs_details = self.score_calculator.calculate_rs_score(stock_data, market_df)
                
                # 4.4 펀더멘털 점수
                financial_data = self.financial_collector.get_financial_statement(stock_code)
                
                # 안전한 변환 적용
                if financial_data:
                    financial_data = self.make_json_safe(financial_data)
                    
                fundamental_score, fundamental_details = self.score_calculator.calculate_fundamental_score(financial_data)
                
                # 4.5 수급 점수
                investor_data = self.stock_collector.get_investor_data(stock_code)
                investor_score, investor_details = self.score_calculator.calculate_investor_score(investor_data)
                
                # 4.6 총점 계산
                total_score = self.score_calculator.calculate_total_score(
                    trend_score, pattern_score, rs_score, fundamental_score, investor_score
                )
                
            except Exception as e:
                return None
            
            # 5. 결과 정리
            try:
                # 데이터 유효성 확인
                latest_price = float(stock_data.iloc[-1]['Close']) if not stock_data.empty else 0.0
                latest_volume = float(stock_data.iloc[-1]['Volume']) if not stock_data.empty else 0.0
                
                # 가격 변화율 계산 (전일 대비)
                price_change = 0.0
                price_change_pct = 0.0
                if len(stock_data) >= 2:
                    prev_price = float(stock_data.iloc[-2]['Close'])
                    price_change = latest_price - prev_price
                    price_change_pct = (price_change / prev_price * 100) if prev_price > 0 else 0.0
                
                # 52주 고저가 대비 위치
                high_52w = float(stock_data.iloc[-1].get('52W_High', latest_price)) if not stock_data.empty else latest_price
                low_52w = float(stock_data.iloc[-1].get('52W_Low', latest_price)) if not stock_data.empty else latest_price
                
                # 52주 고저가 대비 현재 위치 (%)
                if high_52w > low_52w:
                    position_52w = ((latest_price - low_52w) / (high_52w - low_52w)) * 100
                else:
                    position_52w = 50.0  # 기본값
                
                # 패턴 결과 처리 - 튜플에서 데이터 추출
                vcp_found, vcp_data = vcp_result if isinstance(vcp_result, tuple) and len(vcp_result) >= 2 else (False, {})
                pivot_found, pivot_data = pivot_result if isinstance(pivot_result, tuple) and len(pivot_result) >= 2 else (False, {})
                breakout_found, breakout_data = breakout_result if isinstance(breakout_result, tuple) and len(breakout_result) >= 2 else (False, {})
                
                # 패턴 세부 정보 추가
                pattern_details.update({
                    'vcp_data': self.make_json_safe(vcp_data),
                    'pivot_data': self.make_json_safe(pivot_data),
                    'breakout_data': self.make_json_safe(breakout_data)
                })
                
                result = {
                    'code': str(stock_code),
                    'name': str(stock_name),
                    'market': str(market),
                    'sector': str(sector) if sector else '기타',
                    'current_price': round(latest_price, 0),  # 현재가 (정수로 표시)
                    'price_change': round(price_change, 0),   # 전일 대비 변화액
                    'price_change_pct': round(price_change_pct, 2),  # 전일 대비 변화율(%)
                    'volume': int(latest_volume),
                    'high_52w': round(high_52w, 0),          # 52주 고가
                    'low_52w': round(low_52w, 0),            # 52주 저가
                    'position_52w': round(position_52w, 1),   # 52주 고저가 대비 위치(%)
                    'total_score': round(float(total_score), 1),
                    'trend_score': round(float(trend_score), 1),
                    'pattern_score': round(float(pattern_score), 1),
                    'rs_score': round(float(rs_score), 1),
                    'fundamental_score': round(float(fundamental_score), 1),
                    'investor_score': round(float(investor_score), 1),
                    'vcp': bool(vcp_found),
                    'pivot': bool(pivot_found),
                    'breakout': bool(breakout_found),
                    'last_update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'details': {
                        'trend': self.make_json_safe(trend_details),
                        'pattern': self.make_json_safe(pattern_details),
                        'rs': self.make_json_safe(rs_details),
                        'fundamental': self.make_json_safe(fundamental_details),
                        'investor': self.make_json_safe(investor_details)
                    }
                }
                
                return result
                
            except Exception as e:
                return None
                
        except Exception as e:
            return None
    
    def run_screening(self, markets=None, total_score_threshold=0, market_cap_filter='large_cap'):
        """전체 스크리닝 실행
        
        Args:
            markets: 스크리닝할 시장 (None이면 모든 시장)
            total_score_threshold: 최소 총점 기준 (기본값: 0)
            market_cap_filter: 시가총액 필터 ('all', 'large_cap', 'top_300', 'top_500')
            
        Returns:
            results_df: 스크리닝 결과 데이터프레임
        """
        start_time = time.time()
        
        if markets is None:
            markets = ['KOSPI', 'KOSDAQ']
        
        # 시장 데이터 가져오기
        market_data = self.get_market_data()
        if market_data is None:
            print("❌ 시장 데이터를 가져올 수 없어 스크리닝을 중단합니다.")
            return pd.DataFrame()
        
        # 전체 종목 리스트 가져오기 (시가총액 필터 적용)
        all_stocks = self.stock_collector.get_all_stocks(market_cap_filter=market_cap_filter)
        if all_stocks.empty:
            print("❌ 종목 정보를 가져올 수 없어 스크리닝을 중단합니다.")
            return pd.DataFrame()
        
        # 필터링
        if markets:
            all_stocks = all_stocks[all_stocks['Market'].isin(markets)]
        
        if all_stocks.empty:
            print(f"❌ 선택한 시장({markets})에 해당하는 종목이 없습니다.")
            return pd.DataFrame()
        
        # 결과 저장 리스트
        all_results = []
        processed_count = 0
        success_count = 0
        error_count = 0
        
        # 종목 코드 정규화 (유효성 검사 완화)
        valid_stocks = []
        invalid_count = 0
        
        for _, row in all_stocks.iterrows():
            code = row['Code']
            name = row.get('Name', 'Unknown')
            
            if code and isinstance(code, str) and len(code) > 0:
                clean_code = str(code).strip()
                
                # 숫자로만 구성된 코드만 허용 (더 관대한 검사)
                if clean_code.replace('.', '').replace('-', '').isdigit():
                    # 6자리로 맞춤 (앞에 0 추가)
                    formatted_code = clean_code.replace('.', '').replace('-', '').zfill(6)
                    row_copy = row.copy()
                    row_copy['Code'] = formatted_code
                    valid_stocks.append(row_copy)
                else:
                    invalid_count += 1
            else:
                invalid_count += 1
        
        if valid_stocks and len(valid_stocks) >= 10:  # 최소 10개 이상의 유효한 종목이 있어야 함
            all_stocks = pd.DataFrame(valid_stocks)
        else:
            # 유효한 종목 부족 시 기본 종목 목록 사용
            # 기본 종목 목록 생성 (업종 정보 포함)
            default_stocks = [
                {'Code': '005930', 'Name': '삼성전자', 'Market': 'KOSPI', 'Sector': '전기전자'},
                {'Code': '000660', 'Name': 'SK하이닉스', 'Market': 'KOSPI', 'Sector': '전기전자'},
                {'Code': '035420', 'Name': 'NAVER', 'Market': 'KOSPI', 'Sector': '서비스업'},
                {'Code': '035720', 'Name': '카카오', 'Market': 'KOSPI', 'Sector': '서비스업'},
                {'Code': '005380', 'Name': '현대차', 'Market': 'KOSPI', 'Sector': '운수장비'},
                {'Code': '247540', 'Name': '에코프로비엠', 'Market': 'KOSDAQ', 'Sector': '전기전자'},
                {'Code': '086520', 'Name': '에코프로', 'Market': 'KOSDAQ', 'Sector': '전기전자'},
                {'Code': '091990', 'Name': '셀트리온헬스케어', 'Market': 'KOSDAQ', 'Sector': '의료정밀'},
                {'Code': '022100', 'Name': '포스코 DX', 'Market': 'KOSDAQ', 'Sector': '서비스업'},
                {'Code': '196170', 'Name': '알테오젠', 'Market': 'KOSDAQ', 'Sector': '의료정밀'}
            ]
            # 시장 필터링 적용
            if markets:
                default_stocks = [s for s in default_stocks if s['Market'] in markets]
            all_stocks = pd.DataFrame(default_stocks)
        
        total_stocks = len(all_stocks)
        
        # 전종목 분석 시 워커 수 조정 (안정성 우선)
        if total_stocks > 2000:
            worker_count = min(self.max_workers, 2)  # 대용량 분석 시 워커 수 제한
        elif total_stocks > 500:
            worker_count = min(self.max_workers, 3)  # 중간 규모 분석
        else:
            worker_count = min(self.max_workers, 1)
        
        # 멀티스레딩으로 처리
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {}
            
            for _, row in all_stocks.iterrows():
                code = row['Code']
                name = row['Name']
                market = row['Market']
                sector = row.get('Sector', '기타')  # 업종 정보 가져오기
                
                # 병렬 처리 작업 추가
                future = executor.submit(
                    self.process_single_stock, code, name, market, market_data, sector
                )
                futures[future] = (code, name)
            
            # 결과 수집 (안전한 처리)
            for future in as_completed(futures):
                code, name = futures[future]
                try:
                    # 타임아웃 설정 (각 종목당 최대 30초)
                    result = future.result(timeout=30)
                    
                    if result:
                        all_results.append(result)
                        success_count += 1
                    else:
                        error_count += 1
                    
                    processed_count += 1
                    
                    # 진행 상황 출력 (간소화)
                    if total_stocks > 2000:
                        update_interval = 200  # 대용량 분석 시 200개마다
                    elif total_stocks > 500:
                        update_interval = 100  # 중간 규모 시 100개마다
                    else:
                        update_interval = 50  # 소규모 시 50개마다
                        
                    if (processed_count % update_interval == 0) or (processed_count == total_stocks):
                        elapsed_time = time.time() - start_time
                        progress = (processed_count / total_stocks) * 100
                        remaining_time = (elapsed_time / processed_count) * (total_stocks - processed_count) if processed_count > 0 else 0
                        print(f"📊 진행: {processed_count}/{total_stocks} ({progress:.1f}%) - "
                              f"성공: {success_count}, 실패: {error_count} - "
                              f"남은시간: {remaining_time/60:.1f}분")
                        
                        # 메모리 정리 (대용량 분석 시)
                        if total_stocks > 1000 and processed_count % 100 == 0:
                            import gc
                            gc.collect()
                        
                except Exception as e:
                    error_count += 1
                    processed_count += 1
                    
                    # 너무 많은 오류 발생 시 중단
                    if error_count > total_stocks * 0.5:  # 50% 이상 실패 시
                        print(f"❌ 오류율이 너무 높습니다 ({error_count}/{processed_count}). 분석을 중단합니다.")
                        break
        
        # 점수 기준으로 필터링
        filtered_results = [r for r in all_results if r['total_score'] >= total_score_threshold]
        
        # 결과를 데이터프레임으로 변환
        if filtered_results:
            results_df = pd.DataFrame(filtered_results)
            
            # 점수 기준으로 정렬
            if not results_df.empty:
                results_df = results_df.sort_values(by='total_score', ascending=False)
            
            # 결과 저장
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            results_file = os.path.join(self.results_dir, f'screening_results_{timestamp}.csv')
            details_file = os.path.join(self.results_dir, f'screening_details_{timestamp}.json')
            
            # CSV 형태로 주요 결과 저장
            results_df.to_csv(results_file, index=False, encoding='utf-8-sig')
            
            # JSON 형태로 세부 결과 저장
            try:
                # 안전한 JSON 변환
                json_safe_results = []
                for result in filtered_results:
                    try:
                        simplified = {
                            'code': str(result.get('code', '')),
                            'name': str(result.get('name', '')),
                            'market': str(result.get('market', '')),
                            'price': float(result.get('price', 0)),
                            'volume': int(result.get('volume', 0)),
                            'total_score': float(result.get('total_score', 0)),
                            'trend_score': float(result.get('trend_score', 0)),
                            'pattern_score': float(result.get('pattern_score', 0)),
                            'rs_score': float(result.get('rs_score', 0)),
                            'fundamental_score': float(result.get('fundamental_score', 0)),
                            'vcp': bool(result.get('vcp', False)),
                            'pivot': bool(result.get('pivot', False)),
                            'breakout': bool(result.get('breakout', False)),
                            'last_update': str(result.get('last_update', '')),
                            'details': self.make_json_safe(result.get('details', {}))
                        }
                        json_safe_results.append(simplified)
                    except Exception as e:
                        pass  # JSON 변환 오류 메시지 제거
                
                # JSON 파일 저장
                with open(details_file, 'w', encoding='utf-8') as f:
                    json.dump(json_safe_results, f, ensure_ascii=False, indent=2)
                
            except Exception as e:
                print(f"❌ JSON 저장 오류: {e}")
            
            total_time = time.time() - start_time
            
            return results_df
        else:
            print("❌ 기준을 만족하는 종목이 없습니다.")
            return pd.DataFrame()
    
    def get_stock_chart_data(self, code, period=120):
        """차트 표시용 데이터 가져오기"""
        stock_data = self.stock_collector.get_stock_price(code, period='1y')
        if stock_data.empty:
            return None
        
        # 지표 계산
        stock_data = self.stock_collector.calculate_indicators(stock_data)
        
        # 최근 데이터만 잘라내기
        if len(stock_data) > period:
            chart_data = stock_data.tail(period).copy()
        else:
            chart_data = stock_data.copy()
        
        return chart_data 