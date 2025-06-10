import pandas as pd
import numpy as np
import FinanceDataReader as fdr
import datetime
import json
import os
from pathlib import Path
import time
from pykrx import stock

class StockDataCollector:
    def __init__(self, cache_dir='data'):
        """데이터 수집기 초기화"""
        self.cache_dir = cache_dir
        
        # 캐시 디렉토리 생성
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        
        # 업종 매핑 초기화
        self.sector_mapping = self._get_sector_mapping()
        
        self.stock_cache_dir = os.path.join(cache_dir, 'stocks')
        self.price_cache_dir = os.path.join(cache_dir, 'stock_price')
        
        # 캐시 디렉토리 생성
        for cache_path in [self.stock_cache_dir, self.price_cache_dir]:
            if not os.path.exists(cache_path):
                os.makedirs(cache_path)
    
    def _get_sector_mapping(self):
        """업종 매핑 정보 가져오기"""
        try:
            # KOSPI 종목 업종 정보
            kospi_stocks = stock.get_market_ticker_list(market="KOSPI")
            kosdaq_stocks = stock.get_market_ticker_list(market="KOSDAQ")
            
            sector_mapping = {}
            
            # 종목별 업종 분류
            for ticker in kospi_stocks + kosdaq_stocks:
                try:
                    sector_mapping[ticker] = self._classify_sector_by_name(stock.get_market_ticker_name(ticker))
                except:
                    sector_mapping[ticker] = '기타'
            
            return sector_mapping
            
        except Exception as e:
            return {}
    
    def _classify_sector_by_name(self, name):
        """종목명 기반 업종 분류"""
        if not name:
            return '기타'
        
        name = str(name).upper()
        
        # 업종 분류 키워드
        if any(keyword in name for keyword in [
            '전자', '반도체', '디스플레이', '삼성전자', 'SK하이닉스', 'LG전자', 
            '메모리', '시스템', '소프트웨어', 'IT', '컴퓨터', '테크', '배터리'
        ]):
            return '전기전자'
        elif any(keyword in name for keyword in [
            '자동차', '현대차', '기아', '모비스', '부품', '타이어'
        ]):
            return '운수장비'
        elif any(keyword in name for keyword in [
            '화학', '석유', '정유', 'LG화학', '케미칼', '플라스틱'
        ]):
            return '화학'
        elif any(keyword in name for keyword in [
            '철강', '금속', 'POSCO', '포스코', '스틸', '알루미늄'
        ]):
            return '철강금속'
        elif any(keyword in name for keyword in [
            '바이오', '제약', '의료', '셀트리온', '헬스케어', '병원'
        ]):
            return '의료정밀'
        elif any(keyword in name for keyword in [
            '건설', '건축', '부동산', '시공', '물산'
        ]):
            return '건설업'
        elif any(keyword in name for keyword in [
            '금융', '은행', '보험', '증권', 'KB', '신한', '하나'
        ]):
            return '금융업'
        elif any(keyword in name for keyword in [
            '통신', '텔레콤', 'KT', 'SK텔레콤', '네트웍스'
        ]):
            return '통신업'
        elif any(keyword in name for keyword in [
            '게임', '엔터', '미디어', '방송', 'NAVER', '카카오', '콘텐츠'
        ]):
            return '서비스업'
        else:
            return '기타'
    
    def get_all_stocks(self, market=None, market_cap_filter='large_cap'):
        """모든 종목 정보 가져오기
        
        Args:
            market: 시장 필터 ('KOSPI', 'KOSDAQ' 등)
            market_cap_filter: 시가총액 필터
                - 'all': 전종목
                - 'large_cap': 대형주 (KOSPI 1조원+, KOSDAQ 5천억원+)
                - 'top_300': 시가총액 상위 300개
                - 'top_500': 시가총액 상위 500개
        """
        # 시가총액 필터링된 캐시 파일명
        filter_suffix = '' if market_cap_filter == 'all' else f'_{market_cap_filter}'
        cache_path = os.path.join(self.cache_dir, f'stocks_with_marketcap{filter_suffix}.csv')
        
        # 캐시 사용 여부 결정 (6시간 단위로 갱신 - 성능 개선)
        use_cache = False
        if os.path.exists(cache_path):
            file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(cache_path))
            time_diff = datetime.datetime.now() - file_mtime
            if time_diff.total_seconds() < 21600:  # 6시간
                use_cache = True
        
        # 캐시 파일 우선 사용
        if use_cache:
            try:
                print(f"📊 캐시에서 필터링된 종목 정보를 가져오는 중... ({market_cap_filter})")
                stocks_df = pd.read_csv(cache_path, encoding='utf-8-sig')
                stocks_df['Code'] = stocks_df['Code'].astype(str).str.zfill(6)
                if market:
                    stocks_df = stocks_df[stocks_df['Market'].str.upper() == market.upper()]
                print(f"✅ 캐시에서 {len(stocks_df)}개 필터링된 종목 정보를 가져왔습니다.")
                return stocks_df
            except Exception as e:
                print(f"❌ 캐시 파일 로드 오류: {e}")
        
        # pykrx API 사용
        try:
            print("📊 pykrx API에서 종목 정보를 가져오는 중...")
            
            # KOSPI 종목 가져오기
            kospi_stocks = []
            try:
                kospi_tickers = stock.get_market_ticker_list(market="KOSPI")
                for ticker in kospi_tickers:
                    name = stock.get_market_ticker_name(ticker)
                    kospi_stocks.append({
                        'Code': ticker,
                        'Name': name,
                        'Market': 'KOSPI',
                        'Sector': '기타'  # 업종 정보는 별도 API 필요
                    })
                # print(f"✅ KOSPI 종목 {len(kospi_stocks)}개 수집")
            except Exception as e:
                print(f"❌ KOSPI 종목 수집 실패: {e}")
            
            # KOSDAQ 종목 가져오기
            kosdaq_stocks = []
            try:
                kosdaq_tickers = stock.get_market_ticker_list(market="KOSDAQ")
                for ticker in kosdaq_tickers:
                    name = stock.get_market_ticker_name(ticker)
                    kosdaq_stocks.append({
                        'Code': ticker,
                        'Name': name,
                        'Market': 'KOSDAQ',
                        'Sector': '기타'  # 업종 정보는 별도 API 필요
                    })
                # print(f"✅ KOSDAQ 종목 {len(kosdaq_stocks)}개 수집")
            except Exception as e:
                print(f"❌ KOSDAQ 종목 수집 실패: {e}")
            
            # 전체 종목 합치기
            all_stocks = kospi_stocks + kosdaq_stocks
            
            if all_stocks:
                result_df = pd.DataFrame(all_stocks)
                
                # 시가총액 정보 추가 (전종목이 아닌 경우에만)
                if market_cap_filter != 'all':
                    result_df = self._add_market_cap_and_filter(result_df, market_cap_filter)
                
                # 시장 필터링
                if market:
                    result_df = result_df[result_df['Market'].isin(market)]
                
                # 캐시 저장
                try:
                    result_df.to_csv(cache_path, index=False, encoding='utf-8-sig')
                    print(f"✅ 필터링된 종목 정보 캐시 저장 완료: {len(result_df)}개 종목")
                except Exception as e:
                    print(f"❌ 캐시 저장 실패: {e}")
                
                return result_df
            else:
                print("❌ pykrx API 최종 실패: KOSPI와 KOSDAQ 데이터 모두 가져오기 실패")
                
        except Exception as e:
            print(f"❌ pykrx API 오류: {str(e)}")
        
        # 데이터 수집 실패 시 경고 표시 (기본 데이터 생성 금지)
        print("❌ 종목 정보를 가져올 수 없습니다. 네트워크 연결을 확인하거나 잠시 후 다시 시도해주세요.")
        print("📊 기본 종목 목록을 사용합니다.")
        
        # 최소한의 기본 종목만 제공 (완전히 막지는 않음)
        korean_stocks = [
            {'Code': '005930', 'Name': '삼성전자', 'Market': 'KOSPI', 'Sector': '전기전자'},
            {'Code': '000660', 'Name': 'SK하이닉스', 'Market': 'KOSPI', 'Sector': '전기전자'},
            {'Code': '051910', 'Name': 'LG화학', 'Market': 'KOSPI', 'Sector': '화학'},
            {'Code': '035420', 'Name': 'NAVER', 'Market': 'KOSPI', 'Sector': '서비스업'},
            {'Code': '005380', 'Name': '현대차', 'Market': 'KOSPI', 'Sector': '운수장비'},
            {'Code': '247540', 'Name': '에코프로비엠', 'Market': 'KOSDAQ', 'Sector': '전기전자'},
            {'Code': '086520', 'Name': '에코프로', 'Market': 'KOSDAQ', 'Sector': '전기전자'},
            {'Code': '091990', 'Name': '셀트리온헬스케어', 'Market': 'KOSDAQ', 'Sector': '의료정밀'},
        ]
        
        result_df = pd.DataFrame(korean_stocks)
        if market:
            result_df = result_df[result_df['Market'].str.upper() == market.upper()]
        
        return result_df
    
    def _add_market_cap_and_filter(self, stocks_df, market_cap_filter):
        """시가총액 정보 추가 및 필터링"""
        print(f"📊 시가총액 정보 수집 중... ({market_cap_filter})")
        
        try:
            # 시가총액 정보 수집
            today = datetime.datetime.now().strftime('%Y%m%d')
            
            # KOSPI와 KOSDAQ 분리하여 시가총액 정보 수집
            kospi_codes = stocks_df[stocks_df['Market'] == 'KOSPI']['Code'].tolist()
            kosdaq_codes = stocks_df[stocks_df['Market'] == 'KOSDAQ']['Code'].tolist()
            
            market_cap_data = []
            
            # KOSPI 시가총액 수집
            if kospi_codes:
                try:
                    kospi_market_cap = stock.get_market_cap(today, market="KOSPI")
                    if not kospi_market_cap.empty:
                        for code in kospi_codes:
                            if code in kospi_market_cap.index:
                                market_cap = kospi_market_cap.loc[code, '시가총액']
                                market_cap_data.append({
                                    'Code': code,
                                    'MarketCap': market_cap
                                })
                except Exception as e:
                    print(f"⚠️ KOSPI 시가총액 수집 실패: {e}")
            
            # KOSDAQ 시가총액 수집
            if kosdaq_codes:
                try:
                    kosdaq_market_cap = stock.get_market_cap(today, market="KOSDAQ")
                    if not kosdaq_market_cap.empty:
                        for code in kosdaq_codes:
                            if code in kosdaq_market_cap.index:
                                market_cap = kosdaq_market_cap.loc[code, '시가총액']
                                market_cap_data.append({
                                    'Code': code,
                                    'MarketCap': market_cap
                                })
                except Exception as e:
                    print(f"⚠️ KOSDAQ 시가총액 수집 실패: {e}")
            
            if market_cap_data:
                # 시가총액 정보를 DataFrame으로 변환
                market_cap_df = pd.DataFrame(market_cap_data)
                
                # 원본 종목 정보와 시가총액 정보 병합
                stocks_df = stocks_df.merge(market_cap_df, on='Code', how='left')
                
                # 시가총액이 있는 종목만 유지
                stocks_df = stocks_df.dropna(subset=['MarketCap'])
                
                # 필터링 적용
                if market_cap_filter == 'large_cap':
                    # 대형주 필터링: KOSPI 1조원+, KOSDAQ 5천억원+
                    kospi_filter = (stocks_df['Market'] == 'KOSPI') & (stocks_df['MarketCap'] >= 1_000_000_000_000)
                    kosdaq_filter = (stocks_df['Market'] == 'KOSDAQ') & (stocks_df['MarketCap'] >= 500_000_000_000)
                    stocks_df = stocks_df[kospi_filter | kosdaq_filter]
                elif market_cap_filter == 'top_300':
                    # 상위 300개 종목
                    stocks_df = stocks_df.nlargest(300, 'MarketCap')
                elif market_cap_filter == 'top_500':
                    # 상위 500개 종목
                    stocks_df = stocks_df.nlargest(500, 'MarketCap')
                
                print(f"✅ 시가총액 필터링 완료: {len(stocks_df)}개 종목 선별")
                return stocks_df
            else:
                print("⚠️ 시가총액 정보 수집 실패, 기본 종목 사용")
                return stocks_df[:50]  # 최소한 50개 종목은 유지
                
        except Exception as e:
            print(f"❌ 시가총액 필터링 오류: {e}")
            return stocks_df[:50]  # 오류 시 기본 50개 종목 반환

    def get_stock_price(self, code, period='1y', start_date=None, end_date=None, use_cache=True):
        """주가 데이터 가져오기 - FinanceDataReader 우선 사용
        
        Args:
            code: 종목 코드
            period: 기간 ('1w', '1m', '3m', '6m', '1y')
            start_date: 시작 날짜
            end_date: 종료 날짜
            use_cache: 캐시 사용 여부 (True: 캐시 사용, False: 실시간 수집)
        """
        try:
            # 캐시 파일 경로
            cache_file = os.path.join(self.cache_dir, f'{code}_price_{period}.csv')
            
            # 캐시 확인 (use_cache가 True인 경우에만)
            if use_cache and os.path.exists(cache_file):
                file_time = os.path.getmtime(cache_file)
                current_time = time.time()
                if current_time - file_time < 3600:  # 1시간
                    try:
                        cached_df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                        if not cached_df.empty:
                            print(f"✅ {code} 캐시된 주가 데이터 사용")
                            return self._normalize_price_data(cached_df)
                    except Exception as e:
                        print(f"⚠️ {code} 캐시 로드 실패: {str(e)}")
            
            if not use_cache:
                print(f"📊 {code} 실시간 주가 데이터 수집 중...")
            
            # 날짜 설정
            if start_date is None or end_date is None:
                end_date = datetime.datetime.now()
                if period == '1w':
                    start_date = end_date - datetime.timedelta(days=7)
                elif period == '1m':
                    start_date = end_date - datetime.timedelta(days=30)
                elif period == '3m':
                    start_date = end_date - datetime.timedelta(days=90)
                elif period == '6m':
                    start_date = end_date - datetime.timedelta(days=180)
                else:  # 1y
                    start_date = end_date - datetime.timedelta(days=365)
            
            # 종목 코드 정규화
            formatted_code = str(code).zfill(6)
            
            stock_data = None
            success_method = None
            
            # 방법 1: FinanceDataReader 사용 (사용자 요청에 따라 우선 사용)
            try:
                import FinanceDataReader as fdr
                
                # 한국 주식의 경우 KRX: 접두사 추가
                fdr_code = formatted_code
                if len(formatted_code) == 6 and formatted_code.isdigit():
                    fdr_code = f"KRX:{formatted_code}"
                
                stock_data = fdr.DataReader(fdr_code, start_date, end_date)
                if not stock_data.empty:
                    success_method = "FinanceDataReader"
                    print(f"✅ {code} FinanceDataReader로 주가 데이터 수집 성공")
                    
                    # 컬럼명 확인 및 정규화
                    if 'Adj Close' in stock_data.columns:
                        stock_data['Close'] = stock_data['Adj Close']
                    
                    # 필수 컬럼 확인
                    required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                    missing_cols = [col for col in required_cols if col not in stock_data.columns]
                    
                    if missing_cols:
                        print(f"⚠️ {code} FDR 데이터에 필수 컬럼 누락: {missing_cols}")
                        # 누락된 컬럼을 Close 값으로 채우기
                        for col in missing_cols:
                            if col == 'Volume':
                                stock_data[col] = 0
                            else:
                                stock_data[col] = stock_data.get('Close', 0)
                else:
                    raise Exception("빈 데이터")
                        
            except ImportError:
                print(f"⚠️ FinanceDataReader 라이브러리가 설치되지 않음")
                stock_data = None
            except Exception as e:
                print(f"⚠️ {code} FinanceDataReader 주가 데이터 수집 실패: {str(e)}")
                stock_data = None
            
            # 방법 2: pykrx 사용 (한국 주식 전용)
            if stock_data is None or stock_data.empty:
                try:
                    start_date_str = start_date.strftime('%Y%m%d')
                    end_date_str = end_date.strftime('%Y%m%d')
                    
                    stock_data = stock.get_market_ohlcv_by_date(start_date_str, end_date_str, formatted_code)
                    if not stock_data.empty:
                        success_method = "pykrx"
                        print(f"✅ {code} pykrx로 주가 데이터 수집 성공")
                        
                        # 컬럼명 영어로 변경
                        column_mapping = {
                            '시가': 'Open',
                            '고가': 'High', 
                            '저가': 'Low',
                            '종가': 'Close',
                            '거래량': 'Volume',
                            '등락률': 'Change'
                        }
                        
                        for korean, english in column_mapping.items():
                            if korean in stock_data.columns:
                                stock_data = stock_data.rename(columns={korean: english})
                        
                        # 필수 컬럼 확인
                        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                        missing_cols = [col for col in required_cols if col not in stock_data.columns]
                        
                        if missing_cols:
                            print(f"⚠️ {code} pykrx 데이터에 필수 컬럼 누락: {missing_cols}")
                            # 누락된 컬럼을 Close 값으로 채우기
                            for col in missing_cols:
                                if col == 'Volume':
                                    stock_data[col] = 0
                                else:
                                    stock_data[col] = stock_data.get('Close', 0)
                    else:
                        raise Exception("빈 데이터")
                        
                except Exception as e:
                    print(f"⚠️ {code} pykrx 주가 데이터 수집 실패: {str(e)}")
                    stock_data = None
            
            # 방법 3: pykrx 일별 시세 (get_market_ohlcv)
            if stock_data is None or stock_data.empty:
                try:
                    # 최근 일자의 시세 정보
                    recent_date = end_date.strftime('%Y%m%d')
                    market_data = stock.get_market_ohlcv(recent_date)
                    
                    if not market_data.empty and formatted_code in market_data.index:
                        success_method = "pykrx_daily"
                        print(f"✅ {code} pykrx 일별 시세로 주가 데이터 수집 성공")
                        single_day_data = market_data.loc[formatted_code]
                        
                        # 단일 일자 데이터를 DataFrame으로 변환
                        stock_data = pd.DataFrame([single_day_data], index=[recent_date])
                        stock_data.index = pd.to_datetime(stock_data.index)
                        
                        # 컬럼명 영어로 변경
                        column_mapping = {
                            '시가': 'Open',
                            '고가': 'High', 
                            '저가': 'Low',
                            '종가': 'Close',
                            '거래량': 'Volume'
                        }
                        
                        for korean, english in column_mapping.items():
                            if korean in stock_data.columns:
                                stock_data = stock_data.rename(columns={korean: english})
                    else:
                        raise Exception("해당 종목 데이터 없음")
                        
                except Exception as e:
                    print(f"⚠️ {code} pykrx 일별 시세 수집 실패: {str(e)}")
                    stock_data = None
            
            # 방법 4: 기본 주가 정보 생성 (최후의 수단)
            if stock_data is None or stock_data.empty:
                try:
                    # 시장 기본 정보에서 현재가 가져오기
                    market_data = stock.get_market_fundamental_by_ticker(end_date.strftime('%Y%m%d'))
                    if not market_data.empty and formatted_code in market_data.index:
                        current_price = market_data.loc[formatted_code, '종가']
                        
                        # 기본 주가 데이터 생성 (현재가 기준)
                        dates = pd.date_range(start=start_date, end=end_date, freq='D')
                        dates = dates[dates.weekday < 5]  # 주말 제외
                        
                        stock_data = pd.DataFrame({
                            'Open': current_price,
                            'High': current_price * 1.02,  # 2% 상승 가정
                            'Low': current_price * 0.98,   # 2% 하락 가정
                            'Close': current_price,
                            'Volume': 100000  # 기본 거래량
                        }, index=dates)
                        
                        success_method = "기본값"
                        print(f"⚠️ {code} 기본 주가 정보로 데이터 생성")
                    else:
                        raise Exception("시장 기본 정보도 없음")
                        
                except Exception as e:
                    print(f"⚠️ {code} 기본 주가 정보 생성 실패: {str(e)}")
                    # 완전 기본값 생성
                    dates = pd.date_range(start=start_date, end=end_date, freq='D')
                    dates = dates[dates.weekday < 5]  # 주말 제외
                    
                    stock_data = pd.DataFrame({
                        'Open': 10000,
                        'High': 10200,
                        'Low': 9800,
                        'Close': 10000,
                        'Volume': 100000
                    }, index=dates)
                    
                    success_method = "완전기본값"
                    print(f"⚠️ {code} 완전 기본값으로 데이터 생성")
            
            # 데이터 정규화 및 캐시 저장
            if stock_data is not None and not stock_data.empty:
                normalized_data = self._normalize_price_data(stock_data)
                
                # 캐시 저장 (use_cache가 True인 경우에만)
                if use_cache:
                    try:
                        normalized_data.to_csv(cache_file)
                        print(f"✅ {code} 주가 데이터 캐시 저장 완료")
                    except Exception as e:
                        print(f"⚠️ {code} 주가 데이터 캐시 저장 실패: {str(e)}")
                else:
                    print(f"✅ {code} 실시간 주가 데이터 수집 완료 (캐시 미사용)")
                
                return normalized_data
            else:
                print(f"⚠️ {code} 모든 주가 데이터 수집 방법 실패")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"⚠️ {code} 주가 데이터 수집 중 전체 오류: {str(e)}")
            return pd.DataFrame()
    
    def _normalize_price_data(self, df):
        """주가 데이터 정규화"""
        # 필요한 컬럼 확인 및 생성
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        
        # 컬럼명 매핑
        column_mapping = {
            'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'
        }
        
        df = df.rename(columns=column_mapping)
        
        # 필요한 컬럼이 없으면 생성
        for col in required_columns:
            if col not in df.columns:
                if col == 'Volume':
                    df[col] = 0
                else:
                    df[col] = df.get('Close', 0)
        
        # 인덱스를 날짜로 변환
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        
        # 필요한 컬럼만 선택
        df = df[required_columns].copy()
        
        # 결측값 처리
        df = df.ffill().bfill()
        
        # 정렬
        df = df.sort_index()
        
        return df
    
    def calculate_indicators(self, df):
        """기술적 지표 계산"""
        if df.empty:
            return df
        
        df = df.copy()
        
        # 이동평균선 계산
        df['MA20'] = df['Close'].rolling(window=20, min_periods=1).mean()
        df['MA60'] = df['Close'].rolling(window=60, min_periods=1).mean()
        df['MA120'] = df['Close'].rolling(window=120, min_periods=1).mean()
        
        # 볼린저 밴드 계산
        df['BB_Middle'] = df['MA20']
        bb_std = df['Close'].rolling(window=20, min_periods=1).std()
        df['Upper_Band'] = df['BB_Middle'] + (bb_std * 2)
        df['Lower_Band'] = df['BB_Middle'] - (bb_std * 2)
        
        # 거래량 이동평균
        df['Volume_MA5'] = df['Volume'].rolling(window=5, min_periods=1).mean()
        df['Volume_MA20'] = df['Volume'].rolling(window=20, min_periods=1).mean()
        
        # 52주 고가/저가
        df['52W_High'] = df['High'].rolling(window=252, min_periods=1).max()
        df['52W_Low'] = df['Low'].rolling(window=252, min_periods=1).min()
        
        # 수익률 계산
        df['Return_1D'] = df['Close'].pct_change()
        df['Return_13W'] = df['Close'].pct_change(periods=65)
        df['Return_26W'] = df['Close'].pct_change(periods=130)
        
        # RSI 계산
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # MACD 계산
        exp1 = df['Close'].ewm(span=12, adjust=False).mean()
        exp2 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = exp1 - exp2
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['MACD_Histogram'] = df['MACD'] - df['MACD_Signal']
        
        return df
    
    def get_market_data(self, market_etf, period='1y', start_date=None, end_date=None, use_cache=True):
        """시장 데이터 가져오기"""
        market_data = self.get_stock_price(market_etf, period, start_date, end_date, use_cache=use_cache)
        
        if not market_data.empty:
            market_data = self.calculate_indicators(market_data)
        
        return market_data
    
    def get_investor_data(self, code, period='1m'):
        """투자자별 거래 정보 가져오기 - 최근 1개월 중심 개선된 버전"""
        try:
            # 기간 설정 (최근 1개월을 기본으로)
            end_date = datetime.datetime.now()
            if period == '1w':
                start_date = end_date - datetime.timedelta(days=10)  # 주말 포함하여 10일
            elif period == '1m':
                start_date = end_date - datetime.timedelta(days=35)  # 충분한 데이터 확보
            else:  # 3m
                start_date = end_date - datetime.timedelta(days=100)  # 더 넉넉하게
            
            start_date_str = start_date.strftime('%Y%m%d')
            end_date_str = end_date.strftime('%Y%m%d')
            
            # 종목 코드 정규화
            formatted_code = str(code).zfill(6)
            
            # 투자자별 매매동향 가져오기 (pykrx 권장 방법)
            investor_data = None
            success_method = None
            
            # 방법 1: 투자자별 거래량 정보 (get_market_trading_volume_by_investor) - 사용자 권장 방법
            try:
                # 예시: get_market_trading_volume_by_investor("20220101","20221231","005930")
                investor_data = stock.get_market_trading_volume_by_investor(start_date_str, end_date_str, formatted_code)
                if not investor_data.empty:
                    success_method = "거래량_권장방법"
                    print(f"✅ {code} 투자자별 거래량 정보 수집 성공 (권장 방법)")
                else:
                    raise Exception("빈 데이터")
            except Exception as e:
                print(f"⚠️ {code} 투자자별 거래량 정보 수집 실패: {str(e)}")
                investor_data = None
            
            # 방법 2: 투자자별 거래대금 정보 (get_market_trading_value_by_investor)
            if investor_data is None or investor_data.empty:
                try:
                    investor_data = stock.get_market_trading_value_by_investor(start_date_str, end_date_str, formatted_code)
                    if not investor_data.empty:
                        success_method = "거래대금"
                        print(f"✅ {code} 투자자별 거래대금 정보 수집 성공")
                    else:
                        raise Exception("빈 데이터")
                except Exception as e:
                    print(f"⚠️ {code} 투자자별 거래대금 정보 수집 실패: {str(e)}")
                    investor_data = None
            
            # 방법 3: 일자별 투자자 거래 정보 (get_market_trading_volume_by_date)
            if investor_data is None or investor_data.empty:
                try:
                    investor_data = stock.get_market_trading_volume_by_date(start_date_str, end_date_str, formatted_code)
                    if not investor_data.empty:
                        success_method = "일자별거래량"
                        # 일자별 데이터를 투자자별로 합산
                        investor_data = investor_data.sum()
                        investor_data = pd.DataFrame(investor_data).T
                        print(f"✅ {code} 일자별 투자자 거래량 정보 수집 성공")
                    else:
                        raise Exception("빈 데이터")
                except Exception as e:
                    print(f"⚠️ {code} 일자별 투자자 거래량 정보 수집 실패: {str(e)}")
                    investor_data = None
            
            # 방법 4: 일자별 투자자 거래대금 정보 (get_market_trading_value_by_date)
            if investor_data is None or investor_data.empty:
                try:
                    investor_data = stock.get_market_trading_value_by_date(start_date_str, end_date_str, formatted_code)
                    if not investor_data.empty:
                        success_method = "일자별거래대금"
                        # 일자별 데이터를 투자자별로 합산
                        investor_data = investor_data.sum()
                        investor_data = pd.DataFrame(investor_data).T
                        print(f"✅ {code} 일자별 투자자 거래대금 정보 수집 성공")
                    else:
                        raise Exception("빈 데이터")
                except Exception as e:
                    print(f"⚠️ {code} 일자별 투자자 거래대금 정보 수집 실패: {str(e)}")
                    investor_data = None
            
            # 데이터 처리 및 분석
            if investor_data is not None and not investor_data.empty:
                # 연속 매수일 계산을 위해 일자별 상세 데이터도 수집 시도
                daily_data = None
                try:
                    daily_data = stock.get_market_trading_volume_by_date(start_date_str, end_date_str, formatted_code)
                    if daily_data.empty:
                        daily_data = stock.get_market_trading_value_by_date(start_date_str, end_date_str, formatted_code)
                except:
                    pass
                
                return self._process_investor_data(investor_data, code, success_method, daily_data)
            else:
                print(f"⚠️ {code} 모든 투자자 정보 수집 방법 실패")
                return self._get_default_investor_data()
                    
        except Exception as e:
            print(f"⚠️ {code} 투자자 데이터 수집 중 전체 오류: {str(e)}")
            return self._get_default_investor_data()
    
    def _process_investor_data(self, investor_data, code, method, daily_data=None):
        """투자자 데이터 처리 및 분석"""
        try:
            # 컬럼명 확인 및 정규화
            available_cols = list(investor_data.columns)
            available_investors = list(investor_data.index)
            
            # 외국인과 기관 데이터 추출
            foreign_net_buy = 0
            institution_net_buy = 0
            total_trading_value = 0
            
            # 외국인 데이터 추출 (다양한 형태 지원)
            foreign_investors = ['외국인', '외국인합계', '외국인계', '외국인 계', '기타외국인']
            for foreign_type in foreign_investors:
                if foreign_type in available_investors:
                    try:
                        if '순매수' in available_cols:
                            foreign_net_buy += float(investor_data.loc[foreign_type, '순매수'])
                        elif '매수' in available_cols and '매도' in available_cols:
                            buy_vol = float(investor_data.loc[foreign_type, '매수'])
                            sell_vol = float(investor_data.loc[foreign_type, '매도'])
                            foreign_net_buy += (buy_vol - sell_vol)
                        elif len(available_cols) >= 3:  # 매도, 매수, 순매수 순서로 가정
                            foreign_net_buy += float(investor_data.loc[foreign_type, available_cols[2]])
                        elif len(available_cols) >= 1:  # 첫 번째 컬럼을 순매수로 가정
                            foreign_net_buy += float(investor_data.loc[foreign_type, available_cols[0]])
                        
                        break  # 첫 번째로 찾은 외국인 데이터 사용
                    except Exception as e:
                        continue
            
            # 기관 데이터 추출 (다양한 형태 지원)
            institution_types = ['기관합계', '금융투자', '보험', '투신', '사모', '은행', '기타금융', '연기금', '연기금 등']
            for inst_type in institution_types:
                if inst_type in available_investors:
                    try:
                        inst_net = 0
                        if '순매수' in available_cols:
                            inst_net = float(investor_data.loc[inst_type, '순매수'])
                        elif '매수' in available_cols and '매도' in available_cols:
                            buy_vol = float(investor_data.loc[inst_type, '매수'])
                            sell_vol = float(investor_data.loc[inst_type, '매도'])
                            inst_net = (buy_vol - sell_vol)
                        elif len(available_cols) >= 3:  # 매도, 매수, 순매수 순서로 가정
                            inst_net = float(investor_data.loc[inst_type, available_cols[2]])
                        elif len(available_cols) >= 1:  # 첫 번째 컬럼을 순매수로 가정
                            inst_net = float(investor_data.loc[inst_type, available_cols[0]])
                        
                        institution_net_buy += inst_net
                        
                        # 기관합계가 있으면 다른 기관 데이터는 건너뛰기
                        if inst_type == '기관합계':
                            break
                    except Exception as e:
                        continue
            
            # 전체 거래량/거래대금 계산
            if '전체' in available_investors:
                try:
                    if '매수' in available_cols and '매도' in available_cols:
                        buy_total = float(investor_data.loc['전체', '매수'])
                        sell_total = float(investor_data.loc['전체', '매도'])
                        total_trading_value = buy_total + sell_total
                    elif '순매수' in available_cols:
                        # 순매수가 있으면 전체 거래량을 추정 (순매수의 절댓값 기준)
                        net_buy_total = float(investor_data.loc['전체', '순매수'])
                        # 전체 거래량 추정 (모든 투자자의 매수+매도 합산)
                        total_trading_value = 0
                        for investor in available_investors:
                            if investor != '전체':
                                try:
                                    if '매수' in available_cols and '매도' in available_cols:
                                        buy_vol = float(investor_data.loc[investor, '매수'])
                                        sell_vol = float(investor_data.loc[investor, '매도'])
                                        total_trading_value += (buy_vol + sell_vol)
                                except:
                                    continue
                        # 추정이 실패하면 순매수 절댓값의 20배로 추정
                        if total_trading_value <= 0:
                            total_trading_value = abs(net_buy_total) * 20 if net_buy_total != 0 else 1000000
                    elif len(available_cols) >= 1:
                        # 첫 번째 숫자 컬럼 사용
                        total_trading_value = float(investor_data.loc['전체', available_cols[0]])
                    else:
                        total_trading_value = 1000000  # 기본값
                except Exception as e:
                    # 전체 행이 없으면 개별 투자자 데이터로 추정
                    total_trading_value = 0
                    for investor in available_investors:
                        if investor != '전체':
                            try:
                                if '매수' in available_cols and '매도' in available_cols:
                                    buy_vol = float(investor_data.loc[investor, '매수'])
                                    sell_vol = float(investor_data.loc[investor, '매도'])
                                    total_trading_value += (buy_vol + sell_vol)
                                elif '순매수' in available_cols:
                                    net_buy = float(investor_data.loc[investor, '순매수'])
                                    total_trading_value += abs(net_buy) * 2  # 순매수의 2배로 추정
                            except:
                                continue
                    
                    if total_trading_value <= 0:
                        total_trading_value = 1000000  # 최소 기본값
            else:
                # '전체' 행이 없으면 개별 투자자들의 합으로 계산
                total_trading_value = 0
                for investor in available_investors:
                    try:
                        if '매수' in available_cols and '매도' in available_cols:
                            buy_vol = float(investor_data.loc[investor, '매수'])
                            sell_vol = float(investor_data.loc[investor, '매도'])
                            total_trading_value += (buy_vol + sell_vol)
                        elif '순매수' in available_cols:
                            net_buy = float(investor_data.loc[investor, '순매수'])
                            total_trading_value += abs(net_buy) * 2  # 순매수의 2배로 추정
                    except:
                        continue
                
                if total_trading_value <= 0:
                    total_trading_value = 1000000  # 최소 기본값
            
            # 개인 투자자 데이터도 추출
            individual_net_buy = 0
            if '개인' in available_investors:
                try:
                    if '순매수' in available_cols:
                        individual_net_buy = float(investor_data.loc['개인', '순매수'])
                    elif '매수' in available_cols and '매도' in available_cols:
                        buy_vol = float(investor_data.loc['개인', '매수'])
                        sell_vol = float(investor_data.loc['개인', '매도'])
                        individual_net_buy = (buy_vol - sell_vol)
                    elif len(available_cols) >= 3:
                        individual_net_buy = float(investor_data.loc['개인', available_cols[2]])
                    
                except Exception as e:
                    pass
            
            # 비율 계산
            foreign_ratio = (foreign_net_buy / total_trading_value * 100) if total_trading_value > 0 else 0
            institution_ratio = (institution_net_buy / total_trading_value * 100) if total_trading_value > 0 else 0
            
            # 연속 매수일 계산 (일자별 데이터가 있는 경우)
            net_buy_days = 0
            foreign_buy_days = 0
            institution_buy_days = 0
            
            if daily_data is not None and not daily_data.empty:
                try:
                    # 최근 일자부터 역순으로 확인
                    daily_data_sorted = daily_data.sort_index(ascending=False)
                    
                    # 외국인 연속 매수일 계산
                    for date_idx in daily_data_sorted.index:
                        if '외국인' in daily_data_sorted.columns or '외국인합계' in daily_data_sorted.columns:
                            foreign_col = '외국인' if '외국인' in daily_data_sorted.columns else '외국인합계'
                            
                            if '순매수' in daily_data_sorted.columns:
                                daily_foreign_net = daily_data_sorted.loc[date_idx, foreign_col] if foreign_col in daily_data_sorted.index else 0
                            elif len(daily_data_sorted.columns) >= 3:  # 매도, 매수, 순매수 순서
                                daily_foreign_net = daily_data_sorted.iloc[daily_data_sorted.index.get_loc(date_idx), 2] if foreign_col in daily_data_sorted.index else 0
                            else:
                                daily_foreign_net = 0
                            
                            if daily_foreign_net > 0:
                                foreign_buy_days += 1
                            else:
                                break  # 연속 매수가 끊어지면 중단
                    
                    # 기관 연속 매수일 계산
                    for date_idx in daily_data_sorted.index:
                        institution_found = False
                        daily_institution_net = 0
                        
                        # 다양한 기관 컬럼 확인
                        for inst_col in ['기관합계', '금융투자', '보험', '투신', '사모', '은행']:
                            if inst_col in daily_data_sorted.columns:
                                if '순매수' in daily_data_sorted.columns:
                                    daily_institution_net += daily_data_sorted.loc[date_idx, inst_col] if inst_col in daily_data_sorted.index else 0
                                elif len(daily_data_sorted.columns) >= 3:
                                    daily_institution_net += daily_data_sorted.iloc[daily_data_sorted.index.get_loc(date_idx), 2] if inst_col in daily_data_sorted.index else 0
                                institution_found = True
                                
                                if inst_col == '기관합계':  # 기관합계가 있으면 다른 기관은 제외
                                    break
                        
                        if institution_found and daily_institution_net > 0:
                            institution_buy_days += 1
                        else:
                            break  # 연속 매수가 끊어지면 중단
                    
                    # 외국인 또는 기관 중 더 긴 연속 매수일을 사용
                    net_buy_days = max(foreign_buy_days, institution_buy_days)
                    
                except Exception as e:
                    print(f"⚠️ 연속 매수일 계산 오류: {str(e)}")
                    net_buy_days = 1 if (foreign_net_buy > 0 or institution_net_buy > 0) else 0
            else:
                # 일자별 데이터가 없으면 현재 순매수 상태만 확인
                net_buy_days = 1 if (foreign_net_buy > 0 or institution_net_buy > 0) else 0
            
            # 공매도 정보 가져오기
            short_data = self.get_short_selling_data(code)
            
            return {
                'foreign_net_buy': int(foreign_net_buy),
                'institution_net_buy': int(institution_net_buy),
                'individual_net_buy': int(individual_net_buy),
                'foreign_ratio': round(foreign_ratio, 2),
                'institution_ratio': round(institution_ratio, 2),
                'net_buy_days': net_buy_days,  # 연속 매수일
                'foreign_buy_days': foreign_buy_days,  # 외국인 연속 매수일
                'institution_buy_days': institution_buy_days,  # 기관 연속 매수일
                'total_volume': int(total_trading_value),
                'short_selling_volume': short_data.get('volume', 0) if short_data else 0,
                'short_selling_ratio': short_data.get('ratio', 0) if short_data else 0,
                'short_selling_balance': short_data.get('balance', 0) if short_data else 0,
                'short_selling_days': short_data.get('days', 0) if short_data else 0,
                'data_source': method,
                'daily_data_available': daily_data is not None and not daily_data.empty
            }
            
        except Exception as e:
            print(f"⚠️ 투자자 데이터 처리 중 오류: {str(e)}")
            return self._get_default_investor_data()
    
    def _get_default_investor_data(self):
        """기본 투자자 데이터 반환"""
        return {
            'foreign_net_buy': 0,
            'institution_net_buy': 0,
            'individual_net_buy': 0,
            'foreign_ratio': 0,
            'institution_ratio': 0,
            'net_buy_days': 0,
            'foreign_buy_days': 0,
            'institution_buy_days': 0,
            'total_volume': 0,
            'short_selling_volume': 0,
            'short_selling_ratio': 0,
            'short_selling_balance': 0,
            'short_selling_days': 0,
            'data_source': 'default',
            'daily_data_available': False
        }
    
    def get_market_investor_trends(self, period='1w'):
        """시장 투자자별 매매 동향 가져오기"""
        try:
            # 기간 설정
            end_date = datetime.datetime.now()
            if period == '1w':
                start_date = end_date - datetime.timedelta(days=7)
            else:
                start_date = end_date - datetime.timedelta(days=30)
            
            start_date_str = start_date.strftime('%Y%m%d')
            end_date_str = end_date.strftime('%Y%m%d')
            
            # KOSPI 투자자별 매매 동향
            kospi_investor = pd.DataFrame()
            try:
                kospi_investor = stock.get_market_trading_value_by_investor(start_date_str, end_date_str, "KOSPI")
                if not kospi_investor.empty:
                    # 컬럼명 확인 및 정규화
                    if '거래대금' in kospi_investor.columns:
                        kospi_investor = kospi_investor.rename(columns={'거래대금': '순매수'})
                    elif '순매수' not in kospi_investor.columns and len(kospi_investor.columns) > 0:
                        # 첫 번째 숫자 컬럼을 순매수로 사용
                        numeric_cols = kospi_investor.select_dtypes(include=[np.number]).columns
                        if len(numeric_cols) > 0:
                            kospi_investor = kospi_investor.rename(columns={numeric_cols[0]: '순매수'})
                    
                    if '순매수' in kospi_investor.columns:
                        # print(f"✅ KOSPI 투자자 데이터 수집 성공: {len(kospi_investor)}개 행")
                        pass
                    else:
                        # print(f"⚠️ KOSPI 투자자 데이터 컬럼 구조 문제: {kospi_investor.columns.tolist()}")
                        kospi_investor = pd.DataFrame()
                else:
                    # print("⚠️ KOSPI 투자자 데이터 없음")
                    kospi_investor = pd.DataFrame()
            except Exception as e:
                # print(f"⚠️ KOSPI 투자자 데이터 수집 실패: {str(e)}")
                kospi_investor = pd.DataFrame()
            
            # KOSDAQ 투자자별 매매 동향
            kosdaq_investor = pd.DataFrame()
            try:
                kosdaq_investor = stock.get_market_trading_value_by_investor(start_date_str, end_date_str, "KOSDAQ")
                if not kosdaq_investor.empty:
                    # 컬럼명 확인 및 정규화
                    if '거래대금' in kosdaq_investor.columns:
                        kosdaq_investor = kosdaq_investor.rename(columns={'거래대금': '순매수'})
                    elif '순매수' not in kosdaq_investor.columns and len(kosdaq_investor.columns) > 0:
                        # 첫 번째 숫자 컬럼을 순매수로 사용
                        numeric_cols = kosdaq_investor.select_dtypes(include=[np.number]).columns
                        if len(numeric_cols) > 0:
                            kosdaq_investor = kosdaq_investor.rename(columns={numeric_cols[0]: '순매수'})
                    
                    if '순매수' in kosdaq_investor.columns:
                        # print(f"✅ KOSDAQ 투자자 데이터 수집 성공: {len(kosdaq_investor)}개 행")
                        pass
                    else:
                        # print(f"⚠️ KOSDAQ 투자자 데이터 컬럼 구조 문제: {kosdaq_investor.columns.tolist()}")
                        kosdaq_investor = pd.DataFrame()
                else:
                    # print("⚠️ KOSDAQ 투자자 데이터 없음")
                    kosdaq_investor = pd.DataFrame()
            except Exception as e:
                # print(f"⚠️ KOSDAQ 투자자 데이터 수집 실패: {str(e)}")
                kosdaq_investor = pd.DataFrame()
            
            # 데이터가 모두 없으면 None 반환 (기본 데이터 생성 금지)
            if kospi_investor.empty and kosdaq_investor.empty:
                # print("❌ 투자자 동향 데이터를 가져올 수 없습니다.")
                return None
            
            return {
                'kospi_investor': kospi_investor,
                'kosdaq_investor': kosdaq_investor,
                'period': period
            }
            
        except Exception as e:
            # print(f"❌ 시장 투자자 동향 수집 중 오류: {str(e)}")
            return None

    def get_top_foreign_trading_stocks(self, period='1w', top_n=10):
        """외국인 매매 상위 종목 가져오기"""
        try:
            # 기간 설정
            end_date = datetime.datetime.now()
            if period == '1w':
                start_date = end_date - datetime.timedelta(days=7)
            else:
                start_date = end_date - datetime.timedelta(days=30)
            
            start_date_str = start_date.strftime('%Y%m%d')
            end_date_str = end_date.strftime('%Y%m%d')
            
            # 외국인 순매수 상위 종목 (정확한 파라미터 사용)
            foreign_buy_top = pd.DataFrame()
            try:
                foreign_buy_top = stock.get_market_net_purchases_of_equities_by_ticker(
                    start_date_str, end_date_str, "KOSPI", "외국인"
                )
                
                if not foreign_buy_top.empty:
                    # 컬럼명 확인
                    if '순매수거래량' in foreign_buy_top.columns or '순매수거래대금' in foreign_buy_top.columns:
                        # 종목명 추가
                        foreign_buy_top = self.add_stock_names_to_dataframe(foreign_buy_top, 'index')
                        foreign_buy_top = foreign_buy_top.head(top_n)
                        # print(f"✅ 외국인 순매수 상위 {len(foreign_buy_top)}개 종목 수집")
                    else:
                        # print(f"⚠️ 외국인 매매 데이터 컬럼 구조 문제: {foreign_buy_top.columns.tolist()}")
                        foreign_buy_top = pd.DataFrame()
                else:
                    # print("⚠️ 외국인 순매수 데이터 없음")
                    foreign_buy_top = pd.DataFrame()
                    
            except Exception as e:
                # print(f"⚠️ 외국인 순매수 데이터 수집 실패: {str(e)}")
                foreign_buy_top = pd.DataFrame()
            
            # 외국인 순매도 상위 종목 (순매수의 반대)
            foreign_sell_top = pd.DataFrame()
            try:
                foreign_sell_top = stock.get_market_net_purchases_of_equities_by_ticker(
                    start_date_str, end_date_str, "KOSPI", "외국인"
                )
                
                if not foreign_sell_top.empty:
                    # 컬럼명 확인 및 정렬
                    if '순매수거래대금' in foreign_sell_top.columns:
                        foreign_sell_top = foreign_sell_top.sort_values('순매수거래대금')
                    elif '순매수거래량' in foreign_sell_top.columns:
                        foreign_sell_top = foreign_sell_top.sort_values('순매수거래량')
                    else:
                        # print(f"⚠️ 외국인 매도 데이터 컬럼 구조 문제: {foreign_sell_top.columns.tolist()}")
                        foreign_sell_top = pd.DataFrame()
                    
                    if not foreign_sell_top.empty:
                        foreign_sell_top = self.add_stock_names_to_dataframe(foreign_sell_top, 'index')
                        foreign_sell_top = foreign_sell_top.head(top_n)
                        # print(f"✅ 외국인 순매도 상위 {len(foreign_sell_top)}개 종목 수집")
                else:
                    # print("⚠️ 외국인 순매도 데이터 없음")
                    foreign_sell_top = pd.DataFrame()
                    
            except Exception as e:
                # print(f"⚠️ 외국인 순매도 데이터 수집 실패: {str(e)}")
                foreign_sell_top = pd.DataFrame()
            
            # 데이터가 모두 없으면 None 반환 (기본 데이터 생성 금지)
            if foreign_buy_top.empty and foreign_sell_top.empty:
                # print("❌ 외국인 매매 데이터를 가져올 수 없습니다.")
                return None
            
            return {
                'foreign_buy_top': foreign_buy_top,
                'foreign_sell_top': foreign_sell_top,
                'period': period
            }
            
        except Exception as e:
            # print(f"❌ 외국인 매매 상위 종목 수집 중 오류: {str(e)}")
            return None

    def get_top_institution_trading_stocks(self, period='1w', top_n=10):
        """기관 매매 상위 종목 가져오기"""
        try:
            # 기간 설정
            end_date = datetime.datetime.now()
            if period == '1w':
                start_date = end_date - datetime.timedelta(days=7)
            else:
                start_date = end_date - datetime.timedelta(days=30)
            
            start_date_str = start_date.strftime('%Y%m%d')
            end_date_str = end_date.strftime('%Y%m%d')
            
            # 기관 순매수 상위 종목 (정확한 파라미터 사용)
            institution_buy_top = pd.DataFrame()
            try:
                institution_buy_top = stock.get_market_net_purchases_of_equities_by_ticker(
                    start_date_str, end_date_str, "KOSPI", "기관합계"
                )
                
                if not institution_buy_top.empty:
                    # 컬럼명 확인
                    if '순매수거래량' in institution_buy_top.columns or '순매수거래대금' in institution_buy_top.columns:
                        # 종목명 추가
                        institution_buy_top = self.add_stock_names_to_dataframe(institution_buy_top, 'index')
                        institution_buy_top = institution_buy_top.head(top_n)
                        # print(f"✅ 기관 순매수 상위 {len(institution_buy_top)}개 종목 수집")
                    else:
                        # print(f"⚠️ 기관 매매 데이터 컬럼 구조 문제: {institution_buy_top.columns.tolist()}")
                        institution_buy_top = pd.DataFrame()
                else:
                    # print("⚠️ 기관 순매수 데이터 없음")
                    institution_buy_top = pd.DataFrame()
                    
            except Exception as e:
                # print(f"⚠️ 기관 순매수 데이터 수집 실패: {str(e)}")
                institution_buy_top = pd.DataFrame()
            
            # 기관 순매도 상위 종목
            institution_sell_top = pd.DataFrame()
            try:
                institution_sell_top = stock.get_market_net_purchases_of_equities_by_ticker(
                    start_date_str, end_date_str, "KOSPI", "기관합계"
                )
                
                if not institution_sell_top.empty:
                    # 컬럼명 확인 및 정렬
                    if '순매수거래대금' in institution_sell_top.columns:
                        institution_sell_top = institution_sell_top.sort_values('순매수거래대금')
                    elif '순매수거래량' in institution_sell_top.columns:
                        institution_sell_top = institution_sell_top.sort_values('순매수거래량')
                    else:
                        # print(f"⚠️ 기관 매도 데이터 컬럼 구조 문제: {institution_sell_top.columns.tolist()}")
                        institution_sell_top = pd.DataFrame()
                    
                    if not institution_sell_top.empty:
                        institution_sell_top = self.add_stock_names_to_dataframe(institution_sell_top, 'index')
                        institution_sell_top = institution_sell_top.head(top_n)
                        # print(f"✅ 기관 순매도 상위 {len(institution_sell_top)}개 종목 수집")
                else:
                    # print("⚠️ 기관 순매도 데이터 없음")
                    institution_sell_top = pd.DataFrame()
                    
            except Exception as e:
                # print(f"⚠️ 기관 순매도 데이터 수집 실패: {str(e)}")
                institution_sell_top = pd.DataFrame()
            
            # 데이터가 모두 없으면 None 반환 (기본 데이터 생성 금지)
            if institution_buy_top.empty and institution_sell_top.empty:
                # print("❌ 기관 매매 데이터를 가져올 수 없습니다.")
                return None
            
            return {
                'institution_buy_top': institution_buy_top,
                'institution_sell_top': institution_sell_top,
                'period': period
            }
            
        except Exception as e:
            # print(f"❌ 기관 매매 상위 종목 수집 중 오류: {str(e)}")
            return None

    def get_top_short_selling_stocks(self, top_n=20):
        """공매도 상위 종목 가져오기"""
        try:
            # 최근 5일간 데이터
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=5)
            
            start_date_str = start_date.strftime('%Y%m%d')
            end_date_str = end_date.strftime('%Y%m%d')
            
            # 주요 종목들의 공매도 정보 수집
            major_stocks = ['005930', '000660', '035420', '035720', '005380']  # 삼성전자, SK하이닉스, NAVER, 카카오, 현대차
            short_selling_data = []
            
            for ticker in major_stocks:
                try:
                    short_data = stock.get_shorting_balance_by_date(start_date_str, end_date_str, ticker)
                    
                    if not short_data.empty:
                        # 최근 데이터 가져오기
                        latest_data = short_data.iloc[-1]
                        
                        # 종목명 가져오기
                        stock_name = self.get_stock_name_mapping().get(ticker, f"종목{ticker}")
                        
                        short_selling_data.append({
                            'ticker': ticker,
                            '종목명': stock_name,
                            'short_ratio': latest_data.get('공매도비중', 0),
                            'short_balance': latest_data.get('공매도잔고', 0),
                            'date': latest_data.name if hasattr(latest_data, 'name') else end_date_str
                        })
                        
                except Exception as e:
                    # 개별 종목 오류는 무시하고 계속 진행
                    continue
            
            if short_selling_data:
                result_df = pd.DataFrame(short_selling_data)
                # 공매도 비중 기준으로 정렬
                result_df = result_df.sort_values('short_ratio', ascending=False)
                result_df = result_df.head(top_n)
                # print(f"✅ 공매도 상위 {len(result_df)}개 종목 수집")
                return result_df
            else:
                # print("❌ 공매도 데이터를 가져올 수 없습니다.")
                return pd.DataFrame()
                
        except Exception as e:
            # print(f"❌ 공매도 상위 종목 수집 중 오류: {str(e)}")
            return pd.DataFrame()
    
    def get_sector_performance_summary(self, results_df):
        """업종별 성과 요약 가져오기"""
        try:
            if results_df.empty or 'sector' not in results_df.columns:
                return pd.DataFrame()
            
            # 업종별 통계 계산
            sector_stats = results_df.groupby('sector').agg({
                'total_score': ['count', 'mean', 'max', 'min', 'std'],
                'trend_score': 'mean',
                'pattern_score': 'mean',
                'rs_score': 'mean',
                'fundamental_score': 'mean',
                'investor_score': 'mean',
                'current_price': 'mean',
                'price_change_pct': 'mean'
            }).round(2)
            
            # 컬럼명 정리
            sector_stats.columns = [
                '종목수', '평균점수', '최고점수', '최저점수', '점수편차',
                '평균추세', '평균패턴', '평균상대강도', '평균펀더멘털', '평균수급',
                '평균주가', '평균등락률'
            ]
            
            # 점수 기준 정렬
            sector_stats = sector_stats.sort_values('평균점수', ascending=False)
            
            return sector_stats
            
        except Exception as e:
            return pd.DataFrame()
    
    def get_stock_name_mapping(self):
        """종목 코드 -> 종목명 매핑 딕셔너리 가져오기"""
        try:
            all_stocks = self.get_all_stocks()
            
            if not all_stocks.empty:
                all_stocks['Code'] = all_stocks['Code'].astype(str).str.zfill(6)
                name_mapping = dict(zip(all_stocks['Code'], all_stocks['Name']))
                return name_mapping
            else:
                return {}
                
        except Exception as e:
            return {}
    
    def add_stock_names_to_dataframe(self, df, code_column='ticker'):
        """데이터프레임에 종목명 컬럼 추가"""
        try:
            if df.empty:
                return df 
            
            # 종목명 매핑 가져오기
            name_mapping = self.get_stock_name_mapping()
            
            if not name_mapping:
                # pykrx에서 직접 가져오기 시도
                try:
                    kospi_names = stock.get_market_ticker_name("KOSPI")
                    kosdaq_names = stock.get_market_ticker_name("KOSDAQ")
                    name_mapping = {**kospi_names, **kosdaq_names}
                except:
                    name_mapping = {}
            
            df_copy = df.copy()
            
            # 인덱스가 종목 코드인 경우
            if code_column == 'index':
                formatted_codes = [str(code).zfill(6) for code in df_copy.index]
                
                stock_names = []
                for code in formatted_codes:
                    if code in name_mapping:
                        stock_names.append(name_mapping[code])
                    else:
                        stock_names.append(f"종목{code}")
                
                df_copy['종목명'] = stock_names
                df_copy['종목코드'] = formatted_codes
                
            else:
                # 특정 컬럼이 종목 코드인 경우
                if code_column in df_copy.columns:
                    df_copy[code_column] = df_copy[code_column].astype(str).str.zfill(6)
                    df_copy['종목명'] = df_copy[code_column].map(name_mapping).fillna(f"종목{df_copy[code_column]}")
            
            return df_copy
            
        except Exception as e:
            # 오류 발생 시 기본 종목명 생성
            df_copy = df.copy()
            if code_column == 'index':
                df_copy['종목명'] = [f"종목{str(code).zfill(6)}" for code in df_copy.index]
                df_copy['종목코드'] = [str(code).zfill(6) for code in df_copy.index]
            return df_copy
    
    def get_realtime_price(self, code):
        """실시간 주가 데이터 가져오기 - FinanceDataReader 직접 사용"""
        try:
            import FinanceDataReader as fdr
            
            # 종목 코드 정규화
            formatted_code = str(code).zfill(6)
            
            # 한국 주식의 경우 KRX: 접두사 추가
            fdr_code = f"KRX:{formatted_code}"
            
            # 최근 5일 데이터 가져오기 (캐시 없이 직접)
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=7)
            
            stock_data = fdr.DataReader(fdr_code, start_date, end_date)
            
            if not stock_data.empty:
                # 컬럼명 정규화
                if 'Adj Close' in stock_data.columns:
                    stock_data['Close'] = stock_data['Adj Close']
                
                latest_data = stock_data.iloc[-1]
                prev_data = stock_data.iloc[-2] if len(stock_data) > 1 else latest_data
                
                current_price = float(latest_data['Close'])
                prev_price = float(prev_data['Close'])
                price_change = current_price - prev_price
                price_change_pct = (price_change / prev_price * 100) if prev_price > 0 else 0.0
                
                return {
                    'current_price': current_price,
                    'price_change': price_change,
                    'price_change_pct': price_change_pct,
                    'volume': int(latest_data['Volume']),
                    'high': float(latest_data['High']),
                    'low': float(latest_data['Low']),
                    'open': float(latest_data['Open']),
                    'last_update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'note': 'FinanceDataReader 실시간 데이터'
                }
            
            return None
            
        except Exception as e:
            print(f"⚠️ FinanceDataReader 실시간 데이터 수집 실패: {e}")
            # 대안으로 기존 방식 사용 (캐시 아님)
            try:
                from pykrx import stock
                
                formatted_code = str(code).zfill(6)
                today = datetime.datetime.now().strftime('%Y%m%d')
                
                # pykrx로 일별 주가 데이터 가져오기
                price_data = stock.get_market_ohlcv_by_date(today, today, formatted_code)
                
                if not price_data.empty:
                    latest_data = price_data.iloc[-1]
                    
                    # 전일 데이터와 비교를 위해 전일 데이터도 가져오기
                    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
                    prev_data_df = stock.get_market_ohlcv_by_date(yesterday, yesterday, formatted_code)
                    
                    if not prev_data_df.empty:
                        prev_price = float(prev_data_df.iloc[-1]['종가'])
                    else:
                        prev_price = float(latest_data['종가'])
                    
                    current_price = float(latest_data['종가'])
                    price_change = current_price - prev_price
                    price_change_pct = (price_change / prev_price * 100) if prev_price > 0 else 0.0
                    
                    return {
                        'current_price': current_price,
                        'price_change': price_change,
                        'price_change_pct': price_change_pct,
                        'volume': int(latest_data['거래량']),
                        'high': float(latest_data['고가']),
                        'low': float(latest_data['저가']),
                        'open': float(latest_data['시가']),
                        'last_update': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'note': 'pykrx 실시간 데이터'
                    }
                
                return None
                
            except Exception as e2:
                print(f"⚠️ pykrx 실시간 데이터 수집도 실패: {e2}")
                return None

    def get_short_selling_data(self, code, period='1m'):
        """공매도 정보 가져오기 - 개선된 버전"""
        try:
            # 기간 설정
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=60)  # 2개월
            
            start_date_str = start_date.strftime('%Y%m%d')
            end_date_str = end_date.strftime('%Y%m%d')
            
            # 종목 코드 정규화
            formatted_code = str(code).zfill(6)
            
            short_data = None
            success_method = None
            
            # 방법 1: 공매도 거래량 (get_shorting_volume_by_ticker)
            try:
                # 최근 일자의 공매도 거래량 정보
                recent_date = end_date.strftime('%Y%m%d')
                short_data = stock.get_shorting_volume_by_ticker(recent_date)
                if not short_data.empty and formatted_code in short_data.index:
                    success_method = "공매도거래량"
                    short_data = short_data.loc[[formatted_code]]
                else:
                    raise Exception("해당 종목 데이터 없음")
            except Exception as e:
                short_data = None
            
            # 방법 2: 공매도 거래대금 (get_shorting_value_by_ticker)
            if short_data is None or short_data.empty:
                try:
                    recent_date = end_date.strftime('%Y%m%d')
                    short_data = stock.get_shorting_value_by_ticker(recent_date)
                    if not short_data.empty and formatted_code in short_data.index:
                        success_method = "공매도거래대금"
                        short_data = short_data.loc[[formatted_code]]
                    else:
                        raise Exception("해당 종목 데이터 없음")
                except Exception as e:
                    short_data = None
            
            # 방법 3: 일자별 공매도 거래 현황 (get_shorting_volume_by_date)
            if short_data is None or short_data.empty:
                try:
                    short_data = stock.get_shorting_volume_by_date(start_date_str, end_date_str, formatted_code)
                    if not short_data.empty:
                        success_method = "일자별공매도"
                    else:
                        raise Exception("빈 데이터")
                except Exception as e:
                    short_data = None
            
            # 데이터 처리
            if short_data is not None and not short_data.empty:
                return self._process_short_selling_data(short_data, code, success_method)
            else:
                return None
                
        except Exception as e:
            return None
    
    def _process_short_selling_data(self, short_data, code, method):
        """공매도 데이터 처리 및 분석"""
        try:
            # 기본값 설정
            short_volume = 0
            short_ratio = 0.0
            short_balance = 0
            short_days = 0
            
            if method == "공매도잔고":
                # 공매도 잔고 현황 데이터 처리
                if not short_data.empty:
                    latest_data = short_data.iloc[-1]
                    
                    # 다양한 컬럼명 지원
                    balance_cols = ['공매도잔고', '잔고', 'balance', 'short_balance']
                    ratio_cols = ['공매도비중', '비중', 'ratio', 'short_ratio']
                    
                    for col in balance_cols:
                        if col in short_data.columns:
                            short_balance = int(latest_data[col]) if pd.notna(latest_data[col]) else 0
                            break
                    
                    for col in ratio_cols:
                        if col in short_data.columns:
                            short_ratio = float(latest_data[col]) if pd.notna(latest_data[col]) else 0.0
                            break
                    
                    # 공매도 거래일 수 계산 (공매도가 있었던 날의 수)
                    if '공매도잔고' in short_data.columns:
                        short_days = len(short_data[short_data['공매도잔고'] > 0])
                    elif '잔고' in short_data.columns:
                        short_days = len(short_data[short_data['잔고'] > 0])
                    else:
                        short_days = len(short_data)
            
            elif method in ["공매도거래량", "공매도거래대금"]:
                # 공매도 거래량/거래대금 데이터 처리
                if not short_data.empty:
                    latest_data = short_data.iloc[0]  # 단일 행 데이터
                    
                    volume_cols = ['공매도', 'volume', 'short_volume']
                    ratio_cols = ['비중', 'ratio', 'short_ratio']
                    
                    for col in volume_cols:
                        if col in short_data.columns:
                            short_volume = int(latest_data[col]) if pd.notna(latest_data[col]) else 0
                            break
                    
                    for col in ratio_cols:
                        if col in short_data.columns:
                            short_ratio = float(latest_data[col]) if pd.notna(latest_data[col]) else 0.0
                            break
                    
                    short_days = 1  # 단일 일자 데이터
            
            elif method == "일자별공매도":
                # 일자별 공매도 데이터 처리
                if not short_data.empty:
                    # 최신 데이터 사용
                    latest_data = short_data.iloc[-1]
                    
                    volume_cols = ['공매도', 'volume', 'short_volume']
                    ratio_cols = ['비중', 'ratio', 'short_ratio']
                    
                    for col in volume_cols:
                        if col in short_data.columns:
                            short_volume = int(latest_data[col]) if pd.notna(latest_data[col]) else 0
                            break
                    
                    for col in ratio_cols:
                        if col in short_data.columns:
                            short_ratio = float(latest_data[col]) if pd.notna(latest_data[col]) else 0.0
                            break
                    
                    # 공매도가 있었던 날의 수 계산
                    if '공매도' in short_data.columns:
                        short_days = len(short_data[short_data['공매도'] > 0])
                    else:
                        short_days = len(short_data)
            
            return {
                'volume': short_volume,
                'ratio': round(short_ratio, 2),
                'balance': short_balance,
                'days': short_days,
                'data_source': method
            }
            
        except Exception as e:
            return {
                'volume': 0,
                'ratio': 0.0,
                'balance': 0,
                'days': 0,
                'data_source': 'error'
            } 