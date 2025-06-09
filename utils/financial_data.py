import requests
import pandas as pd
import numpy as np
import os
import json
import zipfile
import io
import xmltodict
import datetime
import time
from pathlib import Path
import re
from pykrx import stock

# dart-fss 라이브러리 추가
try:
    import dart_fss as dart
    DART_FSS_AVAILABLE = True
except ImportError:
    DART_FSS_AVAILABLE = False

# NumPy 타입을 기본 Python 타입으로 변환하는 JSON 인코더 추가
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)

class FinancialDataCollector:
    def __init__(self, api_key, cache_dir='data'):
        """재무 데이터 수집기 초기화
        
        Args:
            api_key: DART API 키
            cache_dir: 캐시 디렉토리
        """
        self.api_key = api_key
        self.cache_dir = cache_dir
        
        # 사용자 지정 재무 데이터 경로 (절대 경로)
        self.financial_data_dir = '/Users/elel/Code/sepa_v2/financial_data'
        
        # f_data 폴더 경로 추가
        self.f_data_dir = '/Users/elel/Code/sepa_v2/f_data'
        
        self.ensure_cache_dir()
        
        # dart-fss API 키 설정
        if DART_FSS_AVAILABLE and api_key:
            try:
                dart.set_api_key(api_key)
            except Exception as e:
                pass
        
        # f_data 폴더의 데이터 로드 (우선순위 1)
        self.f_data_cache = self._load_f_data()
        
        # f_data 로드 결과 확인 및 로그 출력
        if self.f_data_cache:
            fundamental_count = len([k for k in self.f_data_cache.keys() if k.startswith('fundamental_')])
            financial_count = len([k for k in self.f_data_cache.keys() if k.startswith('financial_')])
            print(f"✅ f_data 로드 완료: 펀더멘털 {fundamental_count}개, 재무제표 {financial_count}개")
            
            # f_data가 충분히 로드되었으면 기존 파일 검색 생략
            if fundamental_count > 100 or financial_count > 100:  # 충분한 데이터가 있으면
                print("📊 f_data가 충분하여 기존 파일 검색을 생략합니다")
                self.financial_data_cache = {}
            else:
                # 데이터가 부족하면 기존 파일도 로드
                self.financial_data_cache = self._load_financial_data_from_specified_files()
        else:
            print("⚠️ f_data 로드 실패, 기존 파일을 검색합니다")
            # f_data 로드 실패 시 기존 파일 로드
            self.financial_data_cache = self._load_financial_data_from_specified_files()
        
        # 캐시 디렉토리 생성
        os.makedirs(cache_dir, exist_ok=True)
        
        # 재무 데이터 캐시 디렉토리
        self.financial_cache_dir = os.path.join(cache_dir, 'financial')
        os.makedirs(self.financial_cache_dir, exist_ok=True)
    
    def _load_f_data(self):
        """f_data 폴더의 JSON 파일들 로드"""
        f_data_cache = {}
        
        try:
            # 1. fundamental_data 파일 로드 (PER, PBR, ROE 등)
            fundamental_path = os.path.join(self.f_data_dir, 'fundamental_data_202506.json')
            if os.path.exists(fundamental_path):
                with open(fundamental_path, 'r', encoding='utf-8') as f:
                    fundamental_data = json.load(f)
                    
                    # KOSPI와 KOSDAQ 데이터 통합
                    for market in ['KOSPI', 'KOSDAQ']:
                        if market in fundamental_data:
                            for stock in fundamental_data[market]:
                                ticker = stock.get('티커', '')
                                if ticker:
                                    # 6자리로 정규화
                                    normalized_ticker = str(ticker).zfill(6)
                                    f_data_cache[f"fundamental_{normalized_ticker}"] = {
                                        'ticker': normalized_ticker,
                                        'name': stock.get('종목명', ''),
                                        'market': market,
                                        'bps': stock.get('BPS', 0),
                                        'per': stock.get('PER', 0),
                                        'eps': stock.get('EPS', 0),
                                        'pbr': stock.get('PBR', 0),
                                        'roe': stock.get('ROE', 0),
                                        'source': 'fundamental_data_202506'
                                    }
            
            # 2. financial_data_opendart 파일 로드 (분기별 실적)
            financial_path = os.path.join(self.f_data_dir, 'financial_data_opendart_20250605_171920.json')
            if os.path.exists(financial_path):
                # 큰 파일이므로 스트리밍 방식으로 읽기
                try:
                    with open(financial_path, 'r', encoding='utf-8') as f:
                        financial_data = json.load(f)
                        
                        for company_key, company_data in financial_data.items():
                            # 종목 코드 추출 (예: "BGF(027410)" → "027410")
                            if '(' in company_key and ')' in company_key:
                                ticker = company_key.split('(')[1].split(')')[0]
                                normalized_ticker = str(ticker).zfill(6)
                                
                                # 기본 정보
                                basic_info = company_data.get('기본정보', {})
                                
                                # 분기별 데이터
                                quarterly_data = company_data.get('분기별데이터', {})
                                
                                # 펀더멘털 지표
                                fundamental_indicators = company_data.get('펀더멘털지표', {})
                                
                                f_data_cache[f"financial_{normalized_ticker}"] = {
                                    'ticker': normalized_ticker,
                                    'name': basic_info.get('종목명', ''),
                                    'market': basic_info.get('시장구분', ''),
                                    'quarterly_data': quarterly_data,
                                    'fundamental_indicators': fundamental_indicators,
                                    'source': 'financial_data_opendart_20250605'
                                }
                except Exception as e:
                    pass  # 큰 파일 로딩 실패 시 무시
            
            return f_data_cache
            
        except Exception as e:
            return {}
    
    def get_f_data_fundamental(self, code):
        """f_data에서 펀더멘털 지표 가져오기"""
        try:
            normalized_code = str(code).zfill(6)
            
            # fundamental_data에서 찾기
            fundamental_key = f"fundamental_{normalized_code}"
            if fundamental_key in self.f_data_cache:
                data = self.f_data_cache[fundamental_key].copy()
                data['data_source'] = 'f_data_fundamental'
                return data
            
            return None
            
        except Exception as e:
            return None
    
    def get_f_data_financial(self, code):
        """f_data에서 재무제표 데이터 가져오기"""
        try:
            normalized_code = str(code).zfill(6)
            
            # financial_data에서 찾기
            financial_key = f"financial_{normalized_code}"
            if financial_key in self.f_data_cache:
                data = self.f_data_cache[financial_key].copy()
                
                # 분기별 데이터 처리
                quarterly_data = data.get('quarterly_data', {})
                if quarterly_data:
                    # 최신 분기 데이터 찾기
                    latest_quarter = None
                    latest_data = None
                    
                    for quarter_key, quarter_data in quarterly_data.items():
                        if quarter_key > (latest_quarter or ''):
                            latest_quarter = quarter_key
                            latest_data = quarter_data
                    
                    if latest_data and '재무정보' in latest_data:
                        financial_info = latest_data['재무정보']
                        
                        # 표준 형식으로 변환
                        result = {
                            'ticker': normalized_code,
                            'name': data.get('name', ''),
                            'market': data.get('market', ''),
                            'quarter': latest_quarter,
                            'year': latest_data.get('연도', 0),
                            'quarter_num': latest_data.get('분기', 0),
                            'revenue': financial_info.get('매출액', 0),
                            'operating_profit': financial_info.get('영업이익', 0),
                            'net_income': financial_info.get('당기순이익', 0),
                            'total_assets': financial_info.get('자산총계', 0),
                            'total_debt': financial_info.get('부채총계', 0),
                            'total_equity': financial_info.get('자본총계', 0),
                            'data_source': 'f_data_financial'
                        }
                        
                        # 성장률 계산 (전년 동기 대비)
                        prev_year_quarter = f"{latest_data.get('연도', 0) - 1}Q{latest_data.get('분기', 1)}"
                        if prev_year_quarter in quarterly_data:
                            prev_data = quarterly_data[prev_year_quarter].get('재무정보', {})
                            prev_revenue = prev_data.get('매출액', 0)
                            prev_operating_profit = prev_data.get('영업이익', 0)
                            prev_net_income = prev_data.get('당기순이익', 0)
                            
                            if prev_revenue > 0:
                                result['revenue_yoy'] = ((result['revenue'] / prev_revenue) - 1) * 100
                            if prev_operating_profit > 0:
                                result['operating_profit_yoy'] = ((result['operating_profit'] / prev_operating_profit) - 1) * 100
                            if prev_net_income > 0:
                                result['net_income_yoy'] = ((result['net_income'] / prev_net_income) - 1) * 100
                        
                        # 재무비율 계산
                        if result['total_equity'] > 0:
                            result['debt_ratio'] = (result['total_debt'] / result['total_equity']) * 100
                        if result['revenue'] > 0:
                            result['operating_margin'] = (result['operating_profit'] / result['revenue']) * 100
                            result['net_margin'] = (result['net_income'] / result['revenue']) * 100
                        if result['total_equity'] > 0:
                            result['roe'] = (result['net_income'] * 4 / result['total_equity']) * 100  # 연환산
                        
                        return result
                
                data['data_source'] = 'f_data_financial'
                return data
            
            return None
            
        except Exception as e:
            return None
    
    def ensure_cache_dir(self):
        """캐시 디렉토리가 존재하는지 확인하고 없으면 생성"""
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
    
    def _load_financial_data_from_specified_files(self):
        """사용자가 지정한 재무 데이터 파일들에서 데이터 로드"""
        financial_cache = {}
        
        try:
            # 1. collected_data.json 파일 로드 (우선순위 1)
            collected_data_path = os.path.join(self.financial_data_dir, 'collected_data.json')
            if os.path.exists(collected_data_path):
                try:
                    with open(collected_data_path, 'r', encoding='utf-8') as f:
                        collected_data = json.load(f)
                        financial_cache.update(collected_data)
                        print(f"✅ collected_data.json 로드 완료: {len(collected_data)}개 항목")
                except Exception as e:
                    print(f"⚠️ collected_data.json 로드 실패: {e}")
            else:
                print(f"📁 collected_data.json 파일 없음 (선택사항)")
            
            # 2. processed_data.json 파일 로드 (우선순위 2)
            processed_data_path = os.path.join(self.financial_data_dir, 'processed_data.json')
            if os.path.exists(processed_data_path):
                try:
                    with open(processed_data_path, 'r', encoding='utf-8') as f:
                        processed_data = json.load(f)
                        
                        # 데이터 형태 확인 및 처리
                        if isinstance(processed_data, dict):
                            # 안전하게 업데이트
                            for key, value in processed_data.items():
                                if isinstance(key, str) and isinstance(value, dict):
                                    financial_cache[key] = value
                            print(f"✅ processed_data.json 로드 완료: {len(processed_data)}개 항목")
                        elif isinstance(processed_data, list):
                            # 리스트 형태인 경우 각 항목을 처리
                            processed_count = 0
                            for item in processed_data:
                                if isinstance(item, dict):
                                    # 종목 코드를 키로 사용할 수 있는 필드 찾기
                                    code_key = None
                                    for key in ['code', 'symbol', 'stock_code', 'Code', 'Symbol']:
                                        if key in item:
                                            code_key = item[key]
                                            break
                                    
                                    if code_key:
                                        # 연도와 분기 정보 찾기
                                        year_key = None
                                        quarter_key = None
                                        for key in ['year', 'Year', 'bsns_year']:
                                            if key in item:
                                                year_key = item[key]
                                                break
                                        for key in ['quarter', 'Quarter', 'qtr']:
                                            if key in item:
                                                quarter_key = item[key]
                                                break
                                        
                                        if year_key and quarter_key:
                                            # 연도,분기별 키 생성 (예: 005930,2024,Q1)
                                            cache_key = f"{code_key},{year_key},{quarter_key}"
                                            financial_cache[cache_key] = item
                                            
                                            # 연도별 키도 생성하되, collected_data 형태로 변환
                                            year_cache_key = f"{code_key},{year_key}"
                                            if year_cache_key not in financial_cache:
                                                financial_cache[year_cache_key] = {}
                                            
                                            # 분기별 데이터를 collected_data 형태로 저장
                                            financial_cache[year_cache_key][quarter_key] = {
                                                'sales_cum': item.get('sales', 0),
                                                'op_cum': item.get('op_income', 0),
                                                'net_cum': item.get('net_income', 0),
                                                'equity_cum': item.get('equity', 0),
                                                'debt_cum': item.get('debt', 0),
                                                'year': year_key,
                                                'quarter': quarter_key,
                                                'symbol': code_key,
                                                'code': code_key
                                            }
                                            processed_count += 1
                                        elif year_key:
                                            # 분기 정보가 없는 경우 기존 방식 유지
                                            cache_key = f"{code_key},{year_key}"
                                            financial_cache[cache_key] = item
                                            processed_count += 1
                                        else:
                                            # 연도 정보도 없는 경우
                                            cache_key = str(code_key)
                                            financial_cache[cache_key] = item
                                            processed_count += 1
                except Exception as e:
                    print(f"⚠️ processed_data.json 로드 실패: {e}")
            else:
                print(f"📁 processed_data.json 파일 없음 (선택사항)")
            
            # 3. CSV 파일들 로드 (보조 데이터)
            csv_files = [
                'kospi_financial_quarterly.csv',
                'kospi_financial_2023.csv',
                'kospi_financial_2024.csv',
                'kospi_financial_2025.csv'
            ]
            
            csv_loaded_count = 0
            for csv_file in csv_files:
                csv_path = os.path.join(self.financial_data_dir, csv_file)
                if os.path.exists(csv_path):
                    try:
                        df = pd.read_csv(csv_path)
                        # CSV 데이터를 딕셔너리 형태로 변환
                        for _, row in df.iterrows():
                            symbol = row.get('symbol', '')
                            year = row.get('year', '')
                            quarter = row.get('quarter', '')
                            
                            if symbol and year and quarter:
                                key = f"{symbol},{year}"
                                if key not in financial_cache:
                                    financial_cache[key] = {}
                                
                                quarter_key = f"Q{quarter}"
                                financial_cache[key][quarter_key] = {
                                    'sales_cum': row.get('sales', 0),
                                    'op_cum': row.get('op_income', 0),
                                    'net_cum': row.get('net_income', 0),
                                    'equity_cum': row.get('equity', 0),
                                    'debt_cum': row.get('debt', 0),
                                    'sales_yoy': row.get('sales_yoy', 0),
                                    'op_income_yoy': row.get('op_income_yoy', 0),
                                    'net_income_yoy': row.get('net_income_yoy', 0),
                                    'debt_ratio': row.get('debt_ratio', 0)
                                }
                        csv_loaded_count += 1
                    except Exception as e:
                        print(f"⚠️ {csv_file} 로드 실패: {e}")
                        continue
            
            if csv_loaded_count > 0:
                print(f"✅ CSV 파일 {csv_loaded_count}개 로드 완료")
            
            if financial_cache:
                print(f"✅ 기존 재무 데이터 캐시 항목: {len(financial_cache)}개")
            else:
                print(f"📁 기존 재무 데이터 파일 없음")
            return financial_cache
            
        except Exception as e:
            print(f"⚠️ 재무 데이터 파일 로드 중 오류: {e}")
            return {}
    
    def load_corp_codes(self):
        """DART API에서 사용하는 고유번호 매핑 로드"""
        cache_file = os.path.join(self.cache_dir, 'corp_codes.json')
        
        # 캐시 파일이 있으면 로드
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    corp_codes = json.load(f)
                    return corp_codes
            except Exception as e:
                pass  # 오류 메시지 제거
        
        # 없으면 새로 다운로드
        if not self.api_key:
            return {}
        
        url = 'https://opendart.fss.or.kr/api/corpCode.xml'
        params = {'crtfc_key': self.api_key}
        
        try:
            response = requests.get(url, params=params)
            z = zipfile.ZipFile(io.BytesIO(response.content))
            xml_data = z.read('CORPCODE.xml').decode('utf-8')
            
            # XML을 사전 형태로 변환
            data_dict = xmltodict.parse(xml_data)
            
            # 매핑 사전 생성
            corp_codes = {}
            for company in data_dict['result']['list']:
                # 종목코드가 있는 기업만 저장
                if company.get('stock_code') and company.get('stock_code') != ' ':
                    corp_codes[company['stock_code']] = {
                        'corp_code': company['corp_code'],
                        'corp_name': company['corp_name'],
                    }
            
            # 캐시 저장
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(corp_codes, f, ensure_ascii=False, indent=2, cls=NumpyEncoder)
            
            
            return corp_codes
        except Exception as e:
            print(f"⚠️ DART API에서 종목 코드 로드 오류: {e}")
            return {}
    
    def get_corp_code(self, stock_code):
        """종목코드에서 DART 고유번호 조회"""
        if stock_code in self.corp_code_map:
            return self.corp_code_map[stock_code]['corp_code']
        return None
    
    def get_financial_statement(self, code, force_update=False):
        """재무제표 데이터 가져오기 - f_data 폴더 우선 사용"""
        try:
            # 종목 코드 정규화
            formatted_code = str(code).zfill(6)
            
            # 캐시 확인 (force_update가 False인 경우)
            if not force_update:
                cached_data = self._get_from_financial_cache(formatted_code)
                if cached_data:
                    return cached_data
            
            financial_data = None
            success_method = None
            
            # 방법 1: f_data 폴더에서 데이터 검색 (최우선)
            try:
                # 1-1. f_data 재무제표 데이터 시도
                f_financial_data = self.get_f_data_financial(formatted_code)
                if f_financial_data:
                    financial_data = f_financial_data
                    success_method = "f_data_financial"
                else:
                    # 1-2. f_data 펀더멘털 지표 시도
                    f_fundamental_data = self.get_f_data_fundamental(formatted_code)
                    if f_fundamental_data:
                        financial_data = f_fundamental_data
                        success_method = "f_data_fundamental"
                
                if financial_data:
                    pass  # 성공 메시지 제거
                else:
                    raise Exception("f_data에 데이터 없음")
            except Exception as e:
                financial_data = None
            
            # 방법 2: 지정된 JSON 파일들에서 데이터 검색 (2순위)
            if financial_data is None:
                try:
                    financial_data = self._get_from_specified_json_files(formatted_code)
                    if financial_data:
                        success_method = "지정된_JSON_파일"
                    else:
                        raise Exception("지정된 파일에 데이터 없음")
                except Exception as e:
                    financial_data = None
            
            # 방법 2: financial_data 폴더의 기타 데이터 활용
            if financial_data is None:
                try:
                    financial_data = self._get_from_financial_data_folder(formatted_code)
                    if financial_data:
                        success_method = "financial_data_folder"
                        print(f"✅ {code} financial_data 폴더에서 재무 데이터 발견")
                    else:
                        raise Exception("폴더에 데이터 없음")
                except Exception as e:
                    print(f"⚠️ {code} financial_data 폴더 검색 실패: {str(e)}")
                    financial_data = None
            
            # 방법 3: dart-fss 라이브러리 활용
            if financial_data is None and DART_FSS_AVAILABLE:
                try:
                    financial_data = self.get_dart_fss_financial_data(formatted_code)
                    if financial_data:
                        success_method = "dart_fss"
                        print(f"✅ {code} dart-fss로 재무 데이터 수집 성공")
                    else:
                        raise Exception("dart-fss 데이터 없음")
                except Exception as e:
                    print(f"⚠️ {code} dart-fss 재무 데이터 수집 실패: {str(e)}")
                    financial_data = None
            
            # 방법 4: pykrx 시장 기본 정보 활용
            if financial_data is None:
                try:
                    pykrx_data = self.get_market_fundamental_data(formatted_code)
                    if pykrx_data:
                        # pykrx 데이터가 유의미한지 확인
                        has_meaningful_data = any(
                            pykrx_data.get(key, 0) > 0 
                            for key in ['per', 'pbr', 'eps', 'bps', 'roe', 'operating_margin', 'dividend_yield']
                        )
                        
                        if has_meaningful_data:
                            financial_data = pykrx_data
                            success_method = "pykrx_market"
                            print(f"✅ {code} pykrx 시장 기본 정보로 재무 데이터 생성")
                        else:
                            print(f"⚠️ {code} pykrx 데이터가 모두 0이므로 기본값 생성으로 넘어감")
                            raise Exception("pykrx 데이터가 모두 0")
                    else:
                        raise Exception("pykrx 시장 데이터 없음")
                except Exception as e:
                    print(f"⚠️ {code} pykrx 시장 기본 정보 수집 실패: {str(e)}")
                    financial_data = None
            
            # 방법 5: DART API 직접 호출
            if financial_data is None and self.api_key:
                try:
                    financial_data = self._get_from_dart_api(formatted_code)
                    if financial_data:
                        success_method = "dart_api"
                        print(f"✅ {code} DART API로 재무 데이터 수집 성공")
                    else:
                        raise Exception("DART API 데이터 없음")
                except Exception as e:
                    print(f"⚠️ {code} DART API 재무 데이터 수집 실패: {str(e)}")
                    financial_data = None
            
            # 방법 6: 기본 재무 정보 생성 (최후의 수단)
            if financial_data is None:
                try:
                    financial_data = self._generate_default_financial_data(formatted_code)
                    success_method = "기본값"
                    print(f"⚠️ {code} 기본 재무 정보로 데이터 생성")
                except Exception as e:
                    print(f"⚠️ {code} 기본 재무 정보 생성 실패: {str(e)}")
                    financial_data = None
            
            # 결과 처리
            if financial_data:
                # 캐시에 저장
                try:
                    self._save_to_financial_cache(formatted_code, financial_data)
                except Exception as e:
                    print(f"⚠️ {code} 재무 데이터 캐시 저장 실패: {str(e)}")
                
                return financial_data
            else:
                print(f"⚠️ {code} 모든 재무 데이터 수집 방법 실패")
                return None
                
        except Exception as e:
            print(f"⚠️ {code} 재무 데이터 수집 중 전체 오류: {str(e)}")
            return None
    
    def _get_from_specified_json_files(self, code):
        """지정된 JSON 파일들에서 재무 데이터 검색"""
        try:
            # 다양한 코드 형태로 검색
            code_variations = [
                code,
                code.lstrip('0'),  # 앞의 0 제거
                f"A{code}",        # A 접두사 추가
                f"KRX:{code}"      # KRX 접두사 추가
            ]
            
            # 연도별 키 검색 (최신 연도부터)
            current_year = datetime.datetime.now().year
            years_to_check = [str(current_year), str(current_year-1), str(current_year-2)]
            
            # 1차 시도: 분기별 키 검색 (최우선) - 최신 분기부터
            for code_var in code_variations:
                for year in years_to_check:
                    for quarter in ['Q4', 'Q3', 'Q2', 'Q1']:
                        quarter_key = f"{code_var},{year},{quarter}"
                        if quarter_key in self.financial_data_cache:
                            data = self.financial_data_cache[quarter_key]
                            normalized_data = self._normalize_quarterly_data(data, f"지정된_JSON_{year}_{quarter}")
                            if normalized_data:
                                return normalized_data
            
            # 2차 시도: 연도별 키 검색 (collected_data 형태)
            for code_var in code_variations:
                for year in years_to_check:
                    year_key = f"{code_var},{year}"
                    if year_key in self.financial_data_cache:
                        data = self.financial_data_cache[year_key]
                        normalized_data = self._normalize_quarterly_data(data, f"지정된_JSON_{year}")
                        if normalized_data:
                            return normalized_data
                
                # 연도 없는 키도 확인
                if code_var in self.financial_data_cache:
                    data = self.financial_data_cache[code_var]
                    normalized_data = self._normalize_quarterly_data(data, "지정된_JSON")
                    if normalized_data:
                        return normalized_data
            
            return None
            
        except Exception as e:
            print(f"⚠️ 지정된 JSON 파일 검색 중 오류: {str(e)}")
            return None
    
    def _normalize_quarterly_data(self, data, source):
        """분기별 재무 데이터 정규화 - 실제 데이터 구조에 맞게 개선"""
        try:
            if not isinstance(data, dict):
                return None
            
            # 데이터 구조 확인
            # 1. collected_data.json 형태 (Q1, Q2, Q3, Q4 키가 있는 경우)
            if any(key.startswith('Q') for key in data.keys()):
                return self._normalize_collected_data_format(data, source)
            
            # 2. processed_data.json 형태 (단일 분기 데이터)
            elif 'sales' in data or 'op_income' in data or 'net_income' in data:
                return self._normalize_processed_data_format(data, source)
            
            return None
            
        except Exception as e:
            print(f"⚠️ 분기별 재무 데이터 정규화 실패: {str(e)}")
            return None
    
    def _normalize_collected_data_format(self, data, source):
        """collected_data.json 형태 데이터 정규화 (Q1, Q2, Q3, Q4 구조)"""
        try:
            # 최신 분기 데이터 찾기
            quarters = ['Q4', 'Q3', 'Q2', 'Q1']
            latest_quarter_data = None
            latest_quarter = None
            
            for quarter in quarters:
                if quarter in data:
                    latest_quarter_data = data[quarter]
                    latest_quarter = quarter
                    break
            
            if not latest_quarter_data:
                return None
            
            # 재무 데이터 정규화
            normalized = {}
            
            # 매출액 (단위: 원 -> 백만원)
            sales = latest_quarter_data.get('sales_cum', 0)
            normalized['sales'] = int(sales / 1000000) if sales else 0
            
            # 영업이익 (단위: 원 -> 백만원)
            operating_income = latest_quarter_data.get('op_cum', 0)
            normalized['operating_income'] = int(operating_income / 1000000) if operating_income else 0
            
            # 순이익 (단위: 원 -> 백만원)
            net_income = latest_quarter_data.get('net_cum', 0)
            normalized['net_income'] = int(net_income / 1000000) if net_income else 0
            
            # 자기자본 (단위: 원 -> 백만원)
            equity = latest_quarter_data.get('equity_cum', 0)
            normalized['equity'] = int(equity / 1000000) if equity else 0
            
            # 부채 (단위: 원 -> 백만원)
            debt = latest_quarter_data.get('debt_cum', 0)
            normalized['debt'] = int(debt / 1000000) if debt else 0
            
            # 총자산 계산
            normalized['assets'] = normalized['equity'] + normalized['debt']
            
            # 비율 계산
            if normalized['equity'] > 0:
                normalized['roe'] = (normalized['net_income'] / normalized['equity']) * 100
            else:
                normalized['roe'] = 0
            
            if normalized['assets'] > 0:
                normalized['debt_ratio'] = (normalized['debt'] / normalized['assets']) * 100
            else:
                normalized['debt_ratio'] = 0
            
            if normalized['sales'] > 0:
                normalized['operating_margin'] = (normalized['operating_income'] / normalized['sales']) * 100
                normalized['net_margin'] = (normalized['net_income'] / normalized['sales']) * 100
            else:
                normalized['operating_margin'] = 0
                normalized['net_margin'] = 0
            
            # 성장률 계산 (전년 동기 대비) - 개선된 로직
            normalized['sales_yoy'] = self._calculate_yoy_growth(data, latest_quarter, 'sales_cum')
            normalized['op_income_yoy'] = self._calculate_yoy_growth(data, latest_quarter, 'op_cum')
            normalized['net_income_yoy'] = self._calculate_yoy_growth(data, latest_quarter, 'net_cum')
            
            # 전분기 대비 성장률 계산 (QoQ)
            normalized['sales_qoq'] = self._calculate_qoq_growth(data, latest_quarter, 'sales_cum')
            normalized['op_income_qoq'] = self._calculate_qoq_growth(data, latest_quarter, 'op_cum')
            normalized['net_income_qoq'] = self._calculate_qoq_growth(data, latest_quarter, 'net_cum')
            
            normalized['data_source'] = source
            
            return normalized
            
        except Exception as e:
            print(f"⚠️ collected_data 형태 정규화 실패: {str(e)}")
            return None
    
    def _calculate_yoy_growth(self, data, current_quarter, field_name):
        """전년 동기 대비 성장률 계산 - 개선된 로직"""
        try:
            # 현재 분기 데이터
            current_data = data.get(current_quarter, {})
            current_value = current_data.get(field_name, 0)
            
            if current_value == 0:
                return 0
            
            # 현재 데이터에서 종목코드와 연도 추출
            current_year = current_data.get('year')
            symbol = current_data.get('symbol') or current_data.get('code')
            
            if not current_year or not symbol:
                return 0
            
            # 1차 시도: 전년 동기 대비 성장률
            prev_year = int(current_year) - 1
            prev_year_key = f"{symbol},{prev_year}"
            
            if prev_year_key in self.financial_data_cache:
                prev_year_data = self.financial_data_cache[prev_year_key]
                
                # 동일 분기 데이터 찾기
                if isinstance(prev_year_data, dict) and current_quarter in prev_year_data:
                    prev_quarter_data = prev_year_data[current_quarter]
                    prev_value = prev_quarter_data.get(field_name, 0)
                    
                    if prev_value > 0:
                        growth_rate = ((current_value - prev_value) / prev_value) * 100
                        return round(growth_rate, 2)
            
            # 2차 시도: 전년 말 대비 성장률 (동일 분기가 없는 경우)
            if prev_year_key in self.financial_data_cache:
                prev_year_data = self.financial_data_cache[prev_year_key]
                
                # collected_data 형태에서 가장 최근 분기 찾기
                if isinstance(prev_year_data, dict):
                    for quarter in ['Q4', 'Q3', 'Q2', 'Q1']:
                        if quarter in prev_year_data:
                            prev_quarter_data = prev_year_data[quarter]
                            prev_value = prev_quarter_data.get(field_name, 0)
                            
                            if prev_value > 0:
                                growth_rate = ((current_value - prev_value) / prev_value) * 100
                                return round(growth_rate, 2)
            
            # 3차 시도: processed_data 형태에서 전년 데이터와 비교
            if prev_year_key in self.financial_data_cache:
                prev_year_data = self.financial_data_cache[prev_year_key]
                
                if isinstance(prev_year_data, dict) and not any(k.startswith('Q') for k in prev_year_data.keys()):
                    # 필드명 매핑
                    field_mapping = {
                        'sales_cum': 'sales',
                        'op_cum': 'op_income', 
                        'net_cum': 'net_income'
                    }
                    mapped_field = field_mapping.get(field_name, field_name)
                    prev_value = prev_year_data.get(mapped_field, 0)
                    
                    if prev_value > 0:
                        growth_rate = ((current_value - prev_value) / prev_value) * 100
                        return round(growth_rate, 2)
            
            return 0
            
        except Exception as e:
            print(f"⚠️ 성장률 계산 실패: {str(e)}")
            return 0
    
    def _calculate_qoq_growth(self, data, current_quarter, field_name):
        """전분기 대비 성장률 계산 (QoQ)"""
        try:
            # 현재 분기 데이터
            current_data = data.get(current_quarter, {})
            current_value = current_data.get(field_name, 0)
            
            if current_value == 0:
                return 0
            
            # 현재 데이터에서 종목코드와 연도 추출
            current_year = current_data.get('year')
            symbol = current_data.get('symbol') or current_data.get('code')
            
            if not current_year or not symbol:
                return 0
            
            # 분기 순서 정의
            quarter_order = ['Q1', 'Q2', 'Q3', 'Q4']
            
            # 현재 분기의 인덱스 찾기
            try:
                current_index = quarter_order.index(current_quarter)
            except ValueError:
                return 0
            
            # 전분기 찾기
            if current_index > 0:
                # 같은 연도 내 전분기
                prev_quarter = quarter_order[current_index - 1]
                prev_quarter_key = f"{symbol},{current_year},{prev_quarter}"
                
                if prev_quarter_key in self.financial_data_cache:
                    prev_data = self.financial_data_cache[prev_quarter_key]
                    prev_value = prev_data.get(field_name, 0)
                    
                    if prev_value > 0:
                        growth_rate = ((current_value - prev_value) / prev_value) * 100
                        return round(growth_rate, 2)
            else:
                # Q1인 경우 전년도 Q4와 비교
                prev_year = int(current_year) - 1
                prev_q4_key = f"{symbol},{prev_year},Q4"
                
                if prev_q4_key in self.financial_data_cache:
                    prev_q4_data = self.financial_data_cache[prev_q4_key]
                    prev_value = prev_q4_data.get(field_name, 0)
                    
                    if prev_value > 0:
                        growth_rate = ((current_value - prev_value) / prev_value) * 100
                        return round(growth_rate, 2)
                
                # collected_data 형태에서도 찾기
                prev_year_key = f"{symbol},{prev_year}"
                if prev_year_key in self.financial_data_cache:
                    prev_year_data = self.financial_data_cache[prev_year_key]
                    
                    if isinstance(prev_year_data, dict) and 'Q4' in prev_year_data:
                        prev_q4_data = prev_year_data['Q4']
                        # 필드명 매핑 (processed_data -> collected_data)
                        field_mapping = {
                            'sales': 'sales_cum',
                            'op_income': 'op_cum',
                            'net_income': 'net_cum'
                        }
                        mapped_field = field_mapping.get(field_name, field_name)
                        prev_value = prev_q4_data.get(mapped_field, 0)
                        
                        if prev_value > 0:
                            growth_rate = ((current_value - prev_value) / prev_value) * 100
                            return round(growth_rate, 2)
            
            return 0
            
        except Exception as e:
            print(f"⚠️ processed_data QoQ 성장률 계산 실패: {str(e)}")
            return 0
    
    def _normalize_processed_data_format(self, data, source):
        """processed_data.json 형태 데이터 정규화 (단일 분기 데이터)"""
        try:
            normalized = {}
            
            # 매출액 (단위: 원 -> 백만원)
            sales = data.get('sales', 0)
            normalized['sales'] = int(sales / 1000000) if sales else 0
            
            # 영업이익 (단위: 원 -> 백만원)
            operating_income = data.get('op_income', 0)
            normalized['operating_income'] = int(operating_income / 1000000) if operating_income else 0
            
            # 순이익 (단위: 원 -> 백만원)
            net_income = data.get('net_income', 0)
            normalized['net_income'] = int(net_income / 1000000) if net_income else 0
            
            # 자기자본 (단위: 원 -> 백만원)
            equity = data.get('equity', 0)
            normalized['equity'] = int(equity / 1000000) if equity else 0
            
            # 부채 (단위: 원 -> 백만원)
            debt = data.get('debt', 0)
            normalized['debt'] = int(debt / 1000000) if debt else 0
            
            # 총자산 계산
            normalized['assets'] = normalized['equity'] + normalized['debt']
            
            # 비율 계산
            if normalized['equity'] > 0:
                normalized['roe'] = (normalized['net_income'] / normalized['equity']) * 100
            else:
                normalized['roe'] = 0
            
            if normalized['assets'] > 0:
                normalized['debt_ratio'] = (normalized['debt'] / normalized['assets']) * 100
            else:
                normalized['debt_ratio'] = 0
            
            if normalized['sales'] > 0:
                normalized['operating_margin'] = (normalized['operating_income'] / normalized['sales']) * 100
                normalized['net_margin'] = (normalized['net_income'] / normalized['sales']) * 100
            else:
                normalized['operating_margin'] = 0
                normalized['net_margin'] = 0
            
            # 성장률 계산 (전년 동기 대비) - 개선된 로직
            normalized['sales_yoy'] = self._calculate_processed_yoy_growth(data, 'sales')
            normalized['op_income_yoy'] = self._calculate_processed_yoy_growth(data, 'op_income')
            normalized['net_income_yoy'] = self._calculate_processed_yoy_growth(data, 'net_income')
            
            # 전분기 대비 성장률 계산 (QoQ)
            normalized['sales_qoq'] = self._calculate_processed_qoq_growth(data, 'sales')
            normalized['op_income_qoq'] = self._calculate_processed_qoq_growth(data, 'op_income')
            normalized['net_income_qoq'] = self._calculate_processed_qoq_growth(data, 'net_income')
            
            # 추가 정보
            normalized['year'] = data.get('year', 0)
            normalized['quarter'] = data.get('quarter', '')
            normalized['company_name'] = data.get('company_name', '')
            
            normalized['data_source'] = source
            
            return normalized
            
        except Exception as e:
            print(f"⚠️ processed_data 형태 정규화 실패: {str(e)}")
            return None
    
    def _calculate_processed_yoy_growth(self, data, field_name):
        """processed_data 형태에서 전년 동기 대비 성장률 계산 - 개선된 로직"""
        try:
            current_value = data.get(field_name, 0)
            
            if current_value == 0:
                return 0
            
            # 현재 데이터에서 종목코드, 연도, 분기 추출
            symbol = data.get('symbol') or data.get('code')
            current_year = data.get('year')
            current_quarter = data.get('quarter')
            
            if not symbol or not current_year:
                return 0
            
            # 1차 시도: 전년 동기 대비 성장률
            prev_year = int(current_year) - 1
            prev_year_key = f"{symbol},{prev_year}"
            
            if prev_year_key in self.financial_data_cache:
                prev_year_data = self.financial_data_cache[prev_year_key]
                
                # processed_data 형태에서 동일 분기 찾기
                if isinstance(prev_year_data, dict) and prev_year_data.get('quarter') == current_quarter:
                    prev_value = prev_year_data.get(field_name, 0)
                    
                    if prev_value > 0:
                        growth_rate = ((current_value - prev_value) / prev_value) * 100
                        return round(growth_rate, 2)
                
                # collected_data 형태에서 동일 분기 찾기
                elif isinstance(prev_year_data, dict) and current_quarter and current_quarter in prev_year_data:
                    prev_quarter_data = prev_year_data[current_quarter]
                    # 필드명 매핑 (processed_data -> collected_data)
                    field_mapping = {
                        'sales': 'sales_cum',
                        'op_income': 'op_cum',
                        'net_income': 'net_cum'
                    }
                    mapped_field = field_mapping.get(field_name, field_name)
                    prev_value = prev_quarter_data.get(mapped_field, 0)
                    
                    if prev_value > 0:
                        growth_rate = ((current_value - prev_value) / prev_value) * 100
                        return round(growth_rate, 2)
            
            # 2차 시도: 전년 말 대비 성장률 (동일 분기가 없는 경우)
            if prev_year_key in self.financial_data_cache:
                prev_year_data = self.financial_data_cache[prev_year_key]
                
                # collected_data 형태에서 가장 최근 분기 찾기
                if isinstance(prev_year_data, dict) and any(k.startswith('Q') for k in prev_year_data.keys()):
                    for quarter in ['Q4', 'Q3', 'Q2', 'Q1']:
                        if quarter in prev_year_data:
                            prev_quarter_data = prev_year_data[quarter]
                            field_mapping = {
                                'sales': 'sales_cum',
                                'op_income': 'op_cum',
                                'net_income': 'net_cum'
                            }
                            mapped_field = field_mapping.get(field_name, field_name)
                            prev_value = prev_quarter_data.get(mapped_field, 0)
                            
                            if prev_value > 0:
                                growth_rate = ((current_value - prev_value) / prev_value) * 100
                                return round(growth_rate, 2)
                
                # processed_data 형태에서 전년 데이터와 비교
                elif isinstance(prev_year_data, dict):
                    prev_value = prev_year_data.get(field_name, 0)
                    
                    if prev_value > 0:
                        growth_rate = ((current_value - prev_value) / prev_value) * 100
                        return round(growth_rate, 2)
            
            return 0
            
        except Exception as e:
            print(f"⚠️ processed_data 성장률 계산 실패: {str(e)}")
            return 0
    
    def _calculate_processed_qoq_growth(self, data, field_name):
        """processed_data 형태에서 전분기 대비 성장률 계산 (QoQ)"""
        try:
            current_value = data.get(field_name, 0)
            
            if current_value == 0:
                return 0
            
            # 현재 데이터에서 종목코드, 연도, 분기 추출
            symbol = data.get('symbol') or data.get('code')
            current_year = data.get('year')
            current_quarter = data.get('quarter')
            
            if not symbol or not current_year or not current_quarter:
                return 0
            
            # 분기 순서 정의
            quarter_order = ['Q1', 'Q2', 'Q3', 'Q4']
            
            # 현재 분기의 인덱스 찾기
            try:
                current_index = quarter_order.index(current_quarter)
            except ValueError:
                return 0
            
            # 전분기 찾기
            if current_index > 0:
                # 같은 연도 내 전분기
                prev_quarter = quarter_order[current_index - 1]
                prev_quarter_key = f"{symbol},{current_year},{prev_quarter}"
                
                if prev_quarter_key in self.financial_data_cache:
                    prev_data = self.financial_data_cache[prev_quarter_key]
                    prev_value = prev_data.get(field_name, 0)
                    
                    if prev_value > 0:
                        growth_rate = ((current_value - prev_value) / prev_value) * 100
                        return round(growth_rate, 2)
            else:
                # Q1인 경우 전년도 Q4와 비교
                prev_year = int(current_year) - 1
                prev_q4_key = f"{symbol},{prev_year},Q4"
                
                if prev_q4_key in self.financial_data_cache:
                    prev_q4_data = self.financial_data_cache[prev_q4_key]
                    prev_value = prev_q4_data.get(field_name, 0)
                    
                    if prev_value > 0:
                        growth_rate = ((current_value - prev_value) / prev_value) * 100
                        return round(growth_rate, 2)
                
                # collected_data 형태에서도 찾기
                prev_year_key = f"{symbol},{prev_year}"
                if prev_year_key in self.financial_data_cache:
                    prev_year_data = self.financial_data_cache[prev_year_key]
                    
                    if isinstance(prev_year_data, dict) and 'Q4' in prev_year_data:
                        prev_q4_data = prev_year_data['Q4']
                        # 필드명 매핑 (processed_data -> collected_data)
                        field_mapping = {
                            'sales': 'sales_cum',
                            'op_income': 'op_cum',
                            'net_income': 'net_cum'
                        }
                        mapped_field = field_mapping.get(field_name, field_name)
                        prev_value = prev_q4_data.get(mapped_field, 0)
                        
                        if prev_value > 0:
                            growth_rate = ((current_value - prev_value) / prev_value) * 100
                            return round(growth_rate, 2)
            
            return 0
            
        except Exception as e:
            print(f"⚠️ processed_data QoQ 성장률 계산 실패: {str(e)}")
            return 0
    
    def _get_from_financial_data_folder(self, code):
        """financial_data 폴더에서 재무 데이터 검색 - 개선된 버전"""
        try:
            # 다양한 코드 형태로 검색
            code_variations = [
                code,
                code.lstrip('0'),  # 앞의 0 제거
                f"A{code}",        # A 접두사 추가
                f"KRX:{code}"      # KRX 접두사 추가
            ]
            
            # 1. collected_data.json에서 검색
            collected_data_path = os.path.join(self.financial_data_dir, 'collected_data.json')
            if os.path.exists(collected_data_path):
                try:
                    with open(collected_data_path, 'r', encoding='utf-8') as f:
                        collected_data = json.load(f)
                    
                    for code_var in code_variations:
                        if code_var in collected_data:
                            data = collected_data[code_var]
                            return self._normalize_financial_data(data, "collected_data.json")
                        
                        # 연도별 키도 확인
                        for year in ['2024', '2023', '2022']:
                            year_key = f"{code_var},{year}"
                            if year_key in collected_data:
                                data = collected_data[year_key]
                                return self._normalize_financial_data(data, f"collected_data.json ({year})")
                except Exception as e:
                    print(f"⚠️ collected_data.json 검색 실패: {str(e)}")
            
            # 2. processed_data.json에서 검색
            processed_data_path = os.path.join(self.financial_data_dir, 'processed_data.json')
            if os.path.exists(processed_data_path):
                try:
                    with open(processed_data_path, 'r', encoding='utf-8') as f:
                        processed_data = json.load(f)
                    
                    for code_var in code_variations:
                        if code_var in processed_data:
                            data = processed_data[code_var]
                            return self._normalize_financial_data(data, "processed_data.json")
                        
                        # 연도별 키도 확인
                        for year in ['2024', '2023', '2022']:
                            year_key = f"{code_var},{year}"
                            if year_key in processed_data:
                                data = processed_data[year_key]
                                return self._normalize_financial_data(data, f"processed_data.json ({year})")
                except Exception as e:
                    print(f"⚠️ processed_data.json 검색 실패: {str(e)}")
            
            # 3. CSV 파일들에서 검색
            csv_files = [
                'kospi_financial_quarterly.csv',
                'kospi_financial_2023.csv',
                'kospi_financial_2024.csv',
                'kospi_financial_2025.csv'
            ]
            
            for csv_file in csv_files:
                csv_path = os.path.join(self.financial_data_dir, csv_file)
                if os.path.exists(csv_path):
                    try:
                        df = pd.read_csv(csv_path)
                        
                        # 다양한 컬럼명으로 검색
                        code_columns = ['symbol', 'code', 'stock_code', 'ticker', 'Code', 'Symbol']
                        
                        for col in code_columns:
                            if col in df.columns:
                                for code_var in code_variations:
                                    matching_rows = df[df[col].astype(str).str.zfill(6) == code_var]
                                    if not matching_rows.empty:
                                        row = matching_rows.iloc[0]
                                        data = self._extract_financial_data_from_row(row, csv_file)
                                        if data:
                                            return data
                    except Exception as e:
                        print(f"⚠️ {csv_file} 검색 실패: {str(e)}")
                        continue
            
            return None
            
        except Exception as e:
            print(f"⚠️ financial_data 폴더 검색 중 오류: {str(e)}")
            return None
    
    def _normalize_financial_data(self, data, source):
        """재무 데이터 정규화"""
        try:
            normalized = {}
            
            # 매출액 추출
            sales_keys = ['sales', 'revenue', '매출액', 'sales_cum', '매출', 'total_revenue']
            normalized['sales'] = self._safe_get_value(data, sales_keys)
            
            # 영업이익 추출
            op_keys = ['operating_income', 'op_income', '영업이익', 'op_cum', 'operating_profit']
            normalized['operating_income'] = self._safe_get_value(data, op_keys)
            
            # 순이익 추출
            net_keys = ['net_income', '순이익', 'net_cum', 'net_profit', 'profit']
            normalized['net_income'] = self._safe_get_value(data, net_keys)
            
            # 자기자본 추출
            equity_keys = ['equity', '자기자본', 'equity_cum', 'shareholders_equity', 'total_equity']
            normalized['equity'] = self._safe_get_value(data, equity_keys)
            
            # 부채 추출
            debt_keys = ['debt', '부채', 'debt_cum', 'total_debt', 'liabilities']
            normalized['debt'] = self._safe_get_value(data, debt_keys)
            
            # 총자산 추출
            asset_keys = ['assets', '총자산', 'total_assets', 'asset_cum']
            normalized['assets'] = self._safe_get_value(data, asset_keys)
            
            # 비율 계산
            if normalized['equity'] > 0:
                normalized['roe'] = (normalized['net_income'] / normalized['equity']) * 100
            else:
                normalized['roe'] = 0
            
            if normalized['assets'] > 0:
                normalized['debt_ratio'] = (normalized['debt'] / normalized['assets']) * 100
            else:
                normalized['debt_ratio'] = 0
            
            if normalized['sales'] > 0:
                normalized['operating_margin'] = (normalized['operating_income'] / normalized['sales']) * 100
                normalized['net_margin'] = (normalized['net_income'] / normalized['sales']) * 100
            else:
                normalized['operating_margin'] = 0
                normalized['net_margin'] = 0
            
            normalized['data_source'] = source
            
            return normalized
            
        except Exception as e:
            print(f"⚠️ 재무 데이터 정규화 실패: {str(e)}")
            return None
    
    def _get_from_dart_api(self, code):
        """DART API에서 직접 재무 데이터 가져오기"""
        try:
            if not self.api_key:
                return None
            
            # 기업 고유번호 가져오기
            corp_code = self.get_corp_code(code)
            if not corp_code:
                return None
            
            # 최근 3년간 연간 재무제표 요청
            current_year = datetime.datetime.now().year
            
            for year in range(current_year, current_year - 3, -1):
                try:
                    url = 'https://opendart.fss.or.kr/api/fnlttSinglAcnt.json'
                    params = {
                        'crtfc_key': self.api_key,
                        'corp_code': corp_code,
                        'bsns_year': str(year),
                        'reprt_code': '11011',  # 사업보고서
                        'fs_div': 'CFS'  # 연결재무제표
                    }
                    
                    response = requests.get(url, params=params, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('status') == '000':
                            financial_data = self._parse_dart_financial_data(data.get('list', []))
                            if financial_data:
                                return financial_data
                except Exception as e:
                    print(f"⚠️ DART API {year}년 데이터 요청 실패: {str(e)}")
                    continue
            
            return None
            
        except Exception as e:
            print(f"⚠️ DART API 호출 중 오류: {str(e)}")
            return None
    
    def _parse_dart_financial_data(self, dart_list):
        """DART API 응답 데이터 파싱"""
        try:
            financial_data = {
                'sales': 0,
                'operating_income': 0,
                'net_income': 0,
                'equity': 0,
                'debt': 0,
                'assets': 0
            }
            
            for item in dart_list:
                account_nm = item.get('account_nm', '')
                thstrm_amount = item.get('thstrm_amount', '0')
                
                # 숫자 변환
                try:
                    amount = int(thstrm_amount.replace(',', '')) if thstrm_amount != '-' else 0
                except:
                    amount = 0
                
                # 계정과목별 매핑
                if '매출액' in account_nm or '수익' in account_nm:
                    financial_data['sales'] = max(financial_data['sales'], amount)
                elif '영업이익' in account_nm:
                    financial_data['operating_income'] = amount
                elif '당기순이익' in account_nm or '순이익' in account_nm:
                    financial_data['net_income'] = amount
                elif '자기자본' in account_nm:
                    financial_data['equity'] = amount
                elif '부채총계' in account_nm:
                    financial_data['debt'] = amount
                elif '자산총계' in account_nm:
                    financial_data['assets'] = amount
            
            # 유효한 데이터가 있는지 확인
            if any(financial_data[key] > 0 for key in ['sales', 'operating_income', 'net_income']):
                return financial_data
            else:
                return None
                
        except Exception as e:
            print(f"⚠️ DART 재무 데이터 파싱 실패: {str(e)}")
            return None
    
    def _generate_default_financial_data(self, code):
        """기본 재무 정보 생성 (최후의 수단) - 개선된 버전"""
        try:
            # 종목 코드 정규화
            formatted_code = str(code).zfill(6)
            
            # 업종별 기본 추정값 설정
            sector_defaults = {
                # 기술주 (IT, 반도체, 바이오 등)
                'tech': {
                    'roe': 8.0, 'operating_margin': 12.0, 'debt_ratio': 40.0,
                    'sales_yoy': 15.0, 'op_income_yoy': 12.0,
                    'per': 25.0, 'pbr': 3.0, 'dividend_yield': 1.0
                },
                # 제조업 (자동차, 화학, 철강 등)
                'manufacturing': {
                    'roe': 6.0, 'operating_margin': 8.0, 'debt_ratio': 60.0,
                    'sales_yoy': 8.0, 'op_income_yoy': 6.0,
                    'per': 15.0, 'pbr': 1.5, 'dividend_yield': 2.5
                },
                # 서비스업 (금융, 유통, 통신 등)
                'service': {
                    'roe': 7.0, 'operating_margin': 10.0, 'debt_ratio': 50.0,
                    'sales_yoy': 10.0, 'op_income_yoy': 8.0,
                    'per': 18.0, 'pbr': 2.0, 'dividend_yield': 2.0
                },
                # 기본값 (업종 불명)
                'default': {
                    'roe': 5.0, 'operating_margin': 8.0, 'debt_ratio': 50.0,
                    'sales_yoy': 8.0, 'op_income_yoy': 5.0,
                    'per': 20.0, 'pbr': 2.0, 'dividend_yield': 1.5
                }
            }
            
            # 종목 코드별 업종 추정
            def estimate_sector(code):
                """종목 코드를 기반으로 업종 추정"""
                code_int = int(code)
                
                # 코스닥 기술주 범위 (대략적 추정)
                if code_int >= 140000 and code_int <= 150000:  # 파크시스템스 등 기술주
                    return 'tech'
                elif code_int >= 200000 and code_int <= 300000:  # 기술 관련
                    return 'tech'
                elif code_int >= 5000 and code_int <= 10000:  # 대기업 제조업
                    return 'manufacturing'
                elif code_int >= 30000 and code_int <= 40000:  # IT 서비스
                    return 'tech'
                elif code_int >= 50000 and code_int <= 70000:  # 제조업
                    return 'manufacturing'
                else:
                    return 'default'
            
            # 업종 추정 및 기본값 설정
            estimated_sector = estimate_sector(formatted_code)
            defaults = sector_defaults[estimated_sector]
            
            print(f"  📊 {code} 추정 업종: {estimated_sector}")
            print(f"  📊 기본 재무 지표 생성 (추정값)")
            
            # 기본 재무 정보 생성
            financial_data = {
                # 수익성 지표
                'roe': defaults['roe'],
                'operating_margin': defaults['operating_margin'],
                'net_margin': defaults['operating_margin'] * 0.7,  # 영업이익률의 70%로 추정
                
                # 안정성 지표
                'debt_ratio': defaults['debt_ratio'],
                
                # 성장률 지표 (YoY)
                'sales_yoy': defaults['sales_yoy'],
                'op_income_yoy': defaults['op_income_yoy'],
                'net_income_yoy': defaults['op_income_yoy'] * 1.2,  # 영업이익 성장률의 120%로 추정
                
                # 성장률 지표 (QoQ) - YoY의 1/4로 추정
                'sales_qoq': defaults['sales_yoy'] / 4,
                'op_income_qoq': defaults['op_income_yoy'] / 4,
                'net_income_qoq': defaults['op_income_yoy'] * 1.2 / 4,
                
                # 가치평가 지표
                'per': defaults['per'],
                'pbr': defaults['pbr'],
                'dividend_yield': defaults['dividend_yield'],
                
                # 절대값 (가상)
                'sales': 100000000000,  # 1000억 가정
                'operating_income': int(100000000000 * defaults['operating_margin'] / 100),
                'net_income': int(100000000000 * defaults['operating_margin'] * 0.7 / 100),
                'equity': int(100000000000 / defaults['roe'] * 100),
                'debt': int(100000000000 / defaults['roe'] * 100 * defaults['debt_ratio'] / 100),
                'assets': int(100000000000 / defaults['roe'] * 100 * (1 + defaults['debt_ratio'] / 100)),
                
                # EPS, BPS 추정 (발행주식 1000만주 가정)
                'eps': int(100000000000 * defaults['operating_margin'] * 0.7 / 100 / 10000000),
                'bps': int(100000000000 / defaults['roe'] * 100 / 10000000),
                'dps': int(100000000000 * defaults['operating_margin'] * 0.7 / 100 / 10000000 * defaults['dividend_yield'] / 100),
                
                # 메타데이터
                'year': datetime.datetime.now().year,
                'quarter': f"Q{((datetime.datetime.now().month - 1) // 3) + 1}",
                'company_name': f"종목{code}",
                'data_source': f'추정값_{estimated_sector}',
                'source': f'estimated_{estimated_sector}',
                'last_update': datetime.datetime.now().strftime('%Y-%m-%d'),
                'note': '실제 데이터 부족으로 업종별 평균값 기반 추정'
            }
            
            print(f"  📊 추정 ROE: {financial_data['roe']:.1f}%")
            print(f"  📊 추정 영업이익률: {financial_data['operating_margin']:.1f}%")
            print(f"  📊 추정 성장률: 매출 {financial_data['sales_yoy']:.1f}%, 영업이익 {financial_data['op_income_yoy']:.1f}%")
            print(f"  📊 추정 밸류에이션: PER {financial_data['per']:.1f}배, PBR {financial_data['pbr']:.1f}배")
            
            return financial_data
            
        except Exception as e:
            print(f"⚠️ {code} 기본 재무 정보 생성 실패: {str(e)}")
            # 최소한의 기본값
            return {
                'roe': 5.0,
                'operating_margin': 8.0,
                'debt_ratio': 50.0,
                'sales_yoy': 5.0,
                'op_income_yoy': 5.0,
                'per': 20.0,
                'pbr': 2.0,
                'dividend_yield': 1.0,
                'source': 'minimal_default',
                'note': '최소 기본값'
            }
    
    def _save_to_financial_cache(self, code, data):
        """재무 데이터를 캐시에 저장"""
        try:
            cache_file = os.path.join(self.financial_cache_dir, f'{code}_financial.json')
            
            # 타임스탬프 추가
            data_with_timestamp = data.copy()
            data_with_timestamp['cached_at'] = datetime.datetime.now().isoformat()
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(data_with_timestamp, f, ensure_ascii=False, indent=2, cls=NumpyEncoder)
            
        except Exception as e:
            print(f"⚠️ 재무 데이터 캐시 저장 실패: {str(e)}")
    
    def get_market_fundamental_data(self, code):
        """pykrx를 사용하여 시장 기본 정보 가져오기 (PER, PBR, EPS, BPS, 배당수익률, DPS) - 개선된 버전"""
        try:
            # 종목 코드 정규화 (6자리)
            formatted_code = str(code).zfill(6)
            
            # 현재 날짜부터 최근 10일간 시도 (주말 제외)
            today = datetime.datetime.now()
            
            fundamental_data = None
            market_cap_data = None
            used_date = None
            
            for i in range(10):
                try_date = today - datetime.timedelta(days=i)
                # 주말 건너뛰기
                if try_date.weekday() >= 5:  # 토요일(5), 일요일(6)
                    continue
                    
                date_str = try_date.strftime('%Y%m%d')
                
                try:
                    # 방법 1: 시장 기본 정보 (PER, PBR, EPS, BPS, DIV, DPS) 가져오기
                    fundamental_data = stock.get_market_fundamental_by_ticker(date_str, market="ALL")
                    
                    if fundamental_data is not None and not fundamental_data.empty:
                        if formatted_code in fundamental_data.index:
                            print(f"✅ {code} pykrx 기본 정보 수집 성공 ({date_str})")
                            used_date = date_str
                            break
                    
                    # 방법 2: 시가총액 정보 (백업용)
                    if market_cap_data is None:
                        market_cap_data = stock.get_market_cap_by_ticker(date_str, market="ALL")
                        if market_cap_data is not None and not market_cap_data.empty:
                            if formatted_code in market_cap_data.index:
                                used_date = date_str
                                
                except Exception:
                    continue
            
            # 데이터 처리
            result_data = {
                'per': 0.0,
                'pbr': 0.0,
                'eps': 0.0,
                'bps': 0.0,
                'dividend_yield': 0.0,
                'dps': 0.0,
                'market_cap': 0.0,
                'date': used_date or today.strftime('%Y%m%d'),
                'source': 'default'
            }
            
            # 기본 정보가 있는 경우
            if fundamental_data is not None and formatted_code in fundamental_data.index:
                row = fundamental_data.loc[formatted_code]
                
                # 안전한 숫자 변환
                def safe_float(value, default=0.0):
                    try:
                        if pd.notna(value) and value not in ['', '-', None]:
                            return float(value)
                        return default
                    except (ValueError, TypeError):
                        return default
                
                result_data.update({
                    'per': safe_float(row.get('PER', 0)),
                    'pbr': safe_float(row.get('PBR', 0)),
                    'eps': safe_float(row.get('EPS', 0)),
                    'bps': safe_float(row.get('BPS', 0)),
                    'dividend_yield': safe_float(row.get('DIV', 0)),  # 배당수익률
                    'dps': safe_float(row.get('DPS', 0)),  # 주당배당금
                    'source': 'pykrx_fundamental'
                })
                
                # 시가총액 정보 추가
                if market_cap_data is not None and formatted_code in market_cap_data.index:
                    market_row = market_cap_data.loc[formatted_code]
                    result_data['market_cap'] = safe_float(market_row.get('시가총액', 0))
                
                # 추가 계산된 지표들 (pykrx 기본 정보 기반)
                per = result_data['per']
                pbr = result_data['pbr']
                eps = result_data['eps']
                bps = result_data['bps']
                
                # ROE 추정 (EPS / BPS * 100)
                if bps > 0 and eps > 0:
                    estimated_roe = (eps / bps) * 100
                    result_data['roe'] = estimated_roe
                    print(f"  추정 ROE: {estimated_roe:.2f}% (EPS {eps} / BPS {bps})")
                
                # 영업이익률 추정 (보수적으로 순이익률의 1.2배로 가정)
                if eps > 0 and bps > 0:
                    # 간단한 추정: ROE가 있으면 영업이익률을 ROE의 1.5배로 추정
                    if 'roe' in result_data and result_data['roe'] > 0:
                        estimated_operating_margin = min(result_data['roe'] * 1.5, 50)  # 최대 50%로 제한
                        result_data['operating_margin'] = estimated_operating_margin
                        print(f"  추정 영업이익률: {estimated_operating_margin:.2f}%")
                
                # 부채비율 추정 (PBR과 ROE 기반 간단 추정)
                if pbr > 0 and 'roe' in result_data and result_data['roe'] > 0:
                    # 보수적 추정: PBR이 높을수록 부채비율 낮게 추정
                    if pbr <= 1.0:
                        estimated_debt_ratio = 80  # PBR 1배 이하면 부채비율 높게 추정
                    elif pbr <= 2.0:
                        estimated_debt_ratio = 50  # PBR 2배 이하면 중간
                    else:
                        estimated_debt_ratio = 30  # PBR 2배 초과면 낮게 추정
                    
                    result_data['debt_ratio'] = estimated_debt_ratio
                    print(f"  추정 부채비율: {estimated_debt_ratio:.1f}%")
                
                # 성장률 추정 (배당 정보 기반 간단 추정)
                if result_data['dividend_yield'] > 0:
                    # 배당수익률이 높으면 안정적, 낮으면 성장형으로 가정
                    if result_data['dividend_yield'] >= 3:
                        # 고배당 = 안정형 = 낮은 성장률
                        estimated_growth = 5
                    elif result_data['dividend_yield'] >= 1:
                        # 중배당 = 중간 성장률
                        estimated_growth = 10
                    else:
                        # 저배당 = 성장형 = 높은 성장률
                        estimated_growth = 15
                    
                    result_data['sales_yoy'] = estimated_growth
                    result_data['op_income_yoy'] = estimated_growth
                    print(f"  추정 성장률: {estimated_growth:.1f}% (배당 기반)")
                
                print(f"  PER: {per:.1f}, PBR: {pbr:.1f}, EPS: {eps:.0f}, BPS: {bps:.0f}")
                print(f"  배당수익률: {result_data['dividend_yield']:.1f}%, DPS: {result_data['dps']:.0f}")
                
            # 시가총액 정보만 있는 경우
            elif market_cap_data is not None and formatted_code in market_cap_data.index:
                market_row = market_cap_data.loc[formatted_code]
                result_data.update({
                    'market_cap': safe_float(market_row.get('시가총액', 0)),
                    'source': 'pykrx_market_cap'
                })
                print(f"✅ {code} pykrx 시가총액 정보만 수집 성공")
            
            return result_data
            
        except Exception as e:
            print(f"⚠️ {code} pykrx 기본 정보 수집 실패: {str(e)}")
            # 전체적인 오류 발생 시 기본값 반환
            return {
                'per': 0.0,
                'pbr': 0.0,
                'eps': 0.0,
                'bps': 0.0,
                'dividend_yield': 0.0,
                'dps': 0.0,
                'market_cap': 0.0,
                'date': datetime.datetime.now().strftime('%Y%m%d'),
                'source': 'error_default'
            }
    
    def get_dart_fss_financial_data(self, code):
        """dart-fss를 사용하여 재무 데이터 가져오기"""
        if not DART_FSS_AVAILABLE or not self.api_key:
            return None
        
        try:
            # 종목 코드 정규화 (6자리)
            formatted_code = str(code).zfill(6)
            
            # 회사 정보 검색
            try:
                corp = dart.corp.Corp(formatted_code)
                if not corp or not corp.corp_name:
                    return None
            except Exception:
                return None
            
            # 최근 3년간 연간 재무제표 추출
            current_year = datetime.datetime.now().year
            start_date = f"{current_year - 2}0101"  # 3년 전부터
            
            try:
                # 연간 연결재무제표 추출 (우선)
                fs = corp.extract_fs(bgn_de=start_date, report_tp='annual', separate=False)
                
                if fs is None or fs.empty:
                    # 개별재무제표로 재시도
                    fs = corp.extract_fs(bgn_de=start_date, report_tp='annual', separate=True)
                
                if fs is None or fs.empty:
                    return None
                
                # 최신 연도 데이터 추출
                latest_year = fs.index.max()
                latest_data = fs.loc[latest_year]
                
                # 재무 지표 계산
                financial_data = {}
                
                # 매출액
                revenue = self._safe_get_value(latest_data, ['매출액', '수익(매출액)', '영업수익'])
                financial_data['revenue'] = revenue
                
                # 영업이익
                operating_profit = self._safe_get_value(latest_data, ['영업이익', '영업이익(손실)'])
                financial_data['operating_profit'] = operating_profit
                
                # 당기순이익
                net_income = self._safe_get_value(latest_data, ['당기순이익', '당기순이익(손실)', '순이익'])
                financial_data['net_income'] = net_income
                
                # 자산총계
                total_assets = self._safe_get_value(latest_data, ['자산총계', '자산총액'])
                financial_data['total_assets'] = total_assets
                
                # 자본총계
                total_equity = self._safe_get_value(latest_data, ['자본총계', '자본총액', '자기자본'])
                financial_data['total_equity'] = total_equity
                
                # 부채총계
                total_liabilities = self._safe_get_value(latest_data, ['부채총계', '부채총액'])
                financial_data['total_liabilities'] = total_liabilities
                
                # 비율 계산
                if revenue and revenue > 0:
                    financial_data['operating_margin'] = (operating_profit / revenue * 100) if operating_profit else 0
                    financial_data['net_margin'] = (net_income / revenue * 100) if net_income else 0
                
                if total_equity and total_equity > 0:
                    financial_data['roe'] = (net_income / total_equity * 100) if net_income else 0
                
                if total_equity and total_equity > 0:
                    financial_data['debt_ratio'] = (total_liabilities / total_equity * 100) if total_liabilities else 0
                
                # 성장률 계산 (전년 대비)
                if len(fs) >= 2:
                    prev_year_data = fs.iloc[-2]  # 전년 데이터
                    
                    prev_revenue = self._safe_get_value(prev_year_data, ['매출액', '수익(매출액)', '영업수익'])
                    if prev_revenue and prev_revenue > 0 and revenue:
                        financial_data['revenue_growth'] = ((revenue - prev_revenue) / prev_revenue * 100)
                    
                    prev_operating_profit = self._safe_get_value(prev_year_data, ['영업이익', '영업이익(손실)'])
                    if prev_operating_profit and prev_operating_profit > 0 and operating_profit:
                        financial_data['operating_profit_growth'] = ((operating_profit - prev_operating_profit) / prev_operating_profit * 100)
                
                financial_data['source'] = f'dart-fss_{latest_year}'
                financial_data['last_update'] = datetime.datetime.now().strftime('%Y-%m-%d')
                
                return financial_data
                
            except Exception as e:
                return None
                
        except Exception as e:
            return None
    
    def _safe_get_value(self, data, column_names):
        """안전하게 데이터에서 값을 가져오기"""
        for col_name in column_names:
            if col_name in data.index:
                value = data[col_name]
                if pd.notna(value) and value != 0:
                    return float(value)
        return None
    
    def _get_from_financial_cache(self, code):
        """캐시에서 재무 데이터 가져오기"""
        try:
            cache_file = os.path.join(self.financial_cache_dir, f'{code}_financial.json')
            
            if os.path.exists(cache_file):
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # 캐시 유효성 검사 (7일)
                if 'cached_at' in data:
                    cached_time = datetime.datetime.fromisoformat(data['cached_at'])
                    if datetime.datetime.now() - cached_time < datetime.timedelta(days=7):
                        return data
            
            return None
            
        except Exception as e:
            return None 