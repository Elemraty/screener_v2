import streamlit as st
import pandas as pd
import numpy as np
import datetime
import os
import time

from utils.screener import SEPAScreener
from utils.stock_data import StockDataCollector
from ui.chart_components import ChartComponents
from ui.stock_analysis import StockAnalysis

class ScreeningInterface:
    def __init__(self):
        """스크리닝 인터페이스 초기화"""
        self.chart_components = ChartComponents()
        self.stock_analysis = StockAnalysis()
    
    def load_existing_results(self):
        """저장된 결과 파일 로드"""
        results_dir = os.path.join('data', 'results')
        if not os.path.exists(results_dir):
            return []
        
        result_files = [f for f in os.listdir(results_dir) if f.startswith('screening_results_') and f.endswith('.csv')]
        result_files.sort(reverse=True)  # 최신 파일 우선
        
        return result_files
    
    def save_results(self, results_df):
        """결과를 파일로 저장"""
        try:
            results_dir = os.path.join('data', 'results')
            os.makedirs(results_dir, exist_ok=True)
            
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            results_path = os.path.join(results_dir, f'screening_results_{timestamp}.csv')
            
            results_df.to_csv(results_path, index=False, encoding='utf-8-sig')
            st.success(f"✅ {len(results_df)}개 종목의 스크리닝 결과가 저장되었습니다.")
            
        except Exception as e:
            st.error(f"❌ 결과 저장 중 오류가 발생했습니다: {str(e)}")
    
    def display_results(self, results_df, score_threshold=50):
        """스크리닝 결과를 표시"""
        try:
            # 결과 데이터프레임의 열 이름 확인
            score_column = 'total_score' if 'total_score' in results_df.columns else 'Total_Score'
            code_column = 'code' if 'code' in results_df.columns else 'Code'
            name_column = 'name' if 'name' in results_df.columns else 'Name'
            
            # 점수 임계값으로 필터링
            filtered_df = results_df[results_df[score_column] >= score_threshold].copy()
            
            if filtered_df.empty:
                st.warning(f"⚠️ 점수가 {score_threshold} 이상인 종목이 없습니다.")
                return
        
            # 점수 내림차순 정렬
            filtered_df = filtered_df.sort_values(score_column, ascending=False)
            
            # 결과 카운트 표시
            st.subheader(f"🎯 스크리닝 결과: 총 {len(filtered_df)}개 종목 (점수 {score_threshold} 이상)")
            
            # 상위 종목 하이라이트 (가격 정보 포함)
            if len(filtered_df) > 0:
                top_stock = filtered_df.iloc[0]
                
                # 가격 정보 표시
                current_price = top_stock.get('current_price', top_stock.get('price', 0))
                price_change = top_stock.get('price_change', 0)
                price_change_pct = top_stock.get('price_change_pct', 0)
                
                # 가격 변화에 따른 색상 결정
                if price_change > 0:
                    price_color = "🔴"
                    change_text = f"+{price_change:,.0f}원 (+{price_change_pct:.2f}%)"
                elif price_change < 0:
                    price_color = "🔵"
                    change_text = f"{price_change:,.0f}원 ({price_change_pct:.2f}%)"
                else:
                    price_color = "⚪"
                    change_text = "0원 (0.00%)"
                
                st.success(f"🏆 최고 점수: {top_stock[score_column]}점 - {top_stock[name_column]}({top_stock[code_column]}) | "
                          f"{price_color} {current_price:,.0f}원 {change_text}")
            
            # 점수 분포 차트
            if len(filtered_df) >= 5:
                col1, col2 = st.columns(2)
            
                with col1:
                    # 점수 히스토그램
                    fig_hist = self.chart_components.plot_score_distribution(filtered_df, score_column)
                    if fig_hist:
                        st.plotly_chart(fig_hist, use_container_width=True)
            
                with col2:
                    # 상위 10개 종목 바차트
                    fig_bar = self.chart_components.plot_top_stocks_bar(filtered_df, score_column, name_column, code_column, 10)
                    if fig_bar:
                        st.plotly_chart(fig_bar, use_container_width=True)
            
            # 결과 데이터프레임 표시 (가격 정보 포함)
            # 표시할 컬럼 순서 정리
            display_columns = []
            
            # 기본 정보
            if code_column in filtered_df.columns:
                display_columns.append(code_column)
            if name_column in filtered_df.columns:
                display_columns.append(name_column)
            if 'market' in filtered_df.columns:
                display_columns.append('market')
            if 'sector' in filtered_df.columns:
                display_columns.append('sector')
            
            # 가격 정보 (새로운 컬럼들)
            price_columns = ['current_price', 'price_change', 'price_change_pct', 'volume', 'position_52w']
            for col in price_columns:
                if col in filtered_df.columns:
                    display_columns.append(col)
            
            # 점수 정보
            score_columns = ['total_score', 'trend_score', 'pattern_score', 'rs_score', 'fundamental_score', 'investor_score']
            for col in score_columns:
                if col in filtered_df.columns:
                    display_columns.append(col)
            
            # 패턴 정보
            pattern_columns = ['vcp', 'pivot', 'breakout']
            for col in pattern_columns:
                if col in filtered_df.columns:
                    display_columns.append(col)
            
            # 기타 컬럼들 추가
            for col in filtered_df.columns:
                if col not in display_columns:
                    display_columns.append(col)
            
            # 컬럼명 한글화
            column_names = {
                'code': '종목코드',
                'name': '종목명',
                'market': '시장',
                'sector': '업종',
                'current_price': '현재가',
                'price_change': '전일대비',
                'price_change_pct': '등락률(%)',
                'volume': '거래량',
                'position_52w': '52주위치(%)',
                'total_score': '총점',
                'trend_score': '추세',
                'pattern_score': '패턴',
                'rs_score': '상대강도',
                'fundamental_score': '펀더멘털',
                'investor_score': '수급',
                'vcp': 'VCP',
                'pivot': 'Pivot',
                'breakout': 'Breakout'
            }
            
            # 표시용 데이터프레임 생성
            display_df = filtered_df[display_columns].copy()
            display_df = display_df.rename(columns=column_names)
            
            st.dataframe(
                display_df,
                use_container_width=True,
                height=400
            )
            
            # 종목 코드가 있는지 확인
            if code_column not in filtered_df.columns:
                st.error(f"❌ 결과에 {code_column} 칼럼이 없습니다.")
                return
                
            # 종목 선택 및 상세 정보 표시
            selected_stock_code = st.selectbox(
                "📊 종목 선택 (상세 정보 보기)",
                options=filtered_df[code_column].tolist(),
                format_func=lambda x: f"{filtered_df[filtered_df[code_column]==x][name_column].values[0]} ({x})",
                key="stock_selection"
            )
            
            if selected_stock_code:
                st.session_state.selected_stock = selected_stock_code
                self.stock_analysis.display_stock_details(selected_stock_code, filtered_df, show_scores=True)
                
        except Exception as e:
            st.error(f"❌ 결과 표시 중 오류가 발생했습니다: {str(e)}")
    
    def search_and_analyze_stock(self, search_input):
        """종목 검색 및 상세 분석"""
        try:
            # 종목 데이터 수집기 초기화
            collector = StockDataCollector()
            
            # 전체 종목 목록 가져오기
            stocks_df = collector.get_all_stocks()
            
            # 검색어가 숫자인지 확인 (종목 코드)
            if search_input.isdigit():
                # 종목 코드로 검색
                formatted_code = search_input.zfill(6)
                stock_info = stocks_df[stocks_df['Code'].astype(str).str.zfill(6) == formatted_code]
            else:
                # 종목명으로 검색 (부분 일치)
                stock_info = stocks_df[stocks_df['Name'].str.contains(search_input, case=False, na=False)]
            
            if stock_info.empty:
                st.error(f"❌ '{search_input}'에 해당하는 종목을 찾을 수 없습니다.")
                return
            
            # 여러 종목이 검색된 경우 선택하도록 함
            if len(stock_info) > 1:
                st.subheader(f"🔍 '{search_input}' 검색 결과")
                selected_idx = st.selectbox(
                    "종목 선택",
                    range(len(stock_info)),
                    format_func=lambda x: f"{stock_info.iloc[x]['Name']} ({stock_info.iloc[x]['Code']})"
                )
                selected_stock = stock_info.iloc[selected_idx]
            else:
                selected_stock = stock_info.iloc[0]
            
            stock_code = selected_stock['Code']
            stock_name = selected_stock['Name']
            
            st.subheader(f"📊 {stock_name} ({stock_code}) 상세 분석")
            
            # 개별 종목 분석 실행
            with st.spinner(f"📈 {stock_name} 종목을 분석하는 중..."):
                # 스크리너 초기화
                screener = SEPAScreener(
                    dart_api_key=st.session_state.get('dart_api_key', ''),
                    max_workers=1
                )
                
                # 시장 데이터 가져오기
                market_data = screener.get_market_data()
                market = selected_stock.get('Market', 'KOSPI')
                
                # 개별 종목 분석
                sector = selected_stock.get('Sector', '기타')
                result = screener.process_single_stock(
                    stock_code, stock_name, market, market_data, sector
                )
                
                if result:
                    # 상세 차트 및 정보 표시 (점수 정보 포함)
                    self.stock_analysis.display_stock_details(stock_code, pd.DataFrame([result]))
                else:
                    st.error(f"❌ {stock_name} 종목 분석에 실패했습니다.")
                    
        except Exception as e:
            st.error(f"❌ 종목 검색 중 오류가 발생했습니다: {str(e)}")
    
    def render(self):
        """스크리닝 인터페이스 렌더링"""
        # 스크리닝 설정
        st.header("🔍 SEPA 종목 스크리닝")
        
        # 메인 영역을 두 개의 컬럼으로 나누기
        col_main, col_sidebar = st.columns([3, 1])
        
        with col_sidebar:
            st.subheader("⚙️ 스크리닝 설정")
            
            # 시장 선택
            markets = st.multiselect(
                "📊 시장 선택",
                options=['KOSPI', 'KOSDAQ'],
                default=['KOSPI', 'KOSDAQ'],
                help="분석할 시장을 선택하세요"
            )
            
            # 최소 점수 설정
            min_score = st.slider(
                "📈 최소 총점",
                min_value=0,
                max_value=100,
                value=50,
                step=5,
                help="표시할 최소 SEPA 점수를 설정하세요"
            )
            
            # 전종목 스크리닝 실행 버튼
            run_full_screening = st.button(
                "🚀 전종목 스크리닝 실행",
                type="primary",
                use_container_width=True,
                help="설정된 조건으로 전종목 스크리닝을 실행합니다"
            )
            
            st.markdown("---")
            
            # 개별 종목 검색 및 분석 (스크리닝 설정 하단에 배치)
            st.subheader("🔍 개별 종목 분석")
            
            search_input = st.text_input(
                "종목 코드 또는 종목명",
                placeholder="예: 005930, 삼성전자",
                help="종목 코드(6자리) 또는 종목명을 입력하세요",
                key="individual_stock_search"
            )
            
            # 개별 종목 분석 버튼
            analyze_individual = st.button(
                "📊 개별 종목 분석",
                use_container_width=True,
                disabled=not search_input,
                help="선택한 종목의 상세 분석을 실행합니다"
            )
        
        with col_main:
            # 전종목 스크리닝 실행
            if run_full_screening:
                if not markets:
                    st.error("❌ 최소 하나의 시장을 선택해주세요.")
                else:
                    with st.spinner("🔄 SEPA 전종목 스크리닝을 실행하는 중... (시간이 오래 걸릴 수 있습니다)"):
                        try:
                            # 스크리너 초기화
                            screener = SEPAScreener(
                                dart_api_key=st.session_state.dart_api_key,
                                max_workers=2
                            )
                            
                            # 전종목 스크리닝 실행
                            results_df = screener.run_screening(
                                markets=markets,
                                total_score_threshold=0  # 모든 결과 가져온 후 필터링
                            )
                            
                            if not results_df.empty:
                                st.session_state.results = results_df
                                st.success(f"✅ 전종목 스크리닝 완료! {len(results_df)}개 종목이 분석되었습니다.")
                                
                                # 결과 저장
                                self.save_results(results_df)
                                
                            else:
                                st.warning("⚠️ 조건을 만족하는 종목이 없습니다.")
                                
                        except Exception as e:
                            st.error(f"❌ 전종목 스크리닝 중 오류가 발생했습니다: {str(e)}")
            
            # 개별 종목 분석 실행
            if analyze_individual and search_input:
                with st.spinner(f"📈 '{search_input}' 종목을 분석하는 중..."):
                    self.search_and_analyze_stock(search_input)
            
            # 기존 결과 로드 섹션
            st.markdown("---")
            st.subheader("📂 기존 결과 불러오기")
            
            existing_files = self.load_existing_results()
            if existing_files:
                col1, col2 = st.columns(2)
                
                with col1:
                    selected_file = st.selectbox(
                        "저장된 결과 파일 선택",
                        options=existing_files,
                        format_func=lambda x: f"{x.replace('screening_results_', '').replace('.csv', '')} 결과"
                    )
                
                with col2:
                    load_results = st.button("📥 결과 불러오기")
                
                if st.session_state.results is not None:
                    st.info(f"현재 로드된 결과: {len(st.session_state.results)}개 종목")
                
                if load_results and selected_file:
                    try:
                        results_path = os.path.join('data', 'results', selected_file)
                        loaded_df = pd.read_csv(results_path, encoding='utf-8-sig')
                        st.session_state.results = loaded_df
                        st.success(f"✅ {len(loaded_df)}개 종목의 결과를 불러왔습니다.")
                    except Exception as e:
                        st.error(f"❌ 결과 로드 중 오류: {str(e)}")
            else:
                st.info("💡 저장된 스크리닝 결과가 없습니다. 새로운 스크리닝을 실행해주세요.")
            
            # 결과 표시
            if st.session_state.results is not None:
                st.markdown("---")
                self.display_results(st.session_state.results, min_score)