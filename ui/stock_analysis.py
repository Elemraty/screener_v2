import streamlit as st
import pandas as pd
import numpy as np
import datetime
import json
import os
import glob

from utils.stock_data import StockDataCollector
from ui.chart_components import ChartComponents

class StockAnalysis:
    def __init__(self):
        """종목 분석 컴포넌트 초기화"""
        self.chart_components = ChartComponents()
        self.detailed_results = None
        self._load_detailed_results()
    
    def _load_detailed_results(self):
        """data/results 폴더에서 가장 최신 상세 결과 파일을 로드"""
        try:
            results_dir = os.path.join('data', 'results')
            if not os.path.exists(results_dir):
                return
            
            # 가장 최신 screening_details 파일 찾기
            pattern = os.path.join(results_dir, 'screening_details_*.json')
            detail_files = glob.glob(pattern)
            
            if detail_files:
                # 파일명에서 날짜/시간 기준으로 정렬하여 가장 최신 파일 선택
                latest_file = sorted(detail_files)[-1]
                
                with open(latest_file, 'r', encoding='utf-8') as f:
                    self.detailed_results = json.load(f)
                
                print(f"✅ 상세 결과 로드 완료: {os.path.basename(latest_file)} ({len(self.detailed_results)}개 종목)")
            
        except Exception as e:
            print(f"⚠️ 상세 결과 로드 실패: {str(e)}")
            self.detailed_results = None
    
    def _get_stock_details(self, stock_code):
        """종목 코드로 상세 데이터 찾기"""
        if not self.detailed_results:
            return None
        
        # 종목 코드를 6자리로 맞춤
        formatted_code = str(stock_code).zfill(6)
        
        for result in self.detailed_results:
            if result.get('code') == formatted_code:
                return result
        
        return None
    
    def display_detailed_score_analysis(self, stock_info, details=None):
        """상세한 점수 분석 표시"""
        st.subheader("📊 상세 점수 분석")
        
        # stock_info에서 종목 코드 추출
        stock_code = None
        if hasattr(stock_info, 'get'):
            stock_code = stock_info.get('code') or stock_info.get('Code')
        elif isinstance(stock_info, dict):
            stock_code = stock_info.get('code') or stock_info.get('Code')
        
        # 상세 데이터 가져오기 (기존 details보다 파일에서 로드한 데이터 우선)
        detailed_data = self._get_stock_details(stock_code) if stock_code else None
        if detailed_data:
            details = detailed_data.get('details', {})
            stock_info = detailed_data  # 최신 데이터로 업데이트
        
        # 점수 항목별 상세 분석
        score_tabs = st.tabs(["📈 추세 점수", "🔄 패턴 점수", "💪 상대강도", "💰 펀더멘털", "🔄 수급 점수"])
        
        # 안전한 데이터 접근을 위한 헬퍼 함수
        def safe_get(data, key, default=0):
            if hasattr(data, 'get'):
                return data.get(key, default)
            elif isinstance(data, dict):
                return data.get(key, default)
            else:
                return default
        
        with score_tabs[0]:
            st.write("**📈 추세 점수 (25점 만점)**")
            trend_details = details.get('trend', {}) if isinstance(details, dict) else {}
            trend_score = safe_get(stock_info, 'trend_score', 0)
            
            col1, col2 = st.columns([1, 2])
            with col1:
                st.metric("추세 점수", f"{trend_score:.1f}/25")
            with col2:
                st.write("**점수 구성:**")
                st.write(f"• 단기 이평선 정렬: {trend_details.get('ma_alignment_score', 0):.1f}/6")
                st.write(f"• 중기 이평선 정렬: {trend_details.get('ma_trend_score', 0):.1f}/6") 
                st.write(f"• 장기 추세: {trend_details.get('long_trend_score', 0):.1f}/6")
                st.write(f"• 거래량 확인: {trend_details.get('volume_score', 0):.1f}/7")
            
            st.write("**분석 근거:**")
            if trend_score >= 20:
                st.success("🟢 강한 상승 추세 - 모든 이평선이 정배열되어 있고 거래량이 뒷받침되고 있습니다.")
            elif trend_score >= 15:
                st.warning("🟡 보통 추세 - 일부 이평선이 정배열되어 있으나 완전하지 않습니다.")
            else:
                st.error("🔴 약한 추세 - 이평선 정배열이 불완전하거나 하락 추세입니다.")
        
        with score_tabs[1]:
            st.write("**🔄 패턴 점수 (20점 만점)**")
            pattern_details = details.get('pattern', {}) if isinstance(details, dict) else {}
            pattern_score = safe_get(stock_info, 'pattern_score', 0)
            
            col1, col2 = st.columns([1, 2])
            with col1:
                st.metric("패턴 점수", f"{pattern_score:.1f}/20")
            with col2:
                st.write("**패턴 구성:**")
                vcp_detected = safe_get(stock_info, 'vcp', False)
                pivot_detected = safe_get(stock_info, 'pivot', False)
                breakout_detected = safe_get(stock_info, 'breakout', False)
                
                st.write(f"• VCP 패턴: {'✅ 감지됨 (8점)' if vcp_detected else '❌ 없음 (0점)'}")
                st.write(f"• 포켓 피벗: {'✅ 감지됨 (6점)' if pivot_detected else '❌ 없음 (0점)'}")
                st.write(f"• 돌파 패턴: {'✅ 감지됨 (6점)' if breakout_detected else '❌ 없음 (0점)'}")
            
            st.write("**분석 근거:**")
            if pattern_score >= 15:
                st.success("🟢 강력한 패턴 - 여러 긍정적 패턴이 동시에 나타나고 있습니다.")
            elif pattern_score >= 8:
                st.warning("🟡 보통 패턴 - 일부 긍정적 패턴이 나타나고 있습니다.")
            else:
                st.info("🔵 패턴 대기 - 명확한 패턴이 형성되지 않았습니다.")
        
        with score_tabs[2]:
            st.write("**💪 상대강도 점수 (25점 만점)**")
            rs_details = details.get('rs', {}) if isinstance(details, dict) else {}
            rs_score = safe_get(stock_info, 'rs_score', 0)
            
            col1, col2 = st.columns([1, 2])
            with col1:
                st.metric("상대강도 점수", f"{rs_score:.1f}/25")
            with col2:
                st.write("**상대강도 구성:**")
                st.write(f"• 13주 상대수익률: {rs_details.get('rs_13w_score', 0):.1f}/12")
                st.write(f"• 26주 상대수익률: {rs_details.get('rs_26w_score', 0):.1f}/13")
            
            st.write("**분석 근거:**")
            if rs_score >= 20:
                st.success("🟢 시장 대비 강한 성과 - 시장을 크게 앞서고 있습니다.")
            elif rs_score >= 12:
                st.warning("🟡 시장 대비 보통 성과 - 시장과 비슷하거나 약간 앞서고 있습니다.")
            else:
                st.error("🔴 시장 대비 약한 성과 - 시장을 하회하고 있습니다.")
        
        with score_tabs[3]:
            st.write("**💰 펀더멘털 점수 (30점 만점)**")
            fundamental_details = details.get('fundamental', {}) if isinstance(details, dict) else {}
            fundamental_score = safe_get(stock_info, 'fundamental_score', 0)
            
            col1, col2 = st.columns([1, 2])
            with col1:
                st.metric("펀더멘털 점수", f"{fundamental_score:.1f}/30")
            with col2:
                st.write("**펀더멘털 구성:**")
                # 실제 데이터 구조에 맞게 수정
                profitability = fundamental_details.get('profitability', {})
                growth = fundamental_details.get('growth', {})
                stability = fundamental_details.get('stability', {})
                scores_detail = fundamental_details.get('scores_detail', {})
                
                # 실제 점수 값 사용 (scores_detail에서)
                roe_score = scores_detail.get('roe', 0) if scores_detail else profitability.get('roe', 0)
                operating_margin_score = scores_detail.get('operating_margin', 0) if scores_detail else profitability.get('operating_margin', 0)
                revenue_growth_score = scores_detail.get('revenue_growth', 0) if scores_detail else growth.get('revenue_growth', 0)
                op_growth_score = scores_detail.get('operating_profit_growth', 0) if scores_detail else growth.get('operating_profit_growth', 0)
                debt_ratio_score = scores_detail.get('debt_ratio', 0) if scores_detail else stability.get('debt_ratio', 0)
                
                st.write(f"• 수익성 (ROE): {roe_score:.1f}")
                st.write(f"• 수익성 (영업이익률): {operating_margin_score:.1f}")
                st.write(f"• 성장성 (매출): {revenue_growth_score:.1f}")
                st.write(f"• 성장성 (영업이익): {op_growth_score:.1f}")
                st.write(f"• 안정성 (부채비율): {debt_ratio_score:.1f}")
            
            # 추가 재무 정보 표시
            if fundamental_details:
                st.write("**재무 지표:**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    revenue_yoy = growth.get('revenue_yoy', 0) if growth else 0
                    st.write(f"매출 YoY: {revenue_yoy:.1f}%")
                with col2:
                    op_yoy = growth.get('operating_profit_yoy', 0) if growth else 0
                    st.write(f"영업이익 YoY: {op_yoy:.1f}%")
                with col3:
                    data_source = fundamental_details.get('data_source', '알 수 없음')
                    st.write(f"데이터 출처: {data_source}")
                
                # 추가 재무 정보 더 상세히 표시
                col1, col2, col3 = st.columns(3)
                with col1:
                    roe_actual = profitability.get('roe', 0) if profitability else 0
                    st.write(f"실제 ROE: {roe_actual:.1f}%")
                with col2:
                    op_margin_actual = profitability.get('operating_margin', 0) if profitability else 0
                    st.write(f"실제 영업이익률: {op_margin_actual:.1f}%")
                with col3:
                    net_yoy = growth.get('net_income_yoy', 0) if growth else 0
                    st.write(f"순이익 YoY: {net_yoy:.1f}%")
            
            st.write("**분석 근거:**")
            if fundamental_score >= 24:
                st.success("🟢 우수한 펀더멘털 - 수익성, 성장성, 안정성이 모두 양호합니다.")
            elif fundamental_score >= 18:
                st.warning("🟡 보통 펀더멘털 - 일부 지표가 양호하나 개선이 필요한 부분이 있습니다.")
            else:
                st.error("🔴 약한 펀더멘털 - 재무 지표 개선이 필요합니다.")
        
        with score_tabs[4]:
            st.write("**🔄 수급 점수 (12점 만점)**")
            investor_details = details.get('investor', {}) if isinstance(details, dict) else {}
            investor_score = safe_get(stock_info, 'investor_score', 0)
            
            col1, col2 = st.columns([1, 2])
            with col1:
                st.metric("수급 점수", f"{investor_score:.1f}/12")
            with col2:
                st.write("**수급 구성:**")
                # 실제 데이터 구조에 맞게 수정
                foreign_net = investor_details.get('foreign_net_buy', 0)
                foreign_ratio = investor_details.get('foreign_ratio', 0)
                institution_net = investor_details.get('institution_net_buy', 0)
                institution_ratio = investor_details.get('institution_ratio', 0)
                net_buy_days = investor_details.get('net_buy_days', 0)
                short_ratio = investor_details.get('short_selling_ratio', 0)
                data_source = investor_details.get('data_source', '알 수 없음')
                
                # 점수 계산 로직 (ScoreCalculator와 동일)
                foreign_score = 0
                if foreign_net > 0:
                    if foreign_ratio >= 5:
                        foreign_score = 4
                    elif foreign_ratio >= 2:
                        foreign_score = 3
                    elif foreign_ratio >= 1:
                        foreign_score = 2
                    elif foreign_ratio > 0:
                        foreign_score = 1
                
                institution_score = 0
                if institution_net > 0:
                    if institution_ratio >= 3:
                        institution_score = 4
                    elif institution_ratio >= 1.5:
                        institution_score = 3
                    elif institution_ratio >= 0.5:
                        institution_score = 2
                    elif institution_ratio > 0:
                        institution_score = 1
                
                consecutive_score = 0
                if net_buy_days >= 10:
                    consecutive_score = 2
                elif net_buy_days >= 5:
                    consecutive_score = 1
                
                short_score = 0
                if short_ratio <= 1:
                    short_score = 2
                elif short_ratio <= 3:
                    short_score = 1.5
                elif short_ratio <= 5:
                    short_score = 1
                elif short_ratio <= 10:
                    short_score = 0.5
                
                st.write(f"• 외국인 순매수: {foreign_score:.1f}/4")
                st.write(f"• 기관 순매수: {institution_score:.1f}/4")
                st.write(f"• 연속 순매수일: {consecutive_score:.1f}/2")
                st.write(f"• 공매도 비율: {short_score:.1f}/2")
            
            # 추가 수급 정보 표시
            if investor_details:
                st.write("**수급 상세:**")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.write(f"외국인: {foreign_net:,}주")
                    st.write(f"비율: {foreign_ratio:+.2f}%")
                with col2:
                    st.write(f"기관: {institution_net:,}주")
                    st.write(f"비율: {institution_ratio:+.2f}%")
                with col3:
                    st.write(f"공매도: {short_ratio:.2f}%")
                    st.write(f"연속일: {net_buy_days}일")
                with col4:
                    st.write(f"데이터: {data_source}")
            
            st.write("**분석 근거:**")
            if investor_score >= 9:
                st.success("🟢 우수한 수급 - 외국인과 기관이 지속적으로 순매수하고 있습니다.")
            elif investor_score >= 6:
                st.warning("🟡 보통 수급 - 일부 투자자가 순매수하고 있습니다.")
            else:
                st.error("🔴 약한 수급 - 주요 투자자들이 순매도하고 있습니다.")
        
        # 종합 점수 분석
        st.subheader("🎯 종합 점수 분석")
        total_score = safe_get(stock_info, 'total_score', 0)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("총점", f"{total_score:.1f}/100")
        with col2:
            if total_score >= 80:
                grade = "A+"
                color = "🟢"
            elif total_score >= 70:
                grade = "A"
                color = "🟢"
            elif total_score >= 60:
                grade = "B+"
                color = "🟡"
            elif total_score >= 50:
                grade = "B"
                color = "🟡"
            else:
                grade = "C"
                color = "🔴"
            st.metric("등급", f"{color} {grade}")
        with col3:
            percentile = min(100, max(0, total_score))
            st.metric("백분위", f"{percentile:.0f}%")
        
        # 투자 의견
        st.write("**💡 투자 의견:**")
        if total_score >= 80:
            st.success("🚀 **매우 우수** - 모든 지표가 양호하여 적극적인 투자를 고려할 수 있습니다.")
        elif total_score >= 70:
            st.success("✅ **우수** - 대부분의 지표가 양호하여 투자를 고려할 수 있습니다.")
        elif total_score >= 60:
            st.warning("⚠️ **보통** - 일부 지표가 양호하나 신중한 검토가 필요합니다.")
        elif total_score >= 50:
            st.warning("🤔 **주의** - 여러 지표에서 개선이 필요하며 추가 분석이 필요합니다.")
        else:
            st.error("❌ **부적합** - 대부분의 지표가 부정적이며 투자를 권하지 않습니다.")
    
    def display_stock_details(self, stock_code, results_df=None, show_scores=True):
        """선택한 종목의 상세 정보 표시"""
        if not stock_code:
            return
        
        try:
            st.subheader(f"📊 {stock_code} 종목 상세 분석")
            
            # 종목 데이터 수집
            collector = StockDataCollector()
            
            # 종목 목록에서 종목 정보 찾기
            stocks_df = collector.get_all_stocks()
            
            code_column = 'code' if 'code' in stocks_df.columns else 'Code'
            name_column = 'name' if 'name' in stocks_df.columns else 'Name'
            
            # 종목 코드를 6자리로 맞춤
            formatted_stock_code = str(stock_code).zfill(6)
            stocks_df[code_column] = stocks_df[code_column].astype(str).str.zfill(6)
            
            stock_info = stocks_df[stocks_df[code_column] == formatted_stock_code]
            
            if stock_info.empty:
                # 결과 데이터프레임에서 종목 정보 찾기 시도
                if results_df is not None:
                    code_column_result = 'code' if 'code' in results_df.columns else 'Code'
                    name_column_result = 'name' if 'name' in results_df.columns else 'Name'
                    
                    result_stock_info = results_df[results_df[code_column_result] == formatted_stock_code]
                    if not result_stock_info.empty:
                        stock_name = result_stock_info[name_column_result].values[0]
                    else:
                        st.error(f"❌ 종목 코드 {formatted_stock_code}에 대한 정보를 찾을 수 없습니다.")
                        return
                else:
                    st.error(f"❌ 종목 코드 {formatted_stock_code}에 대한 정보를 찾을 수 없습니다.")
                    return
            else:
                # 종목 기본 정보
                stock_name = stock_info[name_column].values[0]
            
            # 스크리닝 결과가 있고 점수 표시가 활성화된 경우 점수 정보 표시
            if results_df is not None and show_scores:
                score_column = 'total_score' if 'total_score' in results_df.columns else 'Total_Score'
                code_column_result = 'code' if 'code' in results_df.columns else 'Code'
                
                stock_result = results_df[results_df[code_column_result] == stock_code]
                if not stock_result.empty:
                    result_row = stock_result.iloc[0]
                    
                    # 스크리닝 시점 가격 정보 표시
                    st.subheader("💰 스크리닝 시점 가격 정보")
                    col1, col2, col3, col4, col5 = st.columns(5)
                    
                    # 안전한 데이터 접근
                    if isinstance(result_row, pd.Series):
                        current_price = result_row.get('current_price', result_row.get('price', 0))
                        price_change = result_row.get('price_change', 0)
                        price_change_pct = result_row.get('price_change_pct', 0)
                        position_52w = result_row.get('position_52w', 0)
                    else:
                        # 딕셔너리나 다른 형태인 경우
                        current_price = result_row.get('current_price', 0) if hasattr(result_row, 'get') else 0
                        price_change = result_row.get('price_change', 0) if hasattr(result_row, 'get') else 0
                        price_change_pct = result_row.get('price_change_pct', 0) if hasattr(result_row, 'get') else 0
                        position_52w = result_row.get('position_52w', 0) if hasattr(result_row, 'get') else 0
                    
                    with col1:
                        st.metric("스크리닝 시점 가격", f"{current_price:,.0f}원")
                    with col2:
                        st.metric("전일 대비", f"{price_change:+,.0f}원", f"{price_change_pct:+.2f}%")
                    with col3:
                        high_52w = result_row.get('high_52w', 0) if hasattr(result_row, 'get') else 0
                        st.metric("52주 고가", f"{high_52w:,.0f}원")
                    with col4:
                        low_52w = result_row.get('low_52w', 0) if hasattr(result_row, 'get') else 0
                        st.metric("52주 저가", f"{low_52w:,.0f}원")
                    with col5:
                        st.metric("52주 위치", f"{position_52w:.1f}%")
                    
                    # 점수 정보 표시
                    st.subheader("📊 SEPA 점수")
                    col1, col2, col3, col4, col5, col6 = st.columns(6)
                    
                    # 안전한 점수 데이터 접근
                    total_score = result_row.get('total_score', 0) if hasattr(result_row, 'get') else 0
                    trend_score = result_row.get('trend_score', 0) if hasattr(result_row, 'get') else 0
                    pattern_score = result_row.get('pattern_score', 0) if hasattr(result_row, 'get') else 0
                    rs_score = result_row.get('rs_score', 0) if hasattr(result_row, 'get') else 0
                    fundamental_score = result_row.get('fundamental_score', 0) if hasattr(result_row, 'get') else 0
                    investor_score = result_row.get('investor_score', 0) if hasattr(result_row, 'get') else 0
                    
                    with col1:
                        st.metric("총점", f"{total_score:.1f}/100")
                    with col2:
                        st.metric("추세", f"{trend_score:.1f}/25")
                    with col3:
                        st.metric("패턴", f"{pattern_score:.1f}/20")
                    with col4:
                        st.metric("상대강도", f"{rs_score:.1f}/25")
                    with col5:
                        st.metric("펀더멘털", f"{fundamental_score:.1f}/30")
                    with col6:
                        st.metric("수급", f"{investor_score:.1f}/12")
                    
                    # 패턴 정보 표시
                    pattern_info = []
                    vcp = result_row.get('vcp', False) if hasattr(result_row, 'get') else False
                    pivot = result_row.get('pivot', False) if hasattr(result_row, 'get') else False
                    breakout = result_row.get('breakout', False) if hasattr(result_row, 'get') else False
                    
                    if vcp:
                        pattern_info.append("🔹 VCP")
                    if pivot:
                        pattern_info.append("🔸 Pocket Pivot")
                    if breakout:
                        pattern_info.append("🔺 Breakout")
                    
                    if pattern_info:
                        st.info(f"🎯 감지된 패턴: {', '.join(pattern_info)}")
                    
                    st.markdown("---")
            
            # 주가 데이터 가져오기 (캐시 사용하지 않음)
            with st.spinner(f"📈 {stock_name}({formatted_stock_code}) 주가 데이터를 가져오는 중..."):
                stock_data = collector.get_stock_price(formatted_stock_code, period='1y', use_cache=False)
            
            if stock_data.empty:
                st.error(f"❌ 종목 {stock_name}({formatted_stock_code})의 주가 데이터를 가져올 수 없습니다.")
                return
            
            # 기술적 지표 계산
            with st.spinner("📊 기술적 지표를 계산하는 중..."):
                stock_data = collector.calculate_indicators(stock_data)
            
            # 실시간 주가 정보
            st.subheader("📈 실시간 주가 정보")
            
            # 실시간 가격 데이터 가져오기 (자동)
            with st.spinner("📊 최신 가격 정보를 가져오는 중..."):
                realtime_data = collector.get_realtime_price(formatted_stock_code)
            
            if realtime_data:
                # 실시간 데이터 사용
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    st.metric(
                        "실시간 가격", 
                        f"{realtime_data['current_price']:,.0f}원",
                        f"{realtime_data['price_change']:+,.0f}원 ({realtime_data['price_change_pct']:+.2f}%)"
                    )
                with col2:
                    st.metric("거래량", f"{realtime_data['volume']:,.0f}주")
                with col3:
                    st.metric("고가", f"{realtime_data['high']:,.0f}원")
                with col4:
                    st.metric("저가", f"{realtime_data['low']:,.0f}원")
                with col5:
                    st.metric("시가", f"{realtime_data['open']:,.0f}원")
                
                # 업데이트 시간 표시
                update_time = realtime_data['last_update']
                if 'note' in realtime_data:
                    st.info(f"⚠️ {realtime_data['note']} | 업데이트: {update_time}")
                else:
                    # 실시간 데이터의 신선도 확인
                    try:
                        update_dt = datetime.datetime.strptime(update_time, '%Y-%m-%d %H:%M:%S')
                        time_diff = datetime.datetime.now() - update_dt
                        minutes_ago = int(time_diff.total_seconds() / 60)
                        
                        if minutes_ago < 5:
                            st.success(f"✅ 실시간 데이터 | 업데이트: {update_time} ({minutes_ago}분 전)")
                        elif minutes_ago < 30:
                            st.warning(f"⚠️ 최근 데이터 | 업데이트: {update_time} ({minutes_ago}분 전)")
                        else:
                            st.error(f"❌ 오래된 데이터 | 업데이트: {update_time} ({minutes_ago}분 전)")
                    except:
                        st.info(f"📊 데이터 | 업데이트: {update_time}")
            else:
                # 실시간 데이터 실패 시 기존 데이터 사용
                st.warning("⚠️ 실시간 데이터를 가져올 수 없어 차트 데이터를 사용합니다.")
                latest_data = stock_data.iloc[-1]
                prev_data = stock_data.iloc[-2] if len(stock_data) > 1 else latest_data
                
                price_change = latest_data['Close'] - prev_data['Close']
                price_change_pct = (price_change / prev_data['Close']) * 100
                
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    st.metric(
                        "차트 기준 가격", 
                        f"{latest_data['Close']:,.0f}원",
                        f"{price_change:+,.0f}원 ({price_change_pct:+.2f}%)"
                    )
                with col2:
                    st.metric("거래량", f"{latest_data['Volume']:,.0f}주")
                with col3:
                    if '52W_High' in latest_data:
                        st.metric("52주 고가", f"{latest_data['52W_High']:,.0f}원")
                with col4:
                    if '52W_Low' in latest_data:
                        st.metric("52주 저가", f"{latest_data['52W_Low']:,.0f}원")
                with col5:
                    # 52주 고저가 대비 현재 위치
                    if '52W_High' in latest_data and '52W_Low' in latest_data:
                        high_52w = latest_data['52W_High']
                        low_52w = latest_data['52W_Low']
                        if high_52w > low_52w:
                            current_position = ((latest_data['Close'] - low_52w) / (high_52w - low_52w)) * 100
                            st.metric("52주 위치", f"{current_position:.1f}%")
                        else:
                            st.metric("52주 위치", "50.0%")
            
            # 차트 그리기
            fig = self.chart_components.plot_stock_chart(stock_data, formatted_stock_code, stock_name)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.error("❌ 차트를 그릴 수 없습니다.")
            
            # 기술적 지표 상세 정보 (차트 데이터 기준)
            latest_data = stock_data.iloc[-1]  # 차트 데이터에서 최신 데이터 가져오기
            
            with st.expander("📊 기술적 지표 상세 정보"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**이동평균선**")
                    if 'MA20' in latest_data:
                        st.write(f"MA20: {latest_data['MA20']:,.0f}원")
                    if 'MA60' in latest_data:
                        st.write(f"MA60: {latest_data['MA60']:,.0f}원")
                    if 'MA120' in latest_data:
                        st.write(f"MA120: {latest_data['MA120']:,.0f}원")
                    
                    st.write("**볼린저 밴드**")
                    if 'Upper_Band' in latest_data:
                        st.write(f"상단: {latest_data['Upper_Band']:,.0f}원")
                    if 'Lower_Band' in latest_data:
                        st.write(f"하단: {latest_data['Lower_Band']:,.0f}원")
                
                with col2:
                    st.write("**모멘텀 지표**")
                    if 'RSI' in latest_data:
                        st.write(f"RSI: {latest_data['RSI']:.2f}")
                    if 'MACD' in latest_data:
                        st.write(f"MACD: {latest_data['MACD']:.4f}")
                    if 'MACD_Signal' in latest_data:
                        st.write(f"MACD Signal: {latest_data['MACD_Signal']:.4f}")
                    
                    st.write("**수익률**")
                    if 'Return_13W' in latest_data:
                        st.write(f"13주 수익률: {latest_data['Return_13W']*100:.2f}%")
                    if 'Return_26W' in latest_data:
                        st.write(f"26주 수익률: {latest_data['Return_26W']*100:.2f}%")
            
            # 상세한 점수 분석 (스크리닝 결과가 있는 경우)
            if results_df is not None and show_scores:
                score_column = 'total_score' if 'total_score' in results_df.columns else 'Total_Score'
                code_column_result = 'code' if 'code' in results_df.columns else 'Code'
                
                stock_result = results_df[results_df[code_column_result] == stock_code]
                if not stock_result.empty:
                    result_row = stock_result.iloc[0]
                    # 안전한 details 접근
                    details = result_row.get('details', {}) if hasattr(result_row, 'get') else {}
                    self.display_detailed_score_analysis(result_row, details)
            
        except Exception as e:
            st.error(f"❌ 종목 상세 정보 표시 중 오류가 발생했습니다: {str(e)}") 