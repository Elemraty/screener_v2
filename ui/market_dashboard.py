import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from utils.stock_data import StockDataCollector

class MarketDashboard:
    def __init__(self):
        """시장 대시보드 컴포넌트 초기화"""
        self.collector = StockDataCollector()
    
    def display_investor_trends(self, market_trends):
        """투자자별 매매 동향 표시"""
        if not market_trends:
            st.warning("⚠️ 투자자 동향 데이터를 가져올 수 없습니다.")
            st.info("💡 네트워크 연결을 확인하거나 잠시 후 다시 시도해주세요.")
            return
        
        kospi_data = market_trends.get('kospi_investor', pd.DataFrame())
        kosdaq_data = market_trends.get('kosdaq_investor', pd.DataFrame())
        period = market_trends.get('period', '1w')
        
        period_text = '최근 1주일' if period == '1w' else '최근 1개월'
        st.subheader(f"📊 {period_text} 투자자별 매매 동향")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**KOSPI 투자자별 매매**")
            if not kospi_data.empty and '순매수' in kospi_data.columns:
                fig_kospi = go.Figure(data=[
                    go.Bar(
                        x=kospi_data['순매수'],
                        y=kospi_data.index,
                        orientation='h',
                        marker_color=['red' if x > 0 else 'blue' for x in kospi_data['순매수']],
                        text=[f"{x:,.0f}" for x in kospi_data['순매수']],
                        textposition='auto'
                    )
                ])
                fig_kospi.update_layout(
                    title="KOSPI 투자자별 순매수 (억원)",
                    xaxis_title="순매수 금액 (억원)",
                    height=400
                )
                st.plotly_chart(fig_kospi, use_container_width=True)
                st.dataframe(kospi_data, use_container_width=True)
            else:
                st.warning("⚠️ KOSPI 투자자 데이터를 가져올 수 없습니다.")
        
        with col2:
            st.write("**KOSDAQ 투자자별 매매**")
            if not kosdaq_data.empty and '순매수' in kosdaq_data.columns:
                fig_kosdaq = go.Figure(data=[
                    go.Bar(
                        x=kosdaq_data['순매수'],
                        y=kosdaq_data.index,
                        orientation='h',
                        marker_color=['red' if x > 0 else 'blue' for x in kosdaq_data['순매수']],
                        text=[f"{x:,.0f}" for x in kosdaq_data['순매수']],
                        textposition='auto'
                    )
                ])
                fig_kosdaq.update_layout(
                    title="KOSDAQ 투자자별 순매수 (억원)",
                    xaxis_title="순매수 금액 (억원)",
                    height=400
                )
                st.plotly_chart(fig_kosdaq, use_container_width=True)
                st.dataframe(kosdaq_data, use_container_width=True)
            else:
                st.warning("⚠️ KOSDAQ 투자자 데이터를 가져올 수 없습니다.")
    
    def display_foreign_trading(self, foreign_data):
        """외국인 매매 상위 종목 표시"""
        if not foreign_data:
            st.warning("⚠️ 외국인 매매 데이터를 가져올 수 없습니다.")
            st.info("💡 네트워크 연결을 확인하거나 잠시 후 다시 시도해주세요.")
            return
        
        buy_top = foreign_data.get('foreign_buy_top', pd.DataFrame())
        sell_top = foreign_data.get('foreign_sell_top', pd.DataFrame())
        period = foreign_data.get('period', '1w')
        
        period_text = '최근 1주일' if period == '1w' else '최근 1개월'
        st.subheader(f"🌍 {period_text} 외국인 매매 상위 종목")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**🔴 외국인 매수 상위 10개 종목**")
            if not buy_top.empty and '순매수거래량' in buy_top.columns:
                buy_display = self.collector.add_stock_names_to_dataframe(buy_top, 'index')
                
                chart_labels = []
                for idx in buy_display.index:
                    name = buy_display.loc[idx, '종목명'] if '종목명' in buy_display.columns else str(idx)
                    chart_labels.append(f"{name}({idx})")
                
                fig_buy = go.Figure(data=[
                    go.Bar(
                        x=buy_display['순매수거래량'],
                        y=chart_labels,
                        orientation='h',
                        marker_color='red',
                        text=[f"{x:,.0f}" for x in buy_display['순매수거래량']],
                        textposition='auto'
                    )
                ])
                fig_buy.update_layout(
                    title="외국인 매수 상위 종목",
                    xaxis_title="순매수 거래량 (주)",
                    height=400
                )
                st.plotly_chart(fig_buy, use_container_width=True)
                st.dataframe(buy_display, use_container_width=True)
            else:
                st.warning("⚠️ 외국인 매수 데이터를 가져올 수 없습니다.")
        
        with col2:
            st.write("**🔵 외국인 매도 상위 10개 종목**")
            if not sell_top.empty and '순매수거래량' in sell_top.columns:
                sell_display = self.collector.add_stock_names_to_dataframe(sell_top, 'index')
                sell_display['매도거래량'] = abs(sell_display['순매수거래량'])
                
                chart_labels = []
                for idx in sell_display.index:
                    name = sell_display.loc[idx, '종목명'] if '종목명' in sell_display.columns else str(idx)
                    chart_labels.append(f"{name}({idx})")
                
                fig_sell = go.Figure(data=[
                    go.Bar(
                        x=sell_display['매도거래량'],
                        y=chart_labels,
                        orientation='h',
                        marker_color='blue',
                        text=[f"{x:,.0f}" for x in sell_display['매도거래량']],
                        textposition='auto'
                    )
                ])
                fig_sell.update_layout(
                    title="외국인 매도 상위 종목",
                    xaxis_title="매도 거래량 (주)",
                    height=400
                )
                st.plotly_chart(fig_sell, use_container_width=True)
                st.dataframe(sell_display, use_container_width=True)
            else:
                st.warning("⚠️ 외국인 매도 데이터를 가져올 수 없습니다.")
    
    def display_institution_trading(self, institution_data):
        """기관 매매 상위 종목 표시"""
        if not institution_data:
            st.warning("⚠️ 기관 매매 데이터를 가져올 수 없습니다.")
            st.info("💡 네트워크 연결을 확인하거나 잠시 후 다시 시도해주세요.")
            return
        
        buy_top = institution_data.get('institution_buy_top', pd.DataFrame())
        sell_top = institution_data.get('institution_sell_top', pd.DataFrame())
        period = institution_data.get('period', '1w')
        
        period_text = '최근 1주일' if period == '1w' else '최근 1개월'
        st.subheader(f"🏢 {period_text} 기관 매매 상위 종목")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**🔴 기관 매수 상위 10개 종목**")
            if not buy_top.empty and '순매수거래량' in buy_top.columns:
                buy_display = self.collector.add_stock_names_to_dataframe(buy_top, 'index')
                
                chart_labels = []
                for idx in buy_display.index:
                    name = buy_display.loc[idx, '종목명'] if '종목명' in buy_display.columns else str(idx)
                    chart_labels.append(f"{name}({idx})")
                
                fig_buy = go.Figure(data=[
                    go.Bar(
                        x=buy_display['순매수거래량'],
                        y=chart_labels,
                        orientation='h',
                        marker_color='green',
                        text=[f"{x:,.0f}" for x in buy_display['순매수거래량']],
                        textposition='auto'
                    )
                ])
                fig_buy.update_layout(
                    title="기관 매수 상위 종목",
                    xaxis_title="순매수 거래량 (주)",
                    height=400
                )
                st.plotly_chart(fig_buy, use_container_width=True)
                st.dataframe(buy_display, use_container_width=True)
            else:
                st.warning("⚠️ 기관 매수 데이터를 가져올 수 없습니다.")
        
        with col2:
            st.write("**🔵 기관 매도 상위 10개 종목**")
            if not sell_top.empty and '순매수거래량' in sell_top.columns:
                sell_display = self.collector.add_stock_names_to_dataframe(sell_top, 'index')
                sell_display['매도거래량'] = abs(sell_display['순매수거래량'])
                
                chart_labels = []
                for idx in sell_display.index:
                    name = sell_display.loc[idx, '종목명'] if '종목명' in sell_display.columns else str(idx)
                    chart_labels.append(f"{name}({idx})")
                
                fig_sell = go.Figure(data=[
                    go.Bar(
                        x=sell_display['매도거래량'],
                        y=chart_labels,
                        orientation='h',
                        marker_color='orange',
                        text=[f"{x:,.0f}" for x in sell_display['매도거래량']],
                        textposition='auto'
                    )
                ])
                fig_sell.update_layout(
                    title="기관 매도 상위 종목",
                    xaxis_title="매도 거래량 (주)",
                    height=400
                )
                st.plotly_chart(fig_sell, use_container_width=True)
                st.dataframe(sell_display, use_container_width=True)
            else:
                st.warning("⚠️ 기관 매도 데이터를 가져올 수 없습니다.")
    
    def display_short_selling_info(self, short_data):
        """공매도 현황 표시"""
        st.subheader("📉 최근 1주일 공매도 상위 종목")
        
        if short_data.empty:
            st.warning("⚠️ 공매도 데이터를 가져올 수 없습니다.")
            return
        
        # 공매도 데이터 처리 (ticker 컬럼 기반)
        if 'short_ratio' in short_data.columns and 'short_balance' in short_data.columns:
            # 상위 15개 종목 선택
            top_15 = short_data.head(15).copy()
            
            # 차트용 라벨 생성
            chart_labels = []
            for _, row in top_15.iterrows():
                name = row.get('종목명', f"종목{row.get('ticker', '')}")
                ticker = row.get('ticker', '')
                chart_labels.append(f"{name}({ticker})")
            
            # 공매도 비중 차트
            fig_ratio = go.Figure(data=[
                go.Bar(
                    x=top_15['short_ratio'],
                    y=chart_labels,
                    orientation='h',
                    marker_color='purple',
                    text=[f"{x:.2f}%" for x in top_15['short_ratio']],
                    textposition='auto'
                )
            ])
            fig_ratio.update_layout(
                title="공매도 비중 상위 종목",
                xaxis_title="공매도 비중 (%)",
                height=500
            )
            st.plotly_chart(fig_ratio, use_container_width=True)
            
            # 공매도 잔고 차트
            fig_balance = go.Figure(data=[
                go.Bar(
                    x=top_15['short_balance'],
                    y=chart_labels,
                    orientation='h',
                    marker_color='red',
                    text=[f"{x:,.0f}" for x in top_15['short_balance']],
                    textposition='auto'
                )
            ])
            fig_balance.update_layout(
                title="공매도 잔고 상위 종목",
                xaxis_title="공매도 잔고 (주)",
                height=500
            )
            st.plotly_chart(fig_balance, use_container_width=True)
            
            # 상세 정보 테이블
            st.write("**상위 종목 상세 정보**")
            display_columns = ['종목명', 'ticker', 'short_ratio', 'short_balance', 'date']
            available_columns = [col for col in display_columns if col in top_15.columns]
            
            if available_columns:
                display_df = top_15[available_columns].copy()
                # 컬럼명 한글화
                column_names = {
                    'ticker': '종목코드',
                    'short_ratio': '공매도비중(%)',
                    'short_balance': '공매도잔고(주)',
                    'date': '기준일'
                }
                display_df = display_df.rename(columns=column_names)
                st.dataframe(display_df, use_container_width=True)
            else:
                st.dataframe(top_15, use_container_width=True)
        else:
            st.warning("⚠️ 공매도 관련 데이터 컬럼을 찾을 수 없습니다.")
            st.write("**사용 가능한 컬럼:**", short_data.columns.tolist())
            if not short_data.empty:
                st.write("**데이터 샘플:**")
                st.dataframe(short_data.head(), use_container_width=True)
    
    def display_sector_analysis_enhanced(self, results_df):
        """향상된 업종별 분석 표시"""
        if results_df.empty:
            st.warning("⚠️ 분석할 데이터가 없습니다.")
            return
        
        st.subheader("📊 업종별 SEPA 점수 분석")
        
        sector_summary = self.collector.get_sector_performance_summary(results_df)
        
        if not sector_summary.empty:
            fig_sector = go.Figure(data=[
                go.Bar(
                    x=sector_summary['평균점수'],
                    y=sector_summary.index,
                    orientation='h',
                    marker_color='lightblue',
                    text=[f"{x:.1f}" for x in sector_summary['평균점수']],
                    textposition='auto'
                )
            ])
            fig_sector.update_layout(
                title="업종별 평균 SEPA 점수",
                xaxis_title="평균 점수",
                height=400
            )
            st.plotly_chart(fig_sector, use_container_width=True)
            
            st.write("**업종별 상세 통계**")
            st.dataframe(sector_summary, use_container_width=True)
    
    def render(self):
        """시장 정보 대시보드 렌더링"""
        st.header("📊 시장 정보 대시보드")
        
        # 기간 선택
        col1, col2 = st.columns([1, 3])
        with col1:
            period = st.selectbox(
                "📅 분석 기간",
                options=['1w', '1m'],
                format_func=lambda x: '최근 1주일' if x == '1w' else '최근 1개월',
                index=0
            )
        
        # 탭 생성
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📈 투자자 동향", 
            "🌍 외국인 매매", 
            "🏢 기관 매매", 
            "📉 공매도 현황",
            "🎯 업종별 SEPA 점수"
        ])
        
        with tab1:
            with st.spinner("📊 투자자 동향 데이터를 가져오는 중..."):
                try:
                    market_trends = self.collector.get_market_investor_trends(period)
                    self.display_investor_trends(market_trends)
                except Exception as e:
                    st.error(f"❌ 투자자 동향 데이터 수집 오류: {str(e)}")
                    st.info("💡 데이터 수집에 실패했습니다. 잠시 후 다시 시도해주세요.")
        
        with tab2:
            with st.spinner("🌍 외국인 매매 데이터를 가져오는 중..."):
                try:
                    foreign_data = self.collector.get_top_foreign_trading_stocks(period, 10)
                    self.display_foreign_trading(foreign_data)
                except Exception as e:
                    st.error(f"❌ 외국인 매매 데이터 수집 오류: {str(e)}")
                    st.info("💡 데이터 수집에 실패했습니다. 잠시 후 다시 시도해주세요.")
        
        with tab3:
            with st.spinner("🏢 기관 매매 데이터를 가져오는 중..."):
                try:
                    institution_data = self.collector.get_top_institution_trading_stocks(period, 10)
                    self.display_institution_trading(institution_data)
                except Exception as e:
                    st.error(f"❌ 기관 매매 데이터 수집 오류: {str(e)}")
                    st.info("💡 데이터 수집에 실패했습니다. 잠시 후 다시 시도해주세요.")
        
        with tab4:
            with st.spinner("📉 공매도 데이터를 가져오는 중..."):
                try:
                    short_data = self.collector.get_top_short_selling_stocks(20)
                    self.display_short_selling_info(short_data)
                except Exception as e:
                    st.error(f"❌ 공매도 데이터 수집 오류: {str(e)}")
                    st.info("💡 데이터 수집에 실패했습니다. 잠시 후 다시 시도해주세요.")
        
        with tab5:
            # 업종별 SEPA 점수 분석 (스크리닝 결과가 있을 때만 표시)
            if st.session_state.get('results') is not None:
                try:
                    self.display_sector_analysis_enhanced(st.session_state.results)
                except Exception as e:
                    st.error(f"❌ 업종별 분석 오류: {str(e)}")
                    st.info("💡 업종별 분석에 실패했습니다.")
            else:
                st.info("💡 업종별 SEPA 점수를 보려면 먼저 종목 스크리닝을 실행해주세요.")
                
                # 스크리닝 실행 버튼
                if st.button("🔍 종목 스크리닝 실행하기", type="primary"):
                    # 스크리닝 탭으로 이동하도록 안내
                    st.info("👈 왼쪽의 '종목 스크리닝' 탭에서 스크리닝을 실행해주세요.") 