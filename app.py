import streamlit as st

# UI 컴포넌트 임포트
from ui.chart_components import ChartComponents
from ui.market_dashboard import MarketDashboard
from ui.screening_interface import ScreeningInterface
from ui.stock_analysis import StockAnalysis

# DART API 키 설정
DART_API_KEY = "1c0d58c20bca902c71666c62da81bd5991c14ec1"

# 페이지 설정
st.set_page_config(
    page_title="SEPA 전종목 스크리너 v2.0", 
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    """메인 함수"""
    st.title("📈 SEPA 전종목 스크리너 v2.0")
    st.markdown("**S**tock **E**valuation **P**attern **A**nalyzer - 주식 종목 분석 및 스크리닝 도구")
    
    # 세션 상태 초기화
    if 'dart_api_key' not in st.session_state:
        st.session_state.dart_api_key = DART_API_KEY
    if 'results' not in st.session_state:
        st.session_state.results = None
    
    # UI 컴포넌트 초기화
    chart_components = ChartComponents()
    market_dashboard = MarketDashboard()
    screening_interface = ScreeningInterface()
    stock_analysis = StockAnalysis()
    
    # 메인 탭 생성
    main_tab1, main_tab2 = st.tabs(["🔍 종목 스크리닝", "📊 시장 정보"])
    
    with main_tab1:
        screening_interface.render()
    
    with main_tab2:
        market_dashboard.render()

if __name__ == "__main__":
    main() 