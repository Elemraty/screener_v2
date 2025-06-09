import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import os

class ChartComponents:
    def __init__(self):
        """차트 컴포넌트 초기화"""
        self.setup_fonts()
    
    def setup_fonts(self):
        """폰트 설정"""
        font_path = None
        if os.path.exists('/usr/share/fonts/truetype/nanum/NanumGothic.ttf'):
            font_path = '/usr/share/fonts/truetype/nanum/NanumGothic.ttf'
        elif os.path.exists('/Library/Fonts/AppleGothic.ttf'):
            font_path = '/Library/Fonts/AppleGothic.ttf'

        if font_path:
            fm.fontManager.addfont(font_path)
            plt.rcParams['font.family'] = fm.FontProperties(fname=font_path).get_name()
        else:
            plt.rcParams['font.family'] = 'Arial'
            
        plt.rcParams['axes.unicode_minus'] = False
    
    def plot_stock_chart(self, data, code, name):
        """종목 차트를 Plotly로 그립니다"""
        if data is None or data.empty:
            return None
        
        try:
            # 서브플롯 생성
            fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                               vertical_spacing=0.03, 
                               subplot_titles=(f'{name} ({code}) - 주가와 이동평균', '거래량'),
                               row_heights=[0.7, 0.3])
            
            # 캔들스틱 차트
            fig.add_trace(
                go.Candlestick(
                    x=data.index,
                    open=data['Open'], 
                    high=data['High'],
                    low=data['Low'], 
                    close=data['Close'],
                    name='주가'
                ),
                row=1, col=1
            )
            
            # 이동평균선
            if 'MA20' in data.columns:
                fig.add_trace(
                    go.Scatter(x=data.index, y=data['MA20'], name='MA20', 
                              line=dict(color='blue', width=1)),
                    row=1, col=1
                )
            
            if 'MA60' in data.columns:
                fig.add_trace(
                    go.Scatter(x=data.index, y=data['MA60'], name='MA60', 
                              line=dict(color='orange', width=1)),
                    row=1, col=1
                )
            
            if 'MA120' in data.columns:
                fig.add_trace(
                    go.Scatter(x=data.index, y=data['MA120'], name='MA120', 
                              line=dict(color='purple', width=1)),
                    row=1, col=1
                )
            
            # 볼린저 밴드
            if 'Upper_Band' in data.columns and 'Lower_Band' in data.columns:
                fig.add_trace(
                    go.Scatter(x=data.index, y=data['Upper_Band'], name='Upper Band', 
                     line=dict(color='rgba(0,0,255,0.3)', width=1), showlegend=False),
                    row=1, col=1
                )
                
                fig.add_trace(
                    go.Scatter(x=data.index, y=data['Lower_Band'], name='Lower Band',
                     line=dict(color='rgba(0,0,255,0.3)', width=1), 
                     fill='tonexty', fillcolor='rgba(0,0,255,0.05)', showlegend=False),
                    row=1, col=1
                )
            
            # 거래량 차트
            colors = ['red' if data['Close'].iloc[i] > data['Open'].iloc[i] else 'green' 
                     for i in range(len(data))]
            
            fig.add_trace(
                go.Bar(x=data.index, y=data['Volume'], name='거래량', marker_color=colors),
                row=2, col=1
            )
            
            # 거래량 이동평균
            if 'Volume_MA5' in data.columns:
                fig.add_trace(
                    go.Scatter(x=data.index, y=data['Volume_MA5'], name='거래량 MA5', 
                     line=dict(color='orange', width=1)),
                    row=2, col=1
                )
            
            if 'Volume_MA20' in data.columns:
                fig.add_trace(
                    go.Scatter(x=data.index, y=data['Volume_MA20'], name='거래량 MA20', 
                     line=dict(color='purple', width=1)),
                    row=2, col=1
                )
            
            # 레이아웃 업데이트
            fig.update_layout(
                title=f'{name} ({code}) - SEPA 분석 차트',
                height=600,
                xaxis_rangeslider_visible=False,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
                template='seaborn'
            )
            
            return fig

        except Exception as e:
            st.error(f"차트 생성 중 오류가 발생했습니다: {str(e)}")
            return None
    
    def plot_score_distribution(self, results_df, score_column='total_score'):
        """점수 분포 히스토그램"""
        if results_df.empty:
            return None
            
        fig = go.Figure(data=[go.Histogram(x=results_df[score_column], nbinsx=20)])
        fig.update_layout(
            title="점수 분포",
            xaxis_title="총점",
            yaxis_title="종목 수",
            height=300
        )
        return fig
    
    def plot_top_stocks_bar(self, results_df, score_column='total_score', name_column='name', code_column='code', top_n=10):
        """상위 종목 바차트"""
        if results_df.empty:
            return None
            
        top_stocks = results_df.head(top_n)
        fig = go.Figure(data=[
            go.Bar(
                x=top_stocks[score_column],
                y=[f"{row[name_column]}({row[code_column]})" for _, row in top_stocks.iterrows()],
                orientation='h'
            )
        ])
        fig.update_layout(
            title=f"상위 {top_n}개 종목",
            xaxis_title="총점",
            height=300
        )
        return fig
    
    def plot_sector_pie_chart(self, sector_stats):
        """업종별 파이차트"""
        if sector_stats.empty:
            return None
            
        fig = go.Figure(data=[go.Pie(
            labels=sector_stats.index,
            values=sector_stats['종목수'],
            hole=0.3
        )])
        fig.update_layout(
            title="업종별 상위 종목 분포",
            height=400
        )
        return fig
    
    def plot_sector_bar_chart(self, sector_stats):
        """업종별 평균 점수 바차트"""
        if sector_stats.empty:
            return None
            
        fig = go.Figure(data=[go.Bar(
            x=sector_stats['평균점수'],
            y=sector_stats.index,
            orientation='h',
            marker_color='lightblue'
        )])
        fig.update_layout(
            title="업종별 평균 SEPA 점수",
            xaxis_title="평균 점수",
            height=400
        )
        return fig 