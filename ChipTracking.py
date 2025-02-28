import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date
import json

# Fungsi untuk menginisialisasi BigQuery client dari secrets
def get_bigquery_client():
    try:
        credentials_json = st.secrets["bigquery"]["credentials"]
        credentials = service_account.Credentials.from_service_account_info(json.loads(credentials_json))
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        return client
    except Exception as e:
        st.error(f"Terjadi kesalahan saat menginisialisasi BigQuery Client: {e}")
        return None

# Fungsi untuk mengambil data dari BigQuery
def fetch_bigquery_data(table_name, search_term, search_column):
    client = get_bigquery_client()
    if client is None:
        return None
    
    try:
        query = f"""
        SELECT *
        FROM `alfred-analytics-406004.analytics_alfred.{table_name}`
        WHERE CAST({search_column} AS STRING) LIKE '%{search_term}%'
        """
        df = client.query(query).to_dataframe()
        for col in df.columns:
            if df[col].dtype == 'int64':
                df[col] = df[col].astype(str)
            elif df[col].dtype == 'object':
                df[col] = df[col].fillna('')
        return df
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data dari BigQuery: {e}")
        return None

# Fungsi untuk format Rupiah
def format_rupiah(value):
    return f"{value:,.0f}".replace(",", ".")

# Fungsi utama yang akan diimpor
def main():
    # Judul aplikasi
    st.markdown("<h1 style='text-align: center;'>MMPP CHIP TRACKING</h1>", unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("<br><br>", unsafe_allow_html=True)

    # Sidebar untuk semua filter
    with st.sidebar:
        st.markdown("**Input No Chip**")
        search_term = st.text_input("Cari berdasarkan NoChip atau NoRS", "", label_visibility="collapsed")
        
        st.markdown("---")
        
        st.markdown("**Filter Transaksi TopUp LinkAja**")
        st.write("Tanggal")
        default_start = date(2023, 1, 1)
        default_end = datetime.now().date()
        linkaja_date_range = st.date_input("Pilih rentang tanggal", [default_start, default_end], key="linkaja_date", label_visibility="collapsed")
        linkaja_start_date, linkaja_end_date = linkaja_date_range if len(linkaja_date_range) == 2 else (default_start, default_end)
        
        st.markdown("---")
        
        st.markdown("**Filter Transaksi pjpnonpjp**")
        st.write("Tanggal")
        ngrs_date_range = st.date_input("Pilih rentang tanggal", [default_start, default_end], key="ngrs_date", label_visibility="collapsed")
        ngrs_start_date, ngrs_end_date = ngrs_date_range if len(ngrs_date_range) == 2 else (default_start, default_end)

    # Filter tambahan di sidebar
    if search_term:
        with st.sidebar:
            df_all_temp = fetch_bigquery_data("All_pjpnonpjp", search_term, "NoChip")
            if df_all_temp is not None and not df_all_temp.empty and "TransactionType" in df_all_temp.columns:
                transaction_types = df_all_temp["TransactionType"].unique().tolist()
                selected_transaction_types = st.multiselect(
                    "Pilih TransactionType", transaction_types, default=transaction_types, 
                    key="transaction_type_filter", label_visibility="collapsed"
                )
            else:
                selected_transaction_types = []
    else:
        selected_transaction_types = []

    # Scorecard
    if search_term:
        with st.spinner("Mengambil data untuk Scorecard..."):
            df_all = fetch_bigquery_data("All_pjpnonpjp", search_term, "NoChip")
            if df_all is not None and not df_all.empty and "Completion" in df_all.columns:
                df_all["Completion"] = pd.to_datetime(df_all["Completion"])
                df_all_filtered = df_all[
                    (df_all["Completion"].dt.date >= ngrs_start_date) & 
                    (df_all["Completion"].dt.date <= ngrs_end_date)
                ]
                if not df_all_filtered.empty:
                    if "TransactionType" in df_all_filtered.columns and selected_transaction_types:
                        df_all_filtered = df_all_filtered[df_all_filtered["TransactionType"].isin(selected_transaction_types)]
                    if not df_all_filtered.empty:
                        st.markdown("<h3 style='text-align: center;'>Ringkasan Data NGRS</h3>", unsafe_allow_html=True)
                        st.markdown("<br>", unsafe_allow_html=True)
                        st.markdown(
                            """
                            <style>
                            .scorecard-container { display: flex; justify-content: space-between; gap: 20px; margin-bottom: 20px; }
                            .scorecard { background-color: #f9f9f9; border-radius: 8px; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); padding: 15px; flex: 1; text-align: center; min-width: 0; }
                            .metric-label { color: #666; font-size: 14px; margin-bottom: 5px; }
                            .metric-value { color: #333; font-size: 18px; font-weight: bold; }
                            </style>
                            """,
                            unsafe_allow_html=True
                        )

                        df_linkaja = fetch_bigquery_data("LinkAjaXPJP", search_term, "NoRS")
                        total_debit = linkaja_transaction_count = 0
                        if df_linkaja is not None and not df_linkaja.empty and "InitiateDate" in df_linkaja.columns and "Debit" in df_linkaja.columns:
                            df_linkaja["InitiateDate"] = pd.to_datetime(df_linkaja["InitiateDate"])
                            df_linkaja_filtered = df_linkaja[
                                (df_linkaja["InitiateDate"].dt.date >= linkaja_start_date) & 
                                (df_linkaja["InitiateDate"].dt.date <= linkaja_end_date)
                            ]
                            if not df_linkaja_filtered.empty:
                                df_linkaja_filtered["Debit"] = pd.to_numeric(df_linkaja_filtered["Debit"], errors='coerce').fillna(0)
                                total_debit = df_linkaja_filtered["Debit"].sum()
                                linkaja_transaction_count = len(df_linkaja_filtered)

                        st.markdown('<div class="scorecard-container">', unsafe_allow_html=True)
                        col_score1, col_score2, col_score3 = st.columns(3)
                        with col_score1:
                            outlet_ids = df_all_filtered["OutletID"].dropna().unique().tolist() if "OutletID" in df_all_filtered.columns else []
                            st.markdown(f'<div class="scorecard"><div class="metric-label">OutletID</div><div class="metric-value">{", ".join(map(str, outlet_ids)) if len(outlet_ids) <= 2 else f"{len(outlet_ids)} (Multiple)" or "N/A"}</div></div>', unsafe_allow_html=True)
                        with col_score2:
                            outlet_names = df_all_filtered["OutletName"].dropna().unique().tolist() if "OutletName" in df_all_filtered.columns else []
                            st.markdown(f'<div class="scorecard"><div class="metric-label">OutletName</div><div class="metric-value">{", ".join(map(str, outlet_names)) if len(outlet_names) <= 2 else f"{len(outlet_names)} (Multiple)" or "N/A"}</div></div>', unsafe_allow_html=True)
                        with col_score3:
                            clusters = df_all_filtered["Cluster"].dropna().unique().tolist() if "Cluster" in df_all_filtered.columns else []
                            st.markdown(f'<div class="scorecard"><div class="metric-label">Cluster</div><div class="metric-value">{", ".join(map(str, clusters)) if len(clusters) <= 2 else f"{len(clusters)} (Multiple)" or "N/A"}</div></div>', unsafe_allow_html=True)
                        st.markdown('</div>', unsafe_allow_html=True)

                        st.markdown('<div class="scorecard-container">', unsafe_allow_html=True)
                        col_score4, col_score5, col_score6, col_score7 = st.columns(4)
                        with col_score4:
                            transaction_count = len(df_all_filtered["TransactionAmount"]) if "TransactionAmount" in df_all_filtered.columns else 0
                            st.markdown(f'<div class="scorecard"><div class="metric-label">Jumlah Transaksi NGRS</div><div class="metric-value">{transaction_count:,}</div></div>', unsafe_allow_html=True)
                        with col_score5:
                            total_spend = df_all_filtered["SpendAmount"].sum() if "SpendAmount" in df_all_filtered.columns else 0
                            st.markdown(f'<div class="scorecard"><div class="metric-label">Total SpendAmount</div><div class="metric-value">Rp {format_rupiah(total_spend)}</div></div>', unsafe_allow_html=True)
                        with col_score6:
                            st.markdown(f'<div class="scorecard"><div class="metric-label">Total Debit (LinkAja)</div><div class="metric-value">Rp {format_rupiah(total_debit)}</div></div>', unsafe_allow_html=True)
                        with col_score7:
                            st.markdown(f'<div class="scorecard"><div class="metric-label">Jumlah Transaksi LinkAja</div><div class="metric-value">{linkaja_transaction_count:,}</div></div>', unsafe_allow_html=True)
                        st.markdown('</div>', unsafe_allow_html=True)
                    else:
                        st.warning("Tidak ada data setelah menerapkan filter untuk scorecard.")
                else:
                    st.warning("Tidak ada data dalam rentang tanggal yang dipilih untuk scorecard.")
            else:
                st.warning("Tidak ada data yang cocok untuk scorecard.")
    st.markdown("<br><br>", unsafe_allow_html=True)

    # Layout kolom
    col1, col2 = st.columns(2)

    # Kolom Kiri: LinkAjaXPJP
    with col1:
        st.markdown("<h3 style='text-align: center;'>Transaksi TopUp LinkAja</h3>", unsafe_allow_html=True)
        if search_term:
            with st.spinner("Mengambil data LinkAjaXPJP..."):
                df_linkaja = fetch_bigquery_data("LinkAjaXPJP", search_term, "NoRS")
                if df_linkaja is not None and not df_linkaja.empty and "InitiateDate" in df_linkaja.columns and "Debit" in df_linkaja.columns:
                    df_linkaja["InitiateDate"] = pd.to_datetime(df_linkaja["InitiateDate"])
                    df_linkaja_filtered = df_linkaja[
                        (df_linkaja["InitiateDate"].dt.date >= linkaja_start_date) & 
                        (df_linkaja["InitiateDate"].dt.date <= linkaja_end_date)
                    ]
                    if not df_linkaja_filtered.empty:
                        df_linkaja_filtered["Debit"] = pd.to_numeric(df_linkaja_filtered["Debit"], errors='coerce').fillna(0)
                        df_linkaja_agg = df_linkaja_filtered.groupby(df_linkaja_filtered["InitiateDate"].dt.date).agg(
                            Count=('InitiateDate', 'size'), Total_Debit=('Debit', 'sum')
                        ).reset_index().sort_values("InitiateDate")
                        fig_linkaja = make_subplots(specs=[[{"secondary_y": True}]])
                        fig_linkaja.add_trace(go.Scatter(
                            x=df_linkaja_agg["InitiateDate"], y=df_linkaja_agg["Count"], mode="lines+markers+text", 
                            name="Jumlah Data", text=df_linkaja_agg["Count"], textposition="top center"
                        ), secondary_y=False)
                        fig_linkaja.add_trace(go.Bar(
                            x=df_linkaja_agg["InitiateDate"], y=df_linkaja_agg["Total_Debit"], name="Total Debit (Rp)", 
                            opacity=0.6, text=df_linkaja_agg["Total_Debit"].apply(format_rupiah), textposition="auto"
                        ), secondary_y=True)
                        fig_linkaja.update_layout(
                            xaxis_title="Tanggal", yaxis_title="Jumlah Data", yaxis2_title="Total Debit (Rp)", 
                            legend=dict(x=0, y=1.1, orientation="h")
                        )
                        st.plotly_chart(fig_linkaja, use_container_width=True)
                        df_linkaja_display = df_linkaja_filtered.copy()
                        if "Debit" in df_linkaja_display.columns:
                            df_linkaja_display["Debit"] = df_linkaja_display["Debit"].apply(format_rupiah)
                        st.dataframe(df_linkaja_display)
                        st.write(f"Total Data: {len(df_linkaja_filtered)}")
                    else:
                        st.warning("Tidak ada data dalam rentang tanggal yang dipilih untuk LinkAjaXPJP.")
                else:
                    st.warning("Tidak ada data yang cocok untuk LinkAjaXPJP.")
        else:
            st.info("Masukkan nomor di kolom pencarian di sidebar untuk melihat data LinkAjaXPJP.")

    # Kolom Kanan: All_pjpnonpjp
    with col2:
        st.markdown("<h3 style='text-align: center;'>Transaksi NGRS</h3>", unsafe_allow_html=True)
        if search_term:
            with st.spinner("Mengambil data All_pjpnonpjp..."):
                df_all = fetch_bigquery_data("All_pjpnonpjp", search_term, "NoChip")
                if df_all is not None and not df_all.empty and "Completion" in df_all.columns:
                    df_all["Completion"] = pd.to_datetime(df_all["Completion"])
                    df_all_filtered = df_all[
                        (df_all["Completion"].dt.date >= ngrs_start_date) & 
                        (df_all["Completion"].dt.date <= ngrs_end_date)
                    ]
                    if not df_all_filtered.empty:
                        if "TransactionType" in df_all_filtered.columns and selected_transaction_types:
                            df_all_filtered = df_all_filtered[df_all_filtered["TransactionType"].isin(selected_transaction_types)]
                        if not df_all_filtered.empty:
                            df_completion = df_all_filtered.groupby(df_all_filtered["Completion"].dt.date).size().reset_index(name="Count").sort_values("Completion")
                            fig_completion = go.Figure()
                            fig_completion.add_trace(go.Scatter(
                                x=df_completion["Completion"], y=df_completion["Count"], mode="lines+markers+text", 
                                name="Jumlah Data", text=df_completion["Count"], textposition="top center"
                            ))
                            fig_completion.update_layout(xaxis_title="Tanggal", yaxis_title="Jumlah Data")
                            st.plotly_chart(fig_completion, use_container_width=True)
                            df_display = df_all_filtered.copy()
                            if "SpendAmount" in df_display.columns:
                                df_display["SpendAmount"] = df_display["SpendAmount"].apply(format_rupiah)
                            st.dataframe(df_display)
                            st.write(f"Total Data: {len(df_all_filtered)}")
                        else:
                            st.warning("Tidak ada data setelah menerapkan filter TransactionType.")
                    else:
                        st.warning("Tidak ada data dalam rentang tanggal yang dipilih untuk All_pjpnonpjp.")
                else:
                    st.warning("Tidak ada data yang cocok untuk All_pjpnonpjp.")
        else:
            st.info("Masukkan nomor di kolom pencarian di sidebar untuk melihat data All_pjpnonpjp.")

if __name__ == "__main__":
    main()