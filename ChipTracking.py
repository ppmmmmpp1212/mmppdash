import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date
import json

# Fungsi untuk menginisialisasi BigQuery client
@st.cache_resource
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

# Fungsi untuk mengambil data Total Chip (dengan caching)
@st.cache_data
def fetch_chip_data_cached(table_name, date_column, start_date, end_date, cluster_column, selected_clusters):
    client = get_bigquery_client()
    if client is None:
        return {"total_chip": 0, "total_chip_unverified": 0}

    try:
        query = f"""
        SELECT 
            COUNT(DISTINCT NoRS) AS total_chip,
            COUNT(DISTINCT CASE WHEN pjp_NoRS IS NULL THEN NoRS END) AS total_chip_unverified
        FROM `alfred-analytics-406004.analytics_alfred.{table_name}`
        WHERE DATE({date_column}) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        AND {cluster_column} IN ({', '.join([str(cluster) for cluster in selected_clusters])})
        """
        job_config = bigquery.QueryJobConfig(use_query_cache=True, priority=bigquery.QueryPriority.INTERACTIVE)
        df = client.query(query, job_config=job_config).to_dataframe()
        total_chip = int(df["total_chip"].iloc[0]) if not df.empty else 0
        total_chip_unverified = int(df["total_chip_unverified"].iloc[0]) if not df.empty else 0
        return {"total_chip": total_chip, "total_chip_unverified": total_chip_unverified}
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data chip: {e}")
        return {"total_chip": 0, "total_chip_unverified": 0}

# Fungsi untuk mengambil data transaksi TopUp dan NGRS per ClusterID (dengan caching)
@st.cache_data
def fetch_transaction_summary_cached(linkaja_table, ngrs_table, date_column_linkaja, date_column_ngrs, start_date, end_date, cluster_column, selected_clusters):
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()

    try:
        linkaja_query = f"""
        SELECT 
            {cluster_column} AS ClusterID,
            COUNT(*) AS total_topup,
            COALESCE(SUM(CAST(Debit AS FLOAT64)), 0) AS value_topup
        FROM `alfred-analytics-406004.analytics_alfred.{linkaja_table}`
        WHERE DATE({date_column_linkaja}) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        AND {cluster_column} IN ({', '.join([str(cluster) for cluster in selected_clusters])})
        GROUP BY {cluster_column}
        """
        ngrs_query = f"""
        SELECT 
            ClusterID AS ClusterID,
            COUNT(*) AS total_ngrs,
            COALESCE(SUM(CAST(SpendAmount AS FLOAT64)), 0) AS value_ngrs
        FROM `alfred-analytics-406004.analytics_alfred.{ngrs_table}`
        WHERE DATE({date_column_ngrs}) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        AND ClusterID IN ({', '.join([str(cluster) for cluster in selected_clusters])})
        GROUP BY ClusterID
        """
        job_config = bigquery.QueryJobConfig(use_query_cache=True, priority=bigquery.QueryPriority.INTERACTIVE)
        df_linkaja = client.query(linkaja_query, job_config=job_config).to_dataframe()
        df_ngrs = client.query(ngrs_query, job_config=job_config).to_dataframe()
        df_combined = df_linkaja.merge(df_ngrs, on="ClusterID", how="outer").fillna(0)
        df_combined.columns = ["ClusterID", "Total Transaksi TopUp", "Nilai TopUp", "Total Trx NGRS", "Nilai Trx NGRS"]
        df_combined["Nilai TopUp"] = df_combined["Nilai TopUp"].apply(lambda x: f"Rp {format_rupiah(x)}")
        df_combined["Nilai Trx NGRS"] = df_combined["Nilai Trx NGRS"].apply(lambda x: f"Rp {format_rupiah(x)}")
        return df_combined
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data transaksi: {e}")
        return pd.DataFrame(columns=["ClusterID", "Total Transaksi TopUp", "Nilai TopUp", "Total Trx NGRS", "Nilai Trx NGRS"])

# Fungsi untuk mengambil aggregated data (dengan caching)
@st.cache_data
def fetch_aggregated_data_cached(start_date, end_date, cluster_ids):
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()
    try:
        query = f"""
        WITH ngrs_aggregated AS (
            SELECT 
                NoChip, 
                SUM(CAST(SpendAmount AS FLOAT64)) AS Total_Transaksi_NGRS, 
                COUNT(SpendAmount) AS Total_SpendAmount
            FROM `alfred-analytics-406004.analytics_alfred.ALL`
            WHERE DATE(Completion) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            GROUP BY NoChip
        ),
        la_aggregated AS (
            SELECT 
                NoRS, 
                ClusterID AS Cluster_ID,
                SUM(CAST(Debit AS FLOAT64)) AS Total_Debit, 
                COUNT(Debit) AS Total_Transaksi_Debit
            FROM `alfred-analytics-406004.analytics_alfred.LinkAjaXPJP`
            WHERE DATE(InitiateDate) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            AND ClusterID IN ({', '.join([str(cluster) for cluster in cluster_ids])})
            AND pjp_NoRS IS NULL
            GROUP BY NoRS, ClusterID
        )
        SELECT 
            LA.NoRS,  
            CAST(LA.ClusterID AS STRING) AS ClusterID,
            COALESCE(la_aggregated.Total_Debit, 0) AS Total_Debit, 
            COALESCE(la_aggregated.Total_Transaksi_Debit, 0) AS Total_Transaksi_Debit, 
            COALESCE(ngrs_aggregated.Total_Transaksi_NGRS, 0) AS Total_Transaksi_NGRS,
            COALESCE(ngrs_aggregated.Total_SpendAmount, 0) AS Total_SpendAmount,
            LA.OutletName
        FROM `alfred-analytics-406004.analytics_alfred.LinkAjaXPJP` AS LA
        LEFT JOIN la_aggregated ON LA.NoRS = la_aggregated.NoRS
        LEFT JOIN ngrs_aggregated ON LA.NoRS = ngrs_aggregated.NoChip
        WHERE DATE(LA.InitiateDate) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        AND LA.ClusterID IN ({', '.join([str(cluster) for cluster in cluster_ids])})
        AND LA.pjp_NoRS IS NULL
        GROUP BY 
            LA.NoRS, 
            LA.ClusterID,
            la_aggregated.Total_Debit, 
            la_aggregated.Total_Transaksi_Debit, 
            ngrs_aggregated.Total_Transaksi_NGRS, 
            ngrs_aggregated.Total_SpendAmount,
            LA.OutletName
        """
        job_config = bigquery.QueryJobConfig(use_query_cache=True, priority=bigquery.QueryPriority.INTERACTIVE)
        df = client.query(query, job_config=job_config).to_dataframe()
        if not df.empty:
            df["Total_Debit"] = df["Total_Debit"].apply(lambda x: f"Rp {format_rupiah(float(x))}" if pd.notna(x) else "Rp 0")
            df["Total_Transaksi_NGRS"] = df["Total_Transaksi_NGRS"].apply(lambda x: f"Rp {format_rupiah(float(x))}" if pd.notna(x) else "Rp 0")
        return df
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data agregat: {e}")
        return pd.DataFrame()

# Fungsi untuk mengambil aggregated data (b) (dengan caching)
@st.cache_data
def fetch_aggregated_data_b_cached(start_date, end_date, cluster_ids):
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()
    try:
        query = f"""
        WITH ngrs_aggregated AS (
            SELECT 
                NoChip, 
                SUM(CAST(SpendAmount AS FLOAT64)) AS Total_Transaksi_NGRS, 
                COUNT(SpendAmount) AS Total_SpendAmount
            FROM `alfred-analytics-406004.analytics_alfred.ALL`
            WHERE DATE(Completion) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            GROUP BY NoChip
        ),
        la_aggregated AS (
            SELECT 
                NoRS, 
                ClusterID AS Cluster_ID,
                SUM(CAST(Debit AS FLOAT64)) AS Total_Debit, 
                COUNT(Debit) AS Total_Transaksi_Debit
            FROM `alfred-analytics-406004.analytics_alfred.LinkAjaXPJP`
            WHERE DATE(InitiateDate) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            AND ClusterID IN ({', '.join([str(cluster) for cluster in cluster_ids])})
            AND pjp_NoRS IS NOT NULL
            GROUP BY NoRS, ClusterID
        )
        SELECT 
            LA.NoRS,  
            CAST(LA.ClusterID AS STRING) AS ClusterID,
            COALESCE(la_aggregated.Total_Debit, 0) AS Total_Debit, 
            COALESCE(la_aggregated.Total_Transaksi_Debit, 0) AS Total_Transaksi_Debit, 
            COALESCE(ngrs_aggregated.Total_Transaksi_NGRS, 0) AS Total_Transaksi_NGRS,
            COALESCE(ngrs_aggregated.Total_SpendAmount, 0) AS Total_SpendAmount,
            LA.OutletName
        FROM `alfred-analytics-406004.analytics_alfred.LinkAjaXPJP` AS LA
        LEFT JOIN la_aggregated ON LA.NoRS = la_aggregated.NoRS
        LEFT JOIN ngrs_aggregated ON LA.NoRS = ngrs_aggregated.NoChip
        WHERE DATE(LA.InitiateDate) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        AND LA.ClusterID IN ({', '.join([str(cluster) for cluster in cluster_ids])})
        AND LA.pjp_NoRS IS NOT NULL
        GROUP BY 
            LA.NoRS, 
            LA.ClusterID,
            la_aggregated.Total_Debit, 
            la_aggregated.Total_Transaksi_Debit, 
            ngrs_aggregated.Total_Transaksi_NGRS, 
            ngrs_aggregated.Total_SpendAmount,
            LA.OutletName
        """
        job_config = bigquery.QueryJobConfig(use_query_cache=True, priority=bigquery.QueryPriority.INTERACTIVE)
        df = client.query(query, job_config=job_config).to_dataframe()
        if not df.empty:
            df["Total_Debit"] = df["Total_Debit"].apply(lambda x: f"Rp {format_rupiah(float(x))}" if pd.notna(x) else "Rp 0")
            df["Total_Transaksi_NGRS"] = df["Total_Transaksi_NGRS"].apply(lambda x: f"Rp {format_rupiah(float(x))}" if pd.notna(x) else "Rp 0")
        return df
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data agregat: {e}")
        return pd.DataFrame()

# Fungsi untuk format Rupiah
def format_rupiah(value):
    return f"{value:,.0f}".replace(",", ".")

# Fungsi utama
def main():
    # Custom CSS untuk tampilan yang lebih menarik
    st.markdown("""
        <style>
      
        /* Judul utama dengan efek shadow dan font modern */
        .main-title {
            font-family: 'Arial', sans-serif;
            font-size: 36px;
            font-weight: bold;
            color: #2c3e50;
            text-align: center;
            text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.1);
            margin-top: 20px;
            margin-bottom: 30px;
        }
        /* Subjudul dengan warna kontras */
        .group-header {
            font-family: 'Arial', sans-serif;
            font-size: 24px;
            font-weight: bold;
            color: #2980b9;
            text-align: center;
            margin-top: 40px;
            margin-bottom: 20px;
        }
        /* Scorecard dengan efek hover */
        .scorecard {
            background: #ffffff;
            border-radius: 12px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
            padding: 20px;
            text-align: center;
            transition: transform 0.2s ease, box-shadow 0.2s ease;
            margin: 10px 0;
        }
        .scorecard:hover {
            transform: translateY(-5px);
            box-shadow: 0 6px 18px rgba(0, 0, 0, 0.15);
        }
        .metric-label {
            color: #7f8c8d;
            font-size: 16px;
            margin-bottom: 8px;
        }
        .metric-value {
            color: #2c3e50;
            font-size: 24px;
            font-weight: bold;
        }
        /* Filter section styling */
        .filter-section {
            background: #f5f7fa;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
            margin: 20px 0;
        }
        .filter-title {
            font-size: 20px;
            font-weight: bold;
            color: #34495e;
            margin-bottom: 15px;
        }
        /* Sidebar styling */
        .sidebar .sidebar-content {
            background: #34495e;
            color: white;
            border-radius: 0 12px 12px 0;
        }
        .sidebar-title {
            font-size: 18px;
            font-weight: bold;
            color: #ecf0f1;
            margin-bottom: 15px;
        }
        /* Tabel dan grafik */
        .stDataFrame {
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 6px rgba(0, 0, 0, 0.1);
        }
        </style>
    """, unsafe_allow_html=True)

    # Judul Utama dengan gaya baru
    st.markdown('<div class="main-title">MMPP Chip Tracking Dashboard</div>', unsafe_allow_html=True)

    # Default tanggal
    current_date = datetime.now().date()
    default_start = date(current_date.year, current_date.month, 1)
    default_end = datetime.now().date()

    # Sidebar dengan desain modern
    with st.sidebar:
        st.markdown('<div class="sidebar-title">Filter Total Chip & Transaksi</div>', unsafe_allow_html=True)
        st.markdown('<p style="color: #bdc3c7;">Pilih rentang tanggal:</p>', unsafe_allow_html=True)
        chip_date_range = st.date_input("", [default_start, default_end], key="chip_date", label_visibility="collapsed")
        chip_start_date, chip_end_date = chip_date_range if len(chip_date_range) == 2 else (default_start, default_end)

        def fetch_clusters():
            client = get_bigquery_client()
            if client is None:
                return []
            query = "SELECT DISTINCT ClusterID FROM `alfred-analytics-406004.analytics_alfred.LinkAjaXPJP` "
            df = client.query(query).to_dataframe()
            return [int(x) for x in df["ClusterID"].tolist()]

        cluster_ids = fetch_clusters()
        selected_cluster_ids = st.multiselect("Pilih ClusterID", cluster_ids, default=cluster_ids, key="cluster_id_filter", 
                                             help="Pilih satu atau lebih ClusterID untuk analisis.")

    # Total Chip Overview
    with st.spinner("Mengambil data Total Chip..."):
        chip_data = fetch_chip_data_cached(
            table_name="LinkAjaXPJP", date_column="InitiateDate",
            start_date=chip_start_date.strftime('%Y-%m-%d'), 
            end_date=chip_end_date.strftime('%Y-%m-%d'), 
            cluster_column="ClusterID", 
            selected_clusters=tuple(selected_cluster_ids)
        )
        total_chip = chip_data["total_chip"]
        total_chip_unverified = chip_data["total_chip_unverified"]

    st.markdown('<div class="group-header">Total Chip Overview</div>', unsafe_allow_html=True)
    col_chip1, col_chip2 = st.columns(2)
    with col_chip1:
        st.markdown(
            f"""
            <div class="scorecard">
                <div class="metric-label">Total Chip</div>
                <div class="metric-value">{total_chip:,}</div>
            </div>
            """, unsafe_allow_html=True
        )
    with col_chip2:
        st.markdown(
            f"""
            <div class="scorecard">
                <div class="metric-label">Total Chip PJP Unverified</div>
                <div class="metric-value">{total_chip_unverified:,}</div>
            </div>
            """, unsafe_allow_html=True
        )

    # Transaction Summary
    with st.spinner("Mengambil data transaksi untuk tabel..."):
        transaction_df = fetch_transaction_summary_cached(
            linkaja_table="LinkAjaXPJP", 
            ngrs_table="ALL", 
            date_column_linkaja="InitiateDate", 
            date_column_ngrs="Completion", 
            start_date=chip_start_date.strftime('%Y-%m-%d'), 
            end_date=chip_end_date.strftime('%Y-%m-%d'), 
            cluster_column="ClusterID", 
            selected_clusters=tuple(selected_cluster_ids)
        )

    st.markdown('<div class="group-header">Transaction Summary</div>', unsafe_allow_html=True)
    if not transaction_df.empty:
        st.dataframe(transaction_df, use_container_width=True)
    else:
        st.warning("Tidak ada data transaksi yang tersedia untuk ditampilkan.")

    # Aggregated Transaction Summary (pjp_NoRS IS NULL)
    with st.spinner("Mengambil data agregat LinkAja dan NGRS..."):
        aggregated_df = fetch_aggregated_data_cached(
            start_date=chip_start_date.strftime('%Y-%m-%d'),
            end_date=chip_end_date.strftime('%Y-%m-%d'),
            cluster_ids=tuple(selected_cluster_ids)
        )

    st.markdown('<div class="group-header">Transasksi TopUp dan NGRS No Chip NoN PJP</div>', unsafe_allow_html=True)
    if not aggregated_df.empty:
        st.dataframe(aggregated_df, use_container_width=True)
    else:
        st.warning("Tidak ada data agregat yang tersedia untuk ditampilkan.")

    # Aggregated Transaction Summary (pjp_NoRS IS NOT NULL)
    with st.spinner("Mengambil data agregat LinkAja dan NGRS..."):
        aggregated_df_b = fetch_aggregated_data_b_cached(
            start_date=chip_start_date.strftime('%Y-%m-%d'),
            end_date=chip_end_date.strftime('%Y-%m-%d'),
            cluster_ids=tuple(selected_cluster_ids)
        )

    st.markdown('<div class="group-header">Transasksi TopUp dan NGRS No Chip PJP</div>', unsafe_allow_html=True)
    if not aggregated_df_b.empty:
        st.dataframe(aggregated_df_b, use_container_width=True)
    else:
        st.warning("Tidak ada data agregat yang tersedia untuk ditampilkan.")

    # Filter Pencarian dan Tanggal dalam container
    with st.container():
        st.markdown('<div class="filter-section"><div class="filter-title">🔍 Filter Pencarian dan Tanggal</div>', unsafe_allow_html=True)
        search_term = st.text_input("", placeholder="Cari NoChip atau NoRS...", label_visibility="collapsed")
        
        col_date1, col_date2 = st.columns(2)
        with col_date1:
            st.markdown('<p style="color: #34495e; font-weight: bold;">Tanggal TopUp LinkAja</p>', unsafe_allow_html=True)
            linkaja_date_range = st.date_input("", [default_start, default_end], key="linkaja_date", label_visibility="collapsed")
            linkaja_start_date, linkaja_end_date = linkaja_date_range if len(linkaja_date_range) == 2 else (default_start, default_end)
        with col_date2:
            st.markdown('<p style="color: #34495e; font-weight: bold;">Tanggal NGRS</p>', unsafe_allow_html=True)
            ngrs_date_range = st.date_input("", [default_start, default_end], key="ngrs_date", label_visibility="collapsed")
            ngrs_start_date, ngrs_end_date = ngrs_date_range if len(ngrs_date_range) == 2 else (default_start, default_end)

        # Filter TransactionType
        if search_term:
            df_all_temp = fetch_bigquery_data("ALL", search_term, "NoChip")
            if df_all_temp is not None and not df_all_temp.empty and "TransactionType" in df_all_temp.columns:
                transaction_types = df_all_temp["TransactionType"].unique().tolist()
                selected_transaction_types = st.multiselect(
                    "Pilih Jenis Transaksi", transaction_types, default=transaction_types, 
                    key="transaction_type_filter", help="Filter transaksi berdasarkan jenis."
                )
            else:
                selected_transaction_types = []
        else:
            selected_transaction_types = []
        st.markdown('</div>', unsafe_allow_html=True)

    # Scorecard NGRS dan LinkAja
    if search_term:
        with st.spinner("Mengambil data untuk Scorecard..."):
            df_all = fetch_bigquery_data("ALL", search_term, "NoChip")
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
                        st.markdown('<div class="group-header">Ringkasan Data NGRS</div>', unsafe_allow_html=True)
                        col_score1, col_score2, col_score3 = st.columns(3)
                        with col_score1:
                            outlet_ids = df_all_filtered["OutletID"].dropna().unique().tolist() if "OutletID" in df_all_filtered.columns else []
                            st.markdown(f'<div class="scorecard"><div class="metric-label">Outlet ID</div><div class="metric-value">{", ".join(map(str, outlet_ids)) if len(outlet_ids) <= 2 else f"{len(outlet_ids)} (Multiple)" or "N/A"}</div></div>', unsafe_allow_html=True)
                        with col_score2:
                            outlet_names = df_all_filtered["OutletName"].dropna().unique().tolist() if "OutletName" in df_all_filtered.columns else []
                            st.markdown(f'<div class="scorecard"><div class="metric-label">Outlet Name</div><div class="metric-value">{", ".join(map(str, outlet_names)) if len(outlet_names) <= 2 else f"{len(outlet_names)} (Multiple)" or "N/A"}</div></div>', unsafe_allow_html=True)
                        with col_score3:
                            clusters = df_all_filtered["Cluster"].dropna().unique().tolist() if "Cluster" in df_all_filtered.columns else []
                            st.markdown(f'<div class="scorecard"><div class="metric-label">Cluster</div><div class="metric-value">{", ".join(map(str, clusters)) if len(clusters) <= 2 else f"{len(clusters)} (Multiple)" or "N/A"}</div></div>', unsafe_allow_html=True)

                        col_score4, col_score5, col_score6, col_score7 = st.columns(4)
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
                        with col_score4:
                            transaction_count = len(df_all_filtered["TransactionAmount"]) if "TransactionAmount" in df_all_filtered.columns else 0
                            st.markdown(f'<div class="scorecard"><div class="metric-label">Jml Transaksi NGRS</div><div class="metric-value">{transaction_count:,}</div></div>', unsafe_allow_html=True)
                        with col_score5:
                            total_spend = df_all_filtered["SpendAmount"].sum() if "SpendAmount" in df_all_filtered.columns else 0
                            st.markdown(f'<div class="scorecard"><div class="metric-label">Total Spend</div><div class="metric-value">Rp {format_rupiah(total_spend)}</div></div>', unsafe_allow_html=True)
                        with col_score6:
                            st.markdown(f'<div class="scorecard"><div class="metric-label">Total Debit LinkAja</div><div class="metric-value">Rp {format_rupiah(total_debit)}</div></div>', unsafe_allow_html=True)
                        with col_score7:
                            st.markdown(f'<div class="scorecard"><div class="metric-label">Jml Transaksi LinkAja</div><div class="metric-value">{linkaja_transaction_count:,}</div></div>', unsafe_allow_html=True)
                    else:
                        st.warning("Tidak ada data setelah menerapkan filter untuk scorecard.")
                else:
                    st.warning("Tidak ada data dalam rentang tanggal yang dipilih untuk scorecard.")
            else:
                st.warning("Tidak ada data yang cocok untuk scorecard.")

    # Layout dua kolom untuk grafik dan tabel
    col1, col2 = st.columns(2)

    # Kolom Kiri: Transaksi TopUp LinkAja
    with col1:
        st.markdown('<div class="group-header">Transaksi TopUp LinkAja</div>', unsafe_allow_html=True)
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
                            name="Jumlah Data", text=df_linkaja_agg["Count"], textposition="top center", line=dict(color="#2980b9")
                        ), secondary_y=False)
                        fig_linkaja.add_trace(go.Bar(
                            x=df_linkaja_agg["InitiateDate"], y=df_linkaja_agg["Total_Debit"], name="Total Debit (Rp)", 
                            opacity=0.6, text=df_linkaja_agg["Total_Debit"].apply(format_rupiah), textposition="auto", marker_color="#3498db"
                        ), secondary_y=True)
                        fig_linkaja.update_layout(
                            xaxis_title="Tanggal", yaxis_title="Jumlah Data", yaxis2_title="Total Debit (Rp)", 
                            legend=dict(x=0, y=1.1, orientation="h"), template="plotly_white"
                        )
                        st.plotly_chart(fig_linkaja, use_container_width=True)
                        df_linkaja_display = df_linkaja_filtered.copy()
                        if "Debit" in df_linkaja_display.columns:
                            df_linkaja_display["Debit"] = df_linkaja_display["Debit"].apply(format_rupiah)
                        st.dataframe(df_linkaja_display, use_container_width=True)
                        st.write(f"**Total Data:** {len(df_linkaja_filtered)}", unsafe_allow_html=True)
                    else:
                        st.warning("Tidak ada data dalam rentang tanggal yang dipilih untuk LinkAjaXPJP.")
                else:
                    st.warning("Tidak ada data yang cocok untuk LinkAjaXPJP.")
        else:
            st.info("Masukkan NoChip atau NoRS untuk melihat data LinkAjaXPJP.")

    # Kolom Kanan: Transaksi NGRS
    with col2:
        st.markdown('<div class="group-header">Transaksi NGRS</div>', unsafe_allow_html=True)
        if search_term:
            with st.spinner("Mengambil data ALL..."):
                df_all = fetch_bigquery_data("ALL", search_term, "NoChip")
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
                                name="Jumlah Data", text=df_completion["Count"], textposition="top center", line=dict(color="#e74c3c")
                            ))
                            fig_completion.update_layout(
                                xaxis_title="Tanggal", yaxis_title="Jumlah Data", template="plotly_white"
                            )
                            st.plotly_chart(fig_completion, use_container_width=True)
                            df_display = df_all_filtered.copy()
                            if "SpendAmount" in df_display.columns:
                                df_display["SpendAmount"] = df_display["SpendAmount"].apply(format_rupiah)
                            st.dataframe(df_display, use_container_width=True)
                            st.write(f"**Total Data:** {len(df_all_filtered)}", unsafe_allow_html=True)
                        else:
                            st.warning("Tidak ada data setelah menerapkan filter TransactionType.")
                    else:
                        st.warning("Tidak ada data dalam rentang tanggal yang dipilih untuk ALL.")
                else:
                    st.warning("Tidak ada data yang cocok untuk ALL.")
        else:
            st.info("Masukkan NoChip atau NoRS untuk melihat data ALL.")

if __name__ == "__main__":
    main()
