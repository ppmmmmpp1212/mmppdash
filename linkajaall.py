import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date
import json
import re
from io import BytesIO

# Fungsi untuk menginisialisasi BigQuery client dari secrets
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

# Fungsi untuk mengambil data dari BigQuery berdasarkan pencarian
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

@st.cache_data
def fetch_aggregate_data(table_name, count_column=None, sum_column=None, date_column=None, start_date=None, end_date=None, cluster_column=None, selected_clusters=None, is_cluster_string=False, filter_column=None, filter_not_zero=False, transaction_type_column=None, selected_transaction_types=None, transaction_scenario=None, credit_condition=False):
    client = get_bigquery_client()
    if client is None:
        return 0, 0
    
    try:
        query_parts = ["SELECT"]
        if count_column:
            query_parts.append(f"COUNT({count_column}) AS row_count")
        if sum_column:
            if count_column:
                query_parts.append(",")
            query_parts.append(f"COALESCE(SUM(CAST({sum_column} AS FLOAT64)), 0) AS total_sum")
        query_parts.append(f"FROM `alfred-analytics-406004.analytics_alfred.{table_name}`")
        
        # Tambahkan kondisi WHERE
        where_conditions = []
        if date_column and start_date and end_date:
            where_conditions.append(f"DATE({date_column}) BETWEEN DATE('{start_date}') AND DATE('{end_date}')")
        if cluster_column and selected_clusters:
            if is_cluster_string:
                clusters_str = ", ".join([f"'{str(cluster)}'" for cluster in selected_clusters])
            else:
                clusters_str = ", ".join([str(cluster) for cluster in selected_clusters])
            where_conditions.append(f"{cluster_column} IN ({clusters_str})")
        if filter_column and filter_not_zero:
            where_conditions.append(f"CAST({filter_column} AS FLOAT64) != 0")
        if transaction_type_column and selected_transaction_types:
            transaction_types_str = ", ".join([f"'{str(ttype)}'" for ttype in selected_transaction_types])
            where_conditions.append(f"{transaction_type_column} IN ({transaction_types_str})")
        if transaction_scenario:
            where_conditions.append(f"TransactionScenario = '{transaction_scenario}'")
        if credit_condition:
            where_conditions.append("CAST(Credit AS FLOAT64) != 0")
        
        if where_conditions:
            query_parts.append("WHERE " + " AND ".join(where_conditions))
        
        query = " ".join(query_parts)
        result = client.query(query).to_dataframe()
        
        row_count = int(result["row_count"].iloc[0]) if "row_count" in result.columns else 0
        total_sum = float(result["total_sum"].iloc[0]) if "total_sum" in result.columns else 0
        return row_count, total_sum
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil agregasi dari {table_name}: {e}")
        return 0, 0

@st.cache_data
def fetch_acquisition_data(start_date, end_date, selected_cluster_ids):
    client = get_bigquery_client()
    if client is None:
        return 0, 0
    
    try:
        query = f"""
        SELECT 
            COUNT(*) AS total_trx_acquisition,
            COALESCE(SUM(ABS(CAST(TransactionAmount AS FLOAT64))), 0) AS total_amount_acquisition
        FROM 
            `alfred-analytics-406004.analytics_alfred.alfred_ngrs_akui`
        WHERE 
            DATE(dt) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            AND ClusterID IN ({', '.join(map(str, selected_cluster_ids))})
        """
        result = client.query(query).to_dataframe()
        total_trx = int(result["total_trx_acquisition"].iloc[0])
        total_amount = float(result["total_amount_acquisition"].iloc[0])
        return total_trx, total_amount
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data Akuisisi dari alfred_ngrs_akui: {e}")
        return 0, 0


# Fungsi untuk format Rupiah
def format_rupiah(value):
    return f"{value:,.0f}".replace(",", ".")

# Fungsi untuk menerapkan filter berdasarkan operator
def apply_filter(df, column, operator, value):
    if value is None or value == "":
        return df
    value = float(value)
    if operator == "Sama dengan":
        return df[df[column] == value]
    elif operator == "Kurang dari atau sama dengan":
        return df[df[column] <= value]
    elif operator == "Lebih dari atau sama dengan":
        return df[df[column] >= value]
    elif operator == "Lebih dari":
        return df[df[column] > value]
    elif operator == "Kurang dari":
        return df[df[column] < value]
    return df

# Fungsi untuk menghasilkan file Excel
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Summary')
    excel_data = output.getvalue()
    return excel_data

@st.cache_data
def fetch_finpay_data(start_date, end_date, selected_cluster_ids):
    client = get_bigquery_client()
    if client is None:
        return 0, 0
    
    try:
        query = f"""
        SELECT 
            COUNT(*) AS total_trx_finpay,
            COALESCE(SUM(CAST(Credit AS FLOAT64)), 0) AS nilai_trx_finpay
        FROM 
            `alfred-analytics-406004.analytics_alfred.alfred_finpay`
        WHERE 
            Transaction = 'RECHARGE'
            AND ClusterID IN ({', '.join(map(str, selected_cluster_ids))})
            AND DATE(dt) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            AND Remarks LIKE 'Biaya%'
        """
        result = client.query(query).to_dataframe()
        total_trx = int(result["total_trx_finpay"].iloc[0])
        total_nilai = float(result["nilai_trx_finpay"].iloc[0])
        return total_trx, total_nilai
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data Finpay: {e}")
        return 0, 0

@st.cache_data
def fetch_total_tp(start_date, end_date, selected_transaction_types, selected_cluster_ids):
    client = get_bigquery_client()
    if client is None:
        return 0
    
    try:
        # Buat query dinamis berdasarkan filter
        query = f"""
        SELECT 
            COALESCE(SUM(
                CASE 
                    WHEN a.SpendAmount BETWEEN r.StartDenom AND r.EndDenom 
                    THEN (a.SpendAmount * (r.TP / 100))
                    ELSE 0 
                END
            ), 0) AS Total_TP
        FROM 
            `alfred-analytics-406004.analytics_alfred.All_pjpnonpjp` a
        LEFT JOIN 
            `alfred-analytics-406004.analytics_alfred.rate_ngrs_reguler` r
        ON 
            a.SpendAmount BETWEEN r.StartDenom AND r.EndDenom
            AND a.dt BETWEEN r.Start_Date AND r.End_Date
            AND a.ClusterID = r.ClusterID  
        WHERE 
            DATE(a.dt) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            AND a.TransactionType IN ({', '.join([f"'{ttype}'" for ttype in selected_transaction_types])})
            AND a.ClusterID IN ({', '.join(map(str, selected_cluster_ids))})
        """
        result = client.query(query).to_dataframe()
        total_tp = float(result["Total_TP"].iloc[0])
        return total_tp
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil Total_TP: {e}")
        return 0



@st.cache_data
def fetch_daily_summary(start_date, end_date, selected_transaction_types_ngrs, selected_cluster_ids):
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()
    
    try:
        query = f"""
        WITH LinkAjaDebit AS (
            SELECT 
                DATE(InitiateDate) AS date,
                COUNT(*) AS linkaja_debit_count,
                COALESCE(SUM(CAST(Debit AS FLOAT64)), 0) AS linkaja_debit_amount
            FROM `alfred-analytics-406004.analytics_alfred.linkaja_Digipos_B2B_tf_Cluster`
            WHERE DATE(InitiateDate) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
                AND ClusterID IN ({', '.join(map(str, selected_cluster_ids))})
                AND CAST(Debit AS FLOAT64) != 0
            GROUP BY DATE(InitiateDate)
        ),
        AlfredLinkAja AS (
            SELECT 
                DATE(InitiateDate) AS date,
                COUNT(*) AS alfred_count,
                COALESCE(SUM(CAST(Credit AS FLOAT64)), 0) AS alfred_amount
            FROM `alfred-analytics-406004.analytics_alfred.alfred_linkaja`
            WHERE DATE(InitiateDate) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
                AND ClusterID IN ({', '.join(map(str, selected_cluster_ids))})
                AND TransactionScenario = 'Digipos B2B Transfer'
                AND CAST(Credit AS FLOAT64) != 0
            GROUP BY DATE(InitiateDate)
        ),
        AlfredReversal AS (
            SELECT 
                DATE(InitiateDate) AS date,
                COUNT(*) AS reversal_count,
                COALESCE(SUM(CAST(Debit AS FLOAT64)), 0) AS reversal_amount
            FROM `alfred-analytics-406004.analytics_alfred.alfred_linkaja`
            WHERE DATE(InitiateDate) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
                AND ClusterID IN ({', '.join(map(str, selected_cluster_ids))})
                AND TransactionScenario = 'Buy Goods Reversal for General Merchant'
            GROUP BY DATE(InitiateDate)
        ),
        Finpay AS (
            SELECT 
                DATE(dt) AS date,
                COUNT(*) AS finpay_count,
                COALESCE(SUM(CAST(Credit AS FLOAT64)), 0) AS finpay_amount
            FROM `alfred-analytics-406004.analytics_alfred.alfred_finpay`
            WHERE DATE(dt) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
                AND ClusterID IN ({', '.join(map(str, selected_cluster_ids))})
                AND Transaction = 'RECHARGE'
                AND Remarks LIKE 'Biaya%'
            GROUP BY DATE(dt)
        ),
        NGRS AS (
            SELECT 
                DATE(Completion) AS date,
                COUNT(*) AS ngrs_count,
                COALESCE(SUM(CAST(SpendAmount AS FLOAT64)), 0) AS ngrs_amount,
                COALESCE(SUM(
                    CASE 
                        WHEN a.SpendAmount BETWEEN r.StartDenom AND r.EndDenom 
                        THEN (a.SpendAmount * (r.TP / 100)) - 20
                        ELSE 0 
                    END
                ), 0) AS total_tp
            FROM `alfred-analytics-406004.analytics_alfred.All_pjpnonpjp` a
            LEFT JOIN `alfred-analytics-406004.analytics_alfred.rate_ngrs_reguler` r
            ON a.SpendAmount BETWEEN r.StartDenom AND r.EndDenom
                AND a.dt BETWEEN r.Start_Date AND r.End_Date
                AND a.ClusterID = r.ClusterID
            WHERE DATE(Completion) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
                AND a.ClusterID IN ({', '.join(map(str, selected_cluster_ids))})
                AND a.TransactionType IN ({', '.join([f"'{ttype}'" for ttype in selected_transaction_types_ngrs])})
            GROUP BY DATE(Completion)
        )
        SELECT 
            COALESCE(n.date, l.date, a.date, r.date, f.date) AS Date,
            COALESCE(n.ngrs_count, 0) AS Total_Transaksi_NGRS,
            COALESCE(n.ngrs_amount, 0) AS Total_Nilai_Denom_NGRS,
            COALESCE(n.total_tp, 0) AS Total_TP_NGRS,
            COALESCE(l.linkaja_debit_count, 0) + COALESCE(a.alfred_count, 0) - COALESCE(r.reversal_count, 0) + COALESCE(f.finpay_count, 0) AS Total_Transaksi_LinkAja_Finpay,
            COALESCE(l.linkaja_debit_amount, 0) + COALESCE(a.alfred_amount, 0) - COALESCE(r.reversal_amount, 0) + COALESCE(f.finpay_amount, 0) AS Total_Nilai_Transaksi_LinkAja_Finpay
        FROM NGRS n
        FULL OUTER JOIN LinkAjaDebit l ON n.date = l.date
        FULL OUTER JOIN AlfredLinkAja a ON n.date = a.date
        FULL OUTER JOIN AlfredReversal r ON n.date = r.date
        FULL OUTER JOIN Finpay f ON n.date = f.date
        ORDER BY Date
        """
        df = client.query(query).to_dataframe()
        
        # Format kolom tanggal dan isi NaN
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        numeric_columns = ['Total_Transaksi_NGRS', 'Total_Nilai_Denom_NGRS', 'Total_TP_NGRS', 
                          'Total_Transaksi_LinkAja_Finpay', 'Total_Nilai_Transaksi_LinkAja_Finpay']
        for col in numeric_columns:
            df[col] = df[col].fillna(0)
        
        return df
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data harian: {e}")
        return pd.DataFrame()

def main():
    st.markdown(
        """
        <h1 style='text-align: center;'>LinkAja x NGRS Validator</h1>
        """,
        unsafe_allow_html=True
    )
    st.markdown("---")
    st.markdown('<div class="summary-title-box">', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
    st.markdown(
        """
        <style>
            .title-box {
                text-align: center;
                padding: 15px;
                background-color: white;
                border-radius: 10px;
                box-shadow: 2px 2px 10px rgba(0,0,0,0.2);
                margin-bottom: 20px;
                font-size: 20px;
                font-weight: bold;
                color: #333;
            }
        </style>

        <div class="title-box">
            Data Overview Summary
        </div>
        """,
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        """
        <style>
        .scorecard-container {
            display: flex;
            justify-content: space-between;
            gap: 20px;
            margin-bottom: 20px;
        }
        .scorecard {
            background-color: #f0f8ff;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            padding: 15px;
            flex: 1;
            text-align: center;
            min-width: 0;
        }
        .metric-label {
            color: #666;
            font-size: 14px;
            margin-bottom: 5px;
        }
        .metric-value {
            color: #333;
            font-size: 18px;
            font-weight: bold;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # Filter tanggal dan Cluster untuk scorecard
    default_start = date(2025, 1, 1)
    default_end = datetime.now().date()

    # Deklarasi variabel di scope fungsi
    start_date = default_start.strftime('%Y-%m-%d')
    end_date = default_end.strftime('%Y-%m-%d')

    with st.sidebar:
        st.sidebar.markdown("**Filter untuk Scorecard Keseluruhan**")
        st.sidebar.markdown("---")

        st.sidebar.markdown("**Filter Tanggal Keseluruhan**")
        filter_type = st.sidebar.selectbox(
            "Tipe Filter Tanggal",
            ["Per Hari", "Rentang Hari"],
            key="filter_type_overall"
        )
        if filter_type == "Per Hari":
            selected_date = st.sidebar.date_input("Pilih Tanggal", default_end, key="overall_date_single")
            start_date = selected_date.strftime('%Y-%m-%d')
            end_date = selected_date.strftime('%Y-%m-%d')
        else:
            date_range = st.sidebar.date_input("Pilih Rentang Tanggal", [default_start, default_end], key="overall_date_range")
            if len(date_range) == 2:
                start_date = date_range[0].strftime('%Y-%m-%d')
                end_date = date_range[1].strftime('%Y-%m-%d')
            else:
                start_date = default_start.strftime('%Y-%m-%d')
                end_date = default_end.strftime('%Y-%m-%d')

        # Filter TransactionType untuk NGRS
        def fetch_transaction_types(table_name):
            client = get_bigquery_client()
            if client is None:
                return []
            query = f"SELECT DISTINCT TransactionType FROM `alfred-analytics-406004.analytics_alfred.{table_name}` WHERE TransactionType IS NOT NULL"
            df = client.query(query).to_dataframe()
            return df["TransactionType"].tolist()

        transaction_types_ngrs = fetch_transaction_types("All_pjpnonpjp")
        selected_transaction_types_ngrs = st.sidebar.multiselect(
            "Pilih TransactionType NGRS",
            transaction_types_ngrs,
            default=transaction_types_ngrs,
            key="transaction_type_ngrs_filter"
        )

        # Filter ClusterID tunggal untuk semua tabel
        def fetch_clusters(table_name, cluster_column):
            client = get_bigquery_client()
            if client is None:
                return []
            query = f"SELECT DISTINCT {cluster_column} FROM `alfred-analytics-406004.analytics_alfred.{table_name}` WHERE {cluster_column} IS NOT NULL"
            df = client.query(query).to_dataframe()
            return [int(x) for x in df[cluster_column].tolist()]

        st.sidebar.markdown("**Filter ClusterID (Berlaku untuk Semua Tabel)**")
        cluster_ids = fetch_clusters("linkaja_Digipos_B2B_tf_Cluster", "ClusterID")
        selected_cluster_ids = st.sidebar.multiselect(
            "Pilih ClusterID",
            cluster_ids,
            default=cluster_ids,
            key="cluster_id_filter"
        )

    with st.spinner("Mengambil data agregasi untuk scorecard..."):
        # Fungsi untuk menghitung semua metrik per cluster
        def calculate_metrics_per_cluster(cluster_list):
            metrics = {}
            for cluster in cluster_list:
                cluster_metrics = {}
                
                # LinkAja Debit
                cluster_metrics['linkaja_row_count_debit'] = fetch_aggregate_data(
                    "linkaja_Digipos_B2B_tf_Cluster", count_column="*", date_column="InitiateDate",
                    start_date=start_date, end_date=end_date, cluster_column="ClusterID",
                    selected_clusters=[cluster], filter_column="Debit", filter_not_zero=True
                )[0]
                
                # LinkAja Credit
                cluster_metrics['linkaja_row_count_credit'] = fetch_aggregate_data(
                    "linkaja_Digipos_B2B_tf_Cluster", count_column="*", date_column="InitiateDate",
                    start_date=start_date, end_date=end_date, cluster_column="ClusterID",
                    selected_clusters=[cluster], filter_column="Credit", filter_not_zero=True
                )[0]
                
                # LinkAja Total Debit
                cluster_metrics['linkaja_total_debit'] = fetch_aggregate_data(
                    "linkaja_Digipos_B2B_tf_Cluster", sum_column="Debit", date_column="InitiateDate",
                    start_date=start_date, end_date=end_date, cluster_column="ClusterID",
                    selected_clusters=[cluster], filter_column="Debit", filter_not_zero=True
                )[1]
                
                # LinkAja Total Credit
                cluster_metrics['linkaja_total_credit'] = fetch_aggregate_data(
                    "linkaja_Digipos_B2B_tf_Cluster", sum_column="Credit", date_column="InitiateDate",
                    start_date=start_date, end_date=end_date, cluster_column="ClusterID",
                    selected_clusters=[cluster], filter_column="Credit", filter_not_zero=True
                )[1]
                
                # NGRS
                cluster_metrics['all_row_count'], cluster_metrics['all_total_spend'] = fetch_aggregate_data(
                    "All_pjpnonpjp", count_column="*", sum_column="SpendAmount", date_column="Completion",
                    start_date=start_date, end_date=end_date, cluster_column="ClusterID",
                    selected_clusters=[cluster], transaction_type_column="TransactionType",
                    selected_transaction_types=selected_transaction_types_ngrs
                )
                
                # Alfred LinkAja
                cluster_metrics['alfred_row_count'], cluster_metrics['alfred_total_amount'] = fetch_aggregate_data(
                    "alfred_linkaja", count_column="*", sum_column="Credit", date_column="InitiateDate",
                    start_date=start_date, end_date=end_date, cluster_column="ClusterID",
                    selected_clusters=[cluster], transaction_scenario="Digipos B2B Transfer",
                    credit_condition=True
                )
                
                # Alfred Reversal
                cluster_metrics['alfred_reversal_row_count'], cluster_metrics['alfred_reversal_total_amount'] = fetch_aggregate_data(
                    "alfred_linkaja", count_column="*", sum_column="Debit", date_column="InitiateDate",
                    start_date=start_date, end_date=end_date, cluster_column="ClusterID",
                    selected_clusters=[cluster], transaction_scenario="Buy Goods Reversal for General Merchant"
                )

                # Total_TP (baru)
                cluster_metrics['total_tp'] = fetch_total_tp(
                    start_date=start_date,
                    end_date=end_date,
                    selected_transaction_types=selected_transaction_types_ngrs,
                    selected_cluster_ids=[cluster]
                )
                
                # Finpay (baru)
                cluster_metrics['total_trx_finpay'], cluster_metrics['nilai_trx_finpay'] = fetch_finpay_data(
                    start_date=start_date,
                    end_date=end_date,
                    selected_cluster_ids=[cluster]
                )

                cluster_metrics['total_trx_acquisition'], cluster_metrics['total_amount_acquisition'] = fetch_acquisition_data(
                    start_date=start_date,
                    end_date=end_date,
                    selected_cluster_ids=[cluster]
                )
                        # Perhitungan tambahan
                # Perhitungan tambahan (diperbarui untuk menyertakan Finpay)
                cluster_metrics['total_transaksi_linkaja'] = (cluster_metrics['linkaja_row_count_debit'] + 
                                                            cluster_metrics['alfred_row_count'] - 
                                                            cluster_metrics['alfred_reversal_row_count'] + 
                                                            cluster_metrics['total_trx_finpay'])
                cluster_metrics['total_nilai_transaksi_ngrs'] = (cluster_metrics['linkaja_total_debit'] + 
                                                                cluster_metrics['alfred_total_amount'] - 
                                                                cluster_metrics['alfred_reversal_total_amount'] + 
                                                                cluster_metrics['nilai_trx_finpay'])
                cluster_metrics['fee'] = cluster_metrics['total_nilai_transaksi_ngrs'] - cluster_metrics['all_total_spend']
                
                metrics[cluster] = cluster_metrics
            return metrics

        # Hitung metrik untuk semua cluster yang dipilih
        all_metrics = calculate_metrics_per_cluster(selected_cluster_ids)

        # Hitung total untuk overview
        linkaja_row_count_debit = sum(m['linkaja_row_count_debit'] for m in all_metrics.values())
        linkaja_row_count_credit = sum(m['linkaja_row_count_credit'] for m in all_metrics.values())
        linkaja_total_debit = sum(m['linkaja_total_debit'] for m in all_metrics.values())
        linkaja_total_credit = sum(m['linkaja_total_credit'] for m in all_metrics.values())
        all_row_count = sum(m['all_row_count'] for m in all_metrics.values())
        all_total_spend = sum(m['all_total_spend'] for m in all_metrics.values())
        alfred_row_count = sum(m['alfred_row_count'] for m in all_metrics.values())
        alfred_total_amount = sum(m['alfred_total_amount'] for m in all_metrics.values())
        alfred_reversal_row_count = sum(m['alfred_reversal_row_count'] for m in all_metrics.values())
        alfred_reversal_total_amount = sum(m['alfred_reversal_total_amount'] for m in all_metrics.values())
        total_trx_finpay = sum(m['total_trx_finpay'] for m in all_metrics.values())  # Baru
        nilai_trx_finpay = sum(m['nilai_trx_finpay'] for m in all_metrics.values())
        total_transaksi_linkaja = sum(m['total_transaksi_linkaja'] for m in all_metrics.values())
        total_nilai_transaksi_ngrs = sum(m['total_nilai_transaksi_ngrs'] for m in all_metrics.values())
        fee = sum(m['fee'] for m in all_metrics.values())
        total_tp = sum(m['total_tp'] for m in all_metrics.values())
        total_trx_acquisition = sum(m['total_trx_acquisition'] for m in all_metrics.values())
        total_amount_acquisition = sum(m['total_amount_acquisition'] for m in all_metrics.values())
        # Tampilan scorecard overview (sama seperti sebelumnya)
        # Tambahkan CSS untuk styling (digunakan untuk semua grup)
        st.markdown(
            """
            <style>
                /* Box besar yang membungkus semua kolom */
                .group-container {
                    background-color: white;
                    padding: 20px;
                    border-radius: 12px;
                    box-shadow: 2px 4px 10px rgba(0,0,0,0.15);
                    margin-bottom: 20px;
                    border: 2px solid #edededed; /* Gray untuk border container */
                }

                /* Header dalam box besar */
                .group-header {
                    text-align: center;
                    font-size: 18px;
                    font-weight: normal;
                    color: #333;
                    padding: 10px;
                    border-bottom: 2px solid #edededed; /* Gray untuk border bawah header */
                    margin-bottom: 15px;
                }

                .group-header-font {
                    text-align: center;
                    font-size: 20px;
                    font-weight: bold;
                    color: #333;
                    padding: 10px;
                    border-bottom: 2px solid #edededed; /* Gray untuk border bawah header */
                    margin-bottom: 15px;
                }

                /* Wrapper untuk scorecards */
                .scorecard-container {
                    display: flex;
                    justify-content: space-between;
                    gap: 15px;
                    padding: 10px;
                }

                /* Box dalam box (scorecard utama) */
                .scorecard {
                    flex: 1;
                    text-align: center;
                    border-radius: 10px;
                    padding: 15px;
                    box-shadow: 2px 2px 8px rgba(0,0,0,0.1);
                    border: 1px solid #edededed; /* Gray untuk border scorecard */
                }

                /* Inner box untuk nilai */
                .metric-box {
                    background: #f9f9f9;
                    padding: 10px;
                    border-radius: 8px;
                    box-shadow: inset 2px 2px 5px rgba(0,0,0,0.1);
                    font-size: 18px;
                    font-weight: bold;
                    color: #333;
                    margin-top: 8px;
                }

                /* Label untuk setiap metric */
                .metric-label {
                    font-size: 14px;
                    color: #666;
                }

                /* Warna spesifik untuk grup berdasarkan kombinasi warna Anda */
                .linkaja-group { background-color: #8ec1da; } /* Med Blue untuk LinkAja */
                .finpay-group { background-color: #cdedecec; } /* Light Blue untuk Finpay */
                .ngrs-group { background-color: #f6d6c2; } /* Light Red untuk NGRS */
                .summary-group { background-color: #d47264; } /* Med Red untuk Summary */
            </style>
            """,
            unsafe_allow_html=True
        )

        # Grup 1: Transaksi LinkAja dengan border (seperti yang ada)
        st.markdown("<div class='group-header-font'>Transaksi LinkAja</div>", unsafe_allow_html=True)

        # Kolom untuk scorecards (dibungkus dalam div container utama)
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown(
                f"""
                <div class="scorecard linkaja-group">
                    <div class="metric-label">Total Transaksi Debit</div>
                    <div class="metric-box">{linkaja_row_count_debit:,}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col2:
            st.markdown(
                f"""
                <div class="scorecard linkaja-group">
                    <div class="metric-label">Total Transaksi Credit</div>
                    <div class="metric-box">{linkaja_row_count_credit:,}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col3:
            st.markdown(
                f"""
                <div class="scorecard linkaja-group">
                    <div class="metric-label">Total Transaksi OutCluster</div>
                    <div class="metric-box">{alfred_row_count:,}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col4:
            st.markdown(
                f"""
                <div class="scorecard linkaja-group">
                    <div class="metric-label">Total Transaksi Reversal</div>
                    <div class="metric-box">{alfred_reversal_row_count:,}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.markdown("</div>", unsafe_allow_html=True)  # Tutup scorecard-container
        st.markdown("</div>", unsafe_allow_html=True)  # Tutup group-container

        # Divider antar grup
        st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

        # Grup 2: Nilai Transaksi LinkAja dengan border (sesuai desain grup 1)
        st.markdown("<div class='group-header-font'>Nilai Transaksi LinkAja (Rp)</div>", unsafe_allow_html=True)

        # Kolom untuk scorecards
        col5, col6, col7, col8 = st.columns(4)

        with col5:
            st.markdown(
                f"""
                <div class="scorecard linkaja-group">
                    <div class="metric-label">Total Nilai Debit</div>
                    <div class="metric-box">Rp {format_rupiah(linkaja_total_debit)}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col6:
            st.markdown(
                f"""
                <div class="scorecard linkaja-group">
                    <div class="metric-label">Total Nilai Credit</div>
                    <div class="metric-box">Rp {format_rupiah(linkaja_total_credit)}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col7:
            st.markdown(
                f"""
                <div class="scorecard linkaja-group">
                    <div class="metric-label">Total Nilai OutCluster</div>
                    <div class="metric-box">Rp {format_rupiah(alfred_total_amount)}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col8:
            st.markdown(
                f"""
                <div class="scorecard linkaja-group">
                    <div class="metric-label">Total Nilai Reversal</div>
                    <div class="metric-box">Rp {format_rupiah(alfred_reversal_total_amount)}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.markdown("</div>", unsafe_allow_html=True)  # Tutup scorecard-container
        st.markdown("</div>", unsafe_allow_html=True)  # Tutup group-container

        # Divider antar grup
        st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

        # Grup 3: Transaksi & Nilai Finpay dengan border (sesuai desain grup 1)
        st.markdown("<div class='group-header-font'>Transaksi & Nilai Finpay</div>", unsafe_allow_html=True)

        # Kolom untuk scorecards
        col12, col13 = st.columns(2)

        with col12:
            st.markdown(
                f"""
                <div class="scorecard finpay-group">
                    <div class="metric-label">Total Transaksi Finpay</div>
                    <div class="metric-box">{total_trx_finpay:,}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col13:
            st.markdown(
                f"""
                <div class="scorecard finpay-group">
                    <div class="metric-label">Nilai Transaksi Finpay</div>
                    <div class="metric-box">Rp {format_rupiah(nilai_trx_finpay)}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.markdown("</div>", unsafe_allow_html=True)  # Tutup scorecard-container
        st.markdown("</div>", unsafe_allow_html=True)  # Tutup group-container

        # Divider antar grup
        st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

        # Grup 4: Transaksi dan Nilai NGRS dengan border (sesuai desain grup 1)
        st.markdown("<div class='group-header-font'>Transaksi & Nilai NGRS</div>", unsafe_allow_html=True)

        # Kolom untuk scorecards
        col9, col10, col11 = st.columns(3)

        with col9:
            st.markdown(
                f"""
                <div class="scorecard ngrs-group">
                    <div class="metric-label">Total Transaksi NGRS</div>
                    <div class="metric-box">{all_row_count:,}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col10:
            st.markdown(
                f"""
                <div class="scorecard ngrs-group">
                    <div class="metric-label">Total Nilai Denom NGRS</div>
                    <div class="metric-box">Rp {format_rupiah(all_total_spend)}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col11:
            st.markdown(
                f"""
                <div class="scorecard ngrs-group">
                    <div class="metric-label">Total NGRS * TP</div>
                    <div class="metric-box">Rp {format_rupiah(total_tp)}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col12:
            st.markdown(
                f"""
                <div class="scorecard ngrs-group">
                    <div class="metric-label">Total Transaksi Akuisisi</div>
                    <div class="metric-box">{total_trx_acquisition:,}</div>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        with col13:
            st.markdown(
                f"""
                <div class="scorecard ngrs-group">
                    <div class="metric-label">Total Nilai Akuisisi</div>
                    <div class="metric-box">Rp {format_rupiah(total_amount_acquisition)}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.markdown("</div>", unsafe_allow_html=True)  # Tutup scorecard-container
        st.markdown("</div>", unsafe_allow_html=True)  # Tutup group-container

        # Divider antar grup
        st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

        # Grup 5: Ringkasan Tambahan dengan border (sesuai desain grup 1)
        st.markdown("<div class='group-header-font'>Total Transaksi NGRS & LinkAja</div>", unsafe_allow_html=True)

        # Baris pertama: 2 kolom
        col14, col15 = st.columns(2)

        with col14:
            st.markdown(
                f"""
                <div class="scorecard summary-group">
                    <div class="metric-label">Total Transaksi LinkAja + Finpay</div>
                    <div class="metric-box">{total_transaksi_linkaja:,}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col15:
            st.markdown(
                f"""
                <div class="scorecard ngrs-group">
                    <div class="metric-label">Total Transaksi NGRS</div>
                    <div class="metric-box">{all_row_count:,}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        # Baris kedua: 2 kolom
        col16, col17 = st.columns(2)

        with col16:
            st.markdown(
                f"""
                <div class="scorecard summary-group">
                    <div class="metric-label">Total Nilai Transaksi LinkAja + Finpay</div>
                    <div class="metric-box">Rp {format_rupiah(total_nilai_transaksi_ngrs)}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col17:
            st.markdown(
                f"""
                <div class="scorecard ngrs-group">
                    <div class="metric-label">Total NGRS * TP</div>
                    <div class="metric-box">Rp {format_rupiah(total_tp)}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.markdown("</div>", unsafe_allow_html=True)  # Tutup group-container
        st.markdown("</div>", unsafe_allow_html=True)  # Tutup group-container

        # Tampilkan tabel per cluster jika ada tepat 6 cluster yang dipilih
        if len(selected_cluster_ids) == 6:
            st.markdown("---")
            st.markdown("<h3 style='text-align: center;'>Summary Per Cluster</h3>", unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)

            # Buat DataFrame untuk tabel
            cluster_data = {
                "Cluster ID": selected_cluster_ids,
                "Total Transaksi LinkAja Debit": [all_metrics[cluster]['linkaja_row_count_debit'] for cluster in selected_cluster_ids],
                "Total Transaksi LinkAja Credit": [all_metrics[cluster]['linkaja_row_count_credit'] for cluster in selected_cluster_ids],
                "Total Transaksi NGRS": [all_metrics[cluster]['all_row_count'] for cluster in selected_cluster_ids],
                "Total Transaksi LinkAja OutCluster": [all_metrics[cluster]['alfred_row_count'] for cluster in selected_cluster_ids],
                "Total Transaksi LinkAja Reversal": [all_metrics[cluster]['alfred_reversal_row_count'] for cluster in selected_cluster_ids],
                "Total Nilai (Rp) LinkAja Debit": [f"Rp {format_rupiah(all_metrics[cluster]['linkaja_total_debit'])}" for cluster in selected_cluster_ids],
                "Total Nilai (Rp) LinkAja Credit": [f"Rp {format_rupiah(all_metrics[cluster]['linkaja_total_credit'])}" for cluster in selected_cluster_ids],
                "Total Nilai Denom NGRS": [f" {format_rupiah(all_metrics[cluster]['all_total_spend'])}" for cluster in selected_cluster_ids],
                "Total Nilai (Rp) LinkAja Outcluster": [f"Rp {format_rupiah(all_metrics[cluster]['alfred_total_amount'])}" for cluster in selected_cluster_ids],
                "Total Nilai (Rp) LinkAja Reversal": [f"Rp {format_rupiah(all_metrics[cluster]['alfred_reversal_total_amount'])}" for cluster in selected_cluster_ids],
                "Total Transaksi LinkAja": [all_metrics[cluster]['total_transaksi_linkaja'] for cluster in selected_cluster_ids],
                "Total Transaksi NGRS": [all_metrics[cluster]['all_row_count'] for cluster in selected_cluster_ids],
                "Total Nilai Transaksi LinkAja": [f"Rp {format_rupiah(all_metrics[cluster]['total_nilai_transaksi_ngrs'])}" for cluster in selected_cluster_ids],
                "Total Nilai Denom NGRS": [f"Rp {format_rupiah(all_metrics[cluster]['all_total_spend'])}" for cluster in selected_cluster_ids],
                "Fee": [f"Rp {format_rupiah(all_metrics[cluster]['fee'])}" for cluster in selected_cluster_ids]
                "Total Transaksi Akuisisi": [all_metrics[cluster]['total_trx_acquisition'] for cluster in selected_cluster_ids],
                "Total Nilai Akuisisi": [f"Rp {format_rupiah(all_metrics[cluster]['total_amount_acquisition'])}" for cluster in selected_cluster_ids]
            }
            df_cluster = pd.DataFrame(cluster_data)

            # Tampilkan tabel
            st.dataframe(df_cluster, use_container_width=True)

        def normalize_phone_number(number):
            if pd.isna(number):  # Handle NaN
                return None
            number = str(number).strip()  # Konversi ke string dan hapus spasi
            if number.startswith('8'):  # Jika dimulai dengan 8 atau 6, tambahkan 62
                return '62' + number
            else:  # Jika tidak dimulai dengan 8 atau 6, kembalikan nomor asli tanpa perubahan
                return number
        
        st.markdown("""</div>""", unsafe_allow_html=True)  # Tutup group-border
        st.markdown("""</div>""", unsafe_allow_html=True)  # Tutup group-border

        with st.spinner("Menyiapkan data untuk Excel dan grafik..."):
            # Fungsi untuk mengambil data harian
            daily_summary_df = fetch_daily_summary(
                start_date=start_date,
                end_date=end_date,
                selected_transaction_types_ngrs=selected_transaction_types_ngrs,
                selected_cluster_ids=selected_cluster_ids
            )
            
            if not daily_summary_df.empty:
                # Tampilkan preview tabel
                st.markdown('</div>', unsafe_allow_html=True)
                st.markdown(
                    """
                    <style>
                        .title-box {
                            text-align: center;
                            padding: 15px;
                            background-color: white;
                            border-radius: 10px;
                            box-shadow: 2px 2px 10px rgba(0,0,0,0.2);
                            margin-bottom: 20px;
                            font-size: 20px;
                            font-weight: bold;
                            color: #333;
                        }
                    </style>

                    <div class="title-box">
                        Preview Summary
                    </div>
                    """,
                    unsafe_allow_html=True
                )
                st.markdown(
                    """
                    <style>
                    .centered-dataframe {
                        display: flex;
                        justify-content: center;
                        margin-bottom: 20px;
                    }
                    .centered-dataframe div[data-testid="stDataFrame"] {
                        width: 80%; /* Lebar maksimum untuk tetap responsif */
                        max-width: 1200px; /* Batas maksimum untuk mencegah terlalu lebar */
                    }
                    </style>
                    """,
                    unsafe_allow_html=True
                )

                # Tampilkan preview tabel di tengah
                with st.container():
                    st.markdown('<div class="centered-dataframe">', unsafe_allow_html=True)
                    st.dataframe(daily_summary_df)
                    st.markdown('</div>', unsafe_allow_html=True)
                
                # Format nilai Rupiah untuk kolom nilai
                df_for_excel = daily_summary_df.copy()
                df_for_excel['Total_Nilai_Denom_NGRS'] = df_for_excel['Total_Nilai_Denom_NGRS'].apply(format_rupiah)
                df_for_excel['Total_TP_NGRS'] = df_for_excel['Total_TP_NGRS'].apply(format_rupiah)
                df_for_excel['Total_Nilai_Transaksi_LinkAja_Finpay'] = df_for_excel['Total_Nilai_Transaksi_LinkAja_Finpay'].apply(format_rupiah)
                
                # Generate Excel file
                excel_data = to_excel(df_for_excel)
                
                # Tombol download
                st.download_button(
                    label="Unduh Summary Harian (Excel)",
                    data=excel_data,
                    file_name=f"Daily_Summary_{start_date}_to_{end_date}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

                st.markdown("""</div>""", unsafe_allow_html=True)  # Tutup group-border
                st.markdown("""</div>""", unsafe_allow_html=True)  # Tutup group-border

                # Buat timeseries plots menggunakan Plotly
                st.markdown(
                    """
                    <style>
                        .title-box {
                            text-align: center;
                            padding: 15px;
                            background-color: white;
                            border-radius: 10px;
                            box-shadow: 2px 2px 10px rgba(0,0,0,0.2);
                            margin-bottom: 20px;
                            font-size: 20px;
                            font-weight: bold;
                            color: #333;
                        }
                    </style>

                    <div class="title-box">
                        Timeseries Plot
                    </div>
                    """,
                    unsafe_allow_html=True
                )

                # Plot 1: Total Transaksi NGRS dan Total Transaksi LinkAja + Finpay (Kiri)
                fig1 = go.Figure()
                fig1.add_trace(go.Scatter(x=daily_summary_df['Date'], y=daily_summary_df['Total_Transaksi_NGRS'], mode='lines+markers', name='Total Transaksi NGRS', line=dict(color='green')))
                fig1.add_trace(go.Scatter(x=daily_summary_df['Date'], y=daily_summary_df['Total_Transaksi_LinkAja_Finpay'], mode='lines+markers', name='Total Transaksi LinkAja + Finpay', line=dict(color='blue')))
                fig1.update_layout(
                    title='Total Transaksi NGRS vs LinkAja + Finpay',
                    xaxis_title='Tanggal',
                    yaxis_title='Jumlah Transaksi',
                    template='plotly_white'
                )

                # Plot 2: Total TP NGRS dan Total Nilai Transaksi LinkAja + Finpay (Kanan)
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(x=daily_summary_df['Date'], y=daily_summary_df['Total_TP_NGRS'], mode='lines+markers', name='Total TP NGRS', line=dict(color='purple')))
                fig2.add_trace(go.Scatter(x=daily_summary_df['Date'], y=daily_summary_df['Total_Nilai_Transaksi_LinkAja_Finpay'], mode='lines+markers', name='Total Nilai Transaksi LinkAja + Finpay', line=dict(color='orange')))
                fig2.update_layout(
                    title='Total TP NGRS vs Nilai Transaksi LinkAja + Finpay',
                    xaxis_title='Tanggal',
                    yaxis_title='Nilai (Rp)',
                    template='plotly_white'
                )

                # Tampilkan plot berdampingan menggunakan container CSS
                st.markdown("<div class='plot-container'>", unsafe_allow_html=True)
                st.plotly_chart(fig1, use_container_width=True)
                st.plotly_chart(fig2, use_container_width=True)
                st.markdown("</div>", unsafe_allow_html=True)
            else:
                st.warning("Tidak ada data untuk periode yang dipilih.")

        # Fungsi untuk mengambil data detail dari masing-masing tabel dengan filter
        def fetch_linkaja_data():
            client = get_bigquery_client()
            if client is None:
                return pd.DataFrame()
            
            query = f"""
            SELECT *
            FROM `alfred-analytics-406004.analytics_alfred.linkaja_Digipos_B2B_tf_Cluster`
            WHERE DATE(InitiateDate) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            AND ClusterID IN ({', '.join(map(str, selected_cluster_ids))})
            AND (CAST(Credit AS FLOAT64) != 0)
            """
            df = client.query(query).to_dataframe()
            
            if 'CounterParty' in df.columns:
                def extract_first_number(text):
                    if pd.isna(text):
                        return None
                    match = re.match(r'(\d+)', str(text))
                    return match.group(1) if match else None
                df['NoRS'] = df['CounterParty'].apply(extract_first_number)
            
            if 'NoRS' in df.columns:
                df['NoRS'] = df['NoRS'].apply(normalize_phone_number)
            
            for col in df.columns:
                dtype_str = str(df[col].dtype).lower()
                if 'date' in dtype_str or dtype_str.startswith('db_dtypes'):
                    df[col] = pd.to_datetime(df[col], utc=True).dt.tz_localize(None)
                    df[col] = df[col].where(df[col].notna(), None)
                elif dtype_str in ['int64', 'int32', 'uint64', 'uint32', 'float64', 'float32']:
                    df[col] = df[col].where(df[col].notna(), None)
                elif dtype_str == 'bool':
                    df[col] = df[col].where(df[col].notna(), False)
                else:
                    df[col] = df[col].fillna('')
            return df

        def fetch_ngrs_data():
            client = get_bigquery_client()
            if client is None:
                return pd.DataFrame()
            
            query = f"""
            SELECT *
            FROM `alfred-analytics-406004.analytics_alfred.All_pjpnonpjp`
            WHERE DATE(Completion) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            AND ClusterID IN ({', '.join(map(str, selected_cluster_ids))})
            AND TransactionType IN ({', '.join([f"'{ttype}'" for ttype in selected_transaction_types_ngrs])})
            """
            df = client.query(query).to_dataframe()
            
            if 'NoChip' in df.columns:
                df['NoChip'] = df['NoChip'].apply(normalize_phone_number)
            
            for col in df.columns:
                dtype_str = str(df[col].dtype).lower()
                if 'date' in dtype_str or dtype_str.startswith('db_dtypes'):
                    df[col] = pd.to_datetime(df[col], utc=True).dt.tz_localize(None)
                    df[col] = df[col].where(df[col].notna(), None)
                elif dtype_str in ['int64', 'int32', 'uint64', 'uint32', 'float64', 'float32']:
                    df[col] = df[col].where(df[col].notna(), None)
                elif dtype_str == 'bool':
                    df[col] = df[col].where(df[col].notna(), False)
                else:
                    df[col] = df[col].fillna('')
            return df

        def fetch_alfred_data():
            client = get_bigquery_client()
            if client is None:
                return pd.DataFrame()
            
            query = f"""
            SELECT *
            FROM `alfred-analytics-406004.analytics_alfred.alfred_linkaja`
            WHERE DATE(InitiateDate) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            AND ClusterID IN ({', '.join(map(str, selected_cluster_ids))})
            AND (
                (TransactionScenario = 'Digipos B2B Transfer' AND CAST(Credit AS FLOAT64) != 0)
                OR TransactionScenario = 'Buy Goods Reversal for General Merchant'
            )
            """
            df = client.query(query).to_dataframe()
            
            if 'CounterParty' in df.columns:
                def extract_first_number(text):
                    if pd.isna(text):
                        return None
                    match = re.match(r'(\d+)', str(text))
                    return match.group(1) if match else None
                df['NoRS'] = df['CounterParty'].apply(extract_first_number)
            
            if 'NoRS' in df.columns:
                df['NoRS'] = df['NoRS'].apply(normalize_phone_number)
            
            for col in df.columns:
                dtype_str = str(df[col].dtype).lower()
                if 'date' in dtype_str or dtype_str.startswith('db_dtypes'):
                    df[col] = pd.to_datetime(df[col], utc=True).dt.tz_localize(None)
                    df[col] = df[col].where(df[col].notna(), None)
                elif dtype_str in ['int64', 'int32', 'uint64', 'uint32', 'float64', 'float32']:
                    df[col] = df[col].where(df[col].notna(), None)
                elif dtype_str == 'bool':
                    df[col] = df[col].where(df[col].notna(), False)
                else:
                    df[col] = df[col].fillna('')
            return df


        @st.cache_data
        def get_missing_numbers_in_ngrs():
            linkaja_df = fetch_linkaja_data()
            ngrs_df = fetch_ngrs_data()
            alfred_df = fetch_alfred_data()

            if linkaja_df.empty or alfred_df.empty or ngrs_df.empty:
                st.error("Salah satu atau semua DataFrame kosong. Periksa query, filter, atau koneksi BigQuery.")
                return pd.DataFrame()

            try:
                combined_nors = pd.concat([linkaja_df['NoRS'], alfred_df['NoRS']]).drop_duplicates().dropna()
            except KeyError as e:
                st.error(f"Error: Kolom 'NoRS' tidak ditemukan. ({str(e)})")
                return pd.DataFrame()

            if combined_nors.empty:
                st.warning("Tidak ada data di kolom NoRS setelah penggabungan dan pembersihan.")
                return pd.DataFrame()

            ngrs_nochip = ngrs_df['NoChip'].drop_duplicates().dropna()

            if ngrs_nochip.empty:
                st.warning("Tidak ada data di kolom NoChip setelah pembersihan.")
                return pd.DataFrame()

            missing_numbers = combined_nors[~combined_nors.isin(ngrs_nochip)]

            if missing_numbers.empty:
                st.info("Tidak ada nomor yang hilang ditemukan antara NoRS dan NoChip.")
                return pd.DataFrame()

            return pd.DataFrame(missing_numbers, columns=['NoRS'])


        @st.cache_data
        def get_missing_numbers_in_linkaja():
            linkaja_df = fetch_linkaja_data()
            ngrs_df = fetch_ngrs_data()
            alfred_df = fetch_alfred_data()

            if linkaja_df.empty or alfred_df.empty or ngrs_df.empty:
                st.error("Salah satu atau semua DataFrame kosong. Periksa query, filter, atau koneksi BigQuery.")
                return pd.DataFrame()

            try:
                combined_nors = pd.concat([linkaja_df['NoRS'], alfred_df['NoRS']]).drop_duplicates().dropna()
            except KeyError as e:
                st.error(f"Error: Kolom 'NoRS' tidak ditemukan. ({str(e)})")
                return pd.DataFrame()

            if combined_nors.empty:
                st.warning("Tidak ada data di kolom NoRS setelah penggabungan dan pembersihan.")
                return pd.DataFrame()

            ngrs_nochip = ngrs_df['NoChip'].drop_duplicates().dropna()

            if ngrs_nochip.empty:
                st.warning("Tidak ada data di kolom NoChip setelah pembersihan.")
                return pd.DataFrame()

            missing_numbers = ngrs_nochip[~ngrs_nochip.isin(combined_nors)]

            if missing_numbers.empty:
                st.info("Tidak ada nomor dari NGRS yang hilang di gabungan LinkAja/Alfred.")
                return pd.DataFrame()

            return pd.DataFrame(missing_numbers, columns=['NoChip'])


        @st.cache_data
        def get_full_missing_in_ngrs():
            linkaja_df = fetch_linkaja_data()
            ngrs_df = fetch_ngrs_data()
            alfred_df = fetch_alfred_data()

            if linkaja_df.empty or alfred_df.empty or ngrs_df.empty:
                st.error("Salah satu atau semua DataFrame kosong. Periksa query, filter, atau koneksi BigQuery.")
                return pd.DataFrame()

            try:
                combined_nors = pd.concat([linkaja_df['NoRS'], alfred_df['NoRS']]).drop_duplicates().dropna()
            except KeyError as e:
                st.error(f"Error: Kolom 'NoRS' tidak ditemukan. ({str(e)})")
                return pd.DataFrame()

            ngrs_nochip = ngrs_df['NoChip'].drop_duplicates().dropna()

            missing_numbers = combined_nors[~combined_nors.isin(ngrs_nochip)]

            if missing_numbers.empty:
                return pd.DataFrame()

            combined_df = pd.concat([linkaja_df, alfred_df])
            full_missing_data = combined_df[combined_df['NoRS'].isin(missing_numbers)]

            return full_missing_data


        @st.cache_data
        def get_full_missing_in_linkaja():
            linkaja_df = fetch_linkaja_data()
            ngrs_df = fetch_ngrs_data()
            alfred_df = fetch_alfred_data()

            if linkaja_df.empty or alfred_df.empty or ngrs_df.empty:
                st.error("Salah satu atau semua DataFrame kosong. Periksa query, filter, atau koneksi BigQuery.")
                return pd.DataFrame()

            try:
                combined_nors = pd.concat([linkaja_df['NoRS'], alfred_df['NoRS']]).drop_duplicates().dropna()
            except KeyError as e:
                st.error(f"Error: Kolom 'NoRS' tidak ditemukan. ({str(e)})")
                return pd.DataFrame()

            ngrs_nochip = ngrs_df['NoChip'].drop_duplicates().dropna()

            missing_numbers = ngrs_nochip[~ngrs_nochip.isin(combined_nors)]

            if missing_numbers.empty:
                return pd.DataFrame()

            full_missing_data = ngrs_df[ngrs_df['NoChip'].isin(missing_numbers)]

            return full_missing_data

        # Streamlit app - Analisis NoChip (dibawah timeseries plots)
        st.markdown("---")
        st.markdown(
                    """
                    <style>
                        .title-box {
                            text-align: center;
                            padding: 15px;
                            background-color: white;
                            border-radius: 10px;
                            box-shadow: 2px 2px 10px rgba(0,0,0,0.2);
                            margin-bottom: 20px;
                            font-size: 20px;
                            font-weight: bold;
                            color: #333;
                        }
                    </style>

                    <div class="title-box">
                        Analisis Transaksi Anomali
                    </div>
                    """,
                    unsafe_allow_html=True
                )
        
        st.markdown("<div class='group-header'>Chip Data LinkAja yang Tidak Ada di Data NGRS</div>", unsafe_allow_html=True)

        with st.spinner("Menghitung total NoChip yang hilang di NGRS..."):
            result_df_ngrs = get_missing_numbers_in_ngrs()
            total_missing_norchip = len(result_df_ngrs) if not result_df_ngrs.empty else 0

        with st.container():
            # Styling tambahan untuk keterangan (opsional, jika ingin konsisten dengan desain Anda)
            st.markdown(
                """
                <style>
                .info-message {
                    margin-bottom: 10px;
                    padding: 10px;
                    background-color: #f0f8ff;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                    text-align: center;
                }
                </style>
                """,
                unsafe_allow_html=True
            )

            with st.spinner("Menyiapkan data missing in NGRS..."):
                result_df_ngrs = get_missing_numbers_in_ngrs()
                total_missing_numbers = len(result_df_ngrs) if not result_df_ngrs.empty else 0

                if not result_df_ngrs.empty:
                    st.success(f"Data ditemukan! Berikut adalah nomor dari LinkAja/Alfred yang tidak ada di NGRS Data: ({total_missing_numbers} nomor ditemukan)")
                    st.dataframe(result_df_ngrs)
                else:
                    st.warning("Tidak ada nomor yang hilang di NGRS.")

            # Tabel tambahan pertama: Data lengkap dari nomor yang hilang di NGRS
            st.markdown("<div class='group-header'>Data Transaksi Chip LinkAja yang Tidak ada di NGRS</div>", unsafe_allow_html=True)
        
        # Tabel tambahan pertama: Data lengkap dari nomor yang hilang di NGRS
        
       
        with st.spinner("Menyiapkan data lengkap missing in NGRS..."):
            full_df_ngrs = get_full_missing_in_ngrs()
            if not full_df_ngrs.empty:
                st.success("Data lengkap ditemukan untuk nomor yang tidak ada di NGRS:")
                st.dataframe(full_df_ngrs)
                excel_data_ngrs = to_excel(full_df_ngrs)
                st.download_button(
                    label="Unduh Data Lengkap Missing in NGRS (Excel)",
                    data=excel_data_ngrs,
                    file_name=f"Full_Missing_in_NGRS_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.warning("Tidak ada data lengkap untuk nomor yang hilang di NGRS.")

        # Tabel kedua: Nomor dari NGRS yang tidak ada di LinkAja/Alfred (hanya nomor)
        
        st.markdown("<div class='group-header'>Chip Data NGRS yang Tidak Ada di Data LinkAja</div>", unsafe_allow_html=True)
       
        with st.spinner("Menyiapkan data missing in LinkAja/Alfred..."):
            result_df_linkaja = get_missing_numbers_in_linkaja()
            if not result_df_linkaja.empty:
                st.success("Data ditemukan! Berikut adalah nomor dari NGRS yang tidak ada di LinkAja/Alfred:")
                st.dataframe(result_df_linkaja)
            else:
                st.warning("Tidak ada nomor dari NGRS yang hilang di LinkAja/Alfred.")


        st.markdown("<div class='group-header'>Data Transaksi Chip NGRS yang Tidak ada di LinkAja</div>", unsafe_allow_html=True)
        # Tabel tambahan kedua: Data lengkap dari nomor NGRS yang hilang di LinkAja/Alfred
        with st.spinner("Menghitung total transaksi dan nilai NoChip hilang di LinkAja/Alfred..."):
            full_df_linkaja = get_full_missing_in_linkaja()
            total_transactions_missing = len(full_df_linkaja) if not full_df_linkaja.empty else 0
            total_value_missing = full_df_linkaja['SpendAmount'].sum() if not full_df_linkaja.empty and 'SpendAmount' in full_df_linkaja.columns else 0

        with st.container():
            st.markdown("""<div class="linkaja-missing-scorecard-container">""", unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(
                    f"""
                    <div class="linkaja-missing-scorecard">
                        <div class="linkaja-missing-metric-label">Total Transaksi</div>
                        <div class="linkaja-missing-metric-value">{total_transactions_missing:,}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            
            with col2:
                st.markdown(
                    f"""
                    <div class="linkaja-missing-scorecard">
                        <div class="linkaja-missing-metric-label">Total Nilai Transaksi </div>
                        <div class="linkaja-missing-metric-value"> {format_rupiah(total_value_missing)}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            
            st.markdown("""</div>""", unsafe_allow_html=True)
            
            st.markdown(
            
                """
                <style>
                .linkaja-missing-scorecard-container {
                    display: flex;
                    gap: 15px;
                    justify-content: center;
                    margin-bottom: 20px;
                }
                .linkaja-missing-scorecard {
                    background-color: #ffb5b5;
                    border-radius: 8px;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                    padding: 15px;
                    flex: 1;
                    text-align: center;
                    min-width: 150px;
                    max-width: 250px;
                }
                .linkaja-missing-metric-label {
                    color: #666;
                    font-size: 14px;
                    margin-bottom: 5px;
                }
                .linkaja-missing-metric-value {
                    color: #333;
                    font-size: 18px;
                    font-weight: bold;
                }
                </style>
                """,
                unsafe_allow_html=True
            )
        # Tabel tambahan kedua: Data lengkap dari nomor NGRS yang hilang di LinkAja/Alfred
        
        with st.spinner("Menyiapkan data lengkap missing in LinkAja/Alfred..."):
            if not full_df_linkaja.empty:
                st.success("Data lengkap ditemukan untuk nomor dari NGRS yang tidak ada di LinkAja/Alfred:")
                st.dataframe(full_df_linkaja)
                excel_data_linkaja = to_excel(full_df_linkaja)
                st.download_button(
                    label="Unduh Data Lengkap Missing in LinkAja/Alfred (Excel)",
                    data=excel_data_linkaja,
                    file_name=f"Full_Missing_in_LinkAja_Alfred_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.warning("Tidak ada data lengkap untuk nomor dari NGRS yang hilang di LinkAja/Alfred.")

if __name__ == "__main__":
    main()
