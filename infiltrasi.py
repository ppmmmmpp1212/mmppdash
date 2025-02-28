import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, date
import json
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO

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

# Fungsi untuk mengambil data agregat dari BigQuery (untuk scorecard dan tabel)
def fetch_aggregate_data(table_name, count_column, sum_column, date_column, start_date, end_date, 
                        cluster_column, selected_clusters, transaction_scenario, filter_column, filter_not_zero):
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()

    try:
        query = f"""
        SELECT 
            {cluster_column},
            COUNT({count_column}) AS row_count,
            COALESCE(SUM(CAST({sum_column} AS FLOAT64)), 0) AS total_sum
        FROM `alfred-analytics-406004.analytics_alfred.{table_name}`
        WHERE TransactionScenario = '{transaction_scenario}'
        AND DATE({date_column}) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        AND {cluster_column} IN ({', '.join([str(cluster) for cluster in selected_clusters])})
        AND CAST({filter_column} AS FLOAT64) != 0
        GROUP BY {cluster_column}
        """
        
        job_config = bigquery.QueryJobConfig(use_query_cache=True, priority=bigquery.QueryPriority.INTERACTIVE)
        df = client.query(query, job_config=job_config).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data: {e}")
        return pd.DataFrame()

# Fungsi untuk format Rupiah
def format_rupiah(value):
    return f"{value:,.0f}".replace(",", ".")

# Fungsi untuk mengambil data CounterParty (untuk grafik treemap/bubble)
def fetch_counterparty_data(table_name, date_column, start_date, end_date, cluster_column, selected_clusters, transaction_scenario):
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()

    try:
        query = f"""
        SELECT 
            CounterParty,
            COUNT(*) AS transaction_count,
            COALESCE(SUM(CAST(Debit AS FLOAT64)), 0) AS total_debit
        FROM `alfred-analytics-406004.analytics_alfred.{table_name}`
        WHERE TransactionScenario = '{transaction_scenario}'
        AND DATE({date_column}) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        AND {cluster_column} IN ({', '.join([str(cluster) for cluster in selected_clusters])})
        AND CAST(Debit AS FLOAT64) != 0
        GROUP BY CounterParty
        HAVING total_debit > 0
        """
        
        job_config = bigquery.QueryJobConfig(use_query_cache=True, priority=bigquery.QueryPriority.INTERACTIVE)
        df = client.query(query, job_config=job_config).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data CounterParty: {e}")
        return pd.DataFrame()

# Fungsi untuk mengambil data mentah dari tabel BigQuery (untuk download)
def fetch_raw_data(table_name, date_column, start_date, end_date, cluster_column, selected_clusters, transaction_scenario):
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()

    try:
        query = f"""
        SELECT *
        FROM `alfred-analytics-406004.analytics_alfred.{table_name}`
        WHERE TransactionScenario = '{transaction_scenario}'
        AND DATE({date_column}) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        AND {cluster_column} IN ({', '.join([str(cluster) for cluster in selected_clusters])})
        """
        
        job_config = bigquery.QueryJobConfig(use_query_cache=True, priority=bigquery.QueryPriority.INTERACTIVE)
        df = client.query(query, job_config=job_config).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data mentah: {e}")
        return pd.DataFrame()

# Fungsi untuk mengambil data timeseries (jumlah transaksi)
def fetch_timeseries_data(table_name, date_column, start_date, end_date, cluster_column, selected_clusters, transaction_scenario):
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()

    try:
        query = f"""
        SELECT 
            DATE({date_column}) AS date,
            COUNT(CASE WHEN CAST(Credit AS FLOAT64) != 0 THEN 1 END) AS total_out_cluster,
            COUNT(CASE WHEN CAST(Debit AS FLOAT64) != 0 THEN 1 END) AS total_in_cluster
        FROM `alfred-analytics-406004.analytics_alfred.{table_name}`
        WHERE TransactionScenario = '{transaction_scenario}'
        AND DATE({date_column}) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        AND {cluster_column} IN ({', '.join([str(cluster) for cluster in selected_clusters])})
        GROUP BY DATE({date_column})
        ORDER BY DATE({date_column})
        """
        
        job_config = bigquery.QueryJobConfig(use_query_cache=True, priority=bigquery.QueryPriority.INTERACTIVE)
        df = client.query(query, job_config=job_config).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data timeseries: {e}")
        return pd.DataFrame()

# Fungsi baru untuk mengambil data timeseries (nilai transaksi)
def fetch_timeseries_value_data(table_name, date_column, start_date, end_date, cluster_column, selected_clusters, transaction_scenario):
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()

    try:
        query = f"""
        SELECT 
            DATE({date_column}) AS date,
            COALESCE(SUM(CASE WHEN CAST(Credit AS FLOAT64) != 0 THEN CAST(Credit AS FLOAT64) ELSE 0 END), 0) AS value_out_cluster,
            COALESCE(SUM(CASE WHEN CAST(Debit AS FLOAT64) != 0 THEN CAST(Debit AS FLOAT64) ELSE 0 END), 0) AS value_in_cluster
        FROM `alfred-analytics-406004.analytics_alfred.{table_name}`
        WHERE TransactionScenario = '{transaction_scenario}'
        AND DATE({date_column}) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        AND {cluster_column} IN ({', '.join([str(cluster) for cluster in selected_clusters])})
        GROUP BY DATE({date_column})
        ORDER BY DATE({date_column})
        """
        
        job_config = bigquery.QueryJobConfig(use_query_cache=True, priority=bigquery.QueryPriority.INTERACTIVE)
        df = client.query(query, job_config=job_config).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data timeseries nilai: {e}")
        return pd.DataFrame()

# Fungsi untuk mengonversi DataFrame ke Excel dengan penanganan timezone
def to_excel(df):
    df_copy = df.copy()
    for column in df_copy.columns:
        if pd.api.types.is_datetime64_any_dtype(df_copy[column]):
            df_copy[column] = df_copy[column].dt.tz_localize(None)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_copy.to_excel(writer, sheet_name='Raw_Data', index=False)
    return output.getvalue()

# Fungsi utama aplikasi
def main():
    st.markdown("<h1 style='text-align: center;'>Inflitrasi Analysis</h1>", unsafe_allow_html=True)
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
        <div class="title-box">Data Overview Summary</div>
        """,
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown(
        """
        <style>
        .scorecard-container { display: flex; justify-content: space-between; gap: 20px; margin-bottom: 20px; }
        .scorecard { background-color: #f0f8ff; border-radius: 8px; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); padding: 15px; flex: 1; text-align: center; min-width: 0; }
        .metric-label { color: #666; font-size: 14px; margin-bottom: 5px; }
        .metric-value { color: #333; font-size: 18px; font-weight: bold; }
        </style>
        """,
        unsafe_allow_html=True
    )

    default_start = date(2025, 1, 1)
    default_end = datetime.now().date()
    start_date = default_start.strftime('%Y-%m-%d')
    end_date = default_end.strftime('%Y-%m-%d')

    with st.sidebar:
        st.sidebar.markdown("**Filter Data**")
        st.sidebar.markdown("---")
        filter_type = st.sidebar.selectbox("Tipe Filter Tanggal", ["Per Hari", "Rentang Hari"], key="filter_type")
        if filter_type == "Per Hari":
            selected_date = st.sidebar.date_input("Pilih Tanggal", default_end, key="date_single")
            start_date = selected_date.strftime('%Y-%m-%d')
            end_date = selected_date.strftime('%Y-%m-%d')
        else:
            date_range = st.sidebar.date_input("Pilih Rentang Tanggal", [default_start, default_end], key="date_range")
            if len(date_range) == 2:
                start_date = date_range[0].strftime('%Y-%m-%d')
                end_date = date_range[1].strftime('%Y-%m-%d')
            else:
                start_date = default_start.strftime('%Y-%m-%d')
                end_date = default_end.strftime('%Y-%m-%d')

        def fetch_clusters():
            client = get_bigquery_client()
            if client is None:
                return []
            query = "SELECT DISTINCT ClusterID FROM `alfred-analytics-406004.analytics_alfred.alfred_linkaja` WHERE ClusterID IS NOT NULL"
            df = client.query(query).to_dataframe()
            return [int(x) for x in df["ClusterID"].tolist()]

        cluster_ids = fetch_clusters()
        selected_cluster_ids = st.sidebar.multiselect("Pilih ClusterID", cluster_ids, default=cluster_ids, key="cluster_id_filter")

    with st.spinner("Mengambil data..."):
        df_out = fetch_aggregate_data(
            table_name="alfred_linkaja", count_column="*", sum_column="Credit", date_column="InitiateDate",
            start_date=start_date, end_date=end_date, cluster_column="ClusterID", selected_clusters=selected_cluster_ids,
            transaction_scenario="Digipos B2B Transfer", filter_column="Credit", filter_not_zero=True
        )
        if not df_out.empty:
            df_out.columns = ["ClusterID", "total_out_cluster", "value_out_cluster"]

        df_in = fetch_aggregate_data(
            table_name="alfred_linkaja", count_column="*", sum_column="Debit", date_column="InitiateDate",
            start_date=start_date, end_date=end_date, cluster_column="ClusterID", selected_clusters=selected_cluster_ids,
            transaction_scenario="Digipos B2B Transfer", filter_column="Debit", filter_not_zero=True
        )
        if not df_in.empty:
            df_in.columns = ["ClusterID", "total_in_cluster", "value_in_cluster"]

        if not df_out.empty and not df_in.empty:
            df_combined = df_out.merge(df_in, on="ClusterID", how="outer").fillna(0)
        elif not df_out.empty:
            df_combined = df_out
            df_combined["total_in_cluster"] = 0
            df_combined["value_in_cluster"] = 0
        elif not df_in.empty:
            df_combined = df_in
            df_combined["total_out_cluster"] = 0
            df_combined["value_out_cluster"] = 0
        else:
            df_combined = pd.DataFrame(columns=["ClusterID", "total_out_cluster", "total_in_cluster", "value_out_cluster", "value_in_cluster"])

        if "scorecard_data" not in st.session_state or st.session_state["last_filters"] != (start_date, end_date, tuple(selected_cluster_ids)):
            st.session_state["scorecard_data"] = {
                "total_out_cluster": int(df_combined["total_out_cluster"].sum()),
                "total_in_cluster": int(df_combined["total_in_cluster"].sum()),
                "value_out_cluster": float(df_combined["value_out_cluster"].sum()),
                "value_in_cluster": float(df_combined["value_in_cluster"].sum()),
            }
            st.session_state["last_filters"] = (start_date, end_date, tuple(selected_cluster_ids))

        total_out_cluster = int(df_combined["total_out_cluster"].sum())
        total_in_cluster = int(df_combined["total_in_cluster"].sum())
        value_out_cluster = float(df_combined["value_out_cluster"].sum())
        value_in_cluster = float(df_combined["value_in_cluster"].sum())

        st.markdown("""<div class="scorecard-container">""", unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f'<div class="scorecard"><div class="metric-label">Total Transaksi Infiltrasi Out Cluster</div><div class="metric-value">{total_out_cluster:,}</div></div>', unsafe_allow_html=True)
        with col2:
            st.markdown(f'<div class="scorecard"><div class="metric-label">Total Transaksi Infiltrasi In Cluster</div><div class="metric-value">{total_in_cluster:,}</div></div>', unsafe_allow_html=True)
        with col3:
            st.markdown(f'<div class="scorecard"><div class="metric-label">Nilai Transaksi Out Cluster</div><div class="metric-value">Rp {format_rupiah(value_out_cluster)}</div></div>', unsafe_allow_html=True)
        with col4:
            st.markdown(f'<div class="scorecard"><div class="metric-label">Nilai Transaksi In Cluster</div><div class="metric-value">Rp {format_rupiah(value_in_cluster)}</div></div>', unsafe_allow_html=True)
        st.markdown("""</div>""", unsafe_allow_html=True)

        if not df_combined.empty:
            st.markdown("---")
            st.markdown('<div class="title-box">Detail per Cluster</div>', unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)

            df_display = df_combined.copy()
            df_display["value_out_cluster"] = df_display["value_out_cluster"].apply(lambda x: f"Rp {format_rupiah(x)}")
            df_display["value_in_cluster"] = df_display["value_in_cluster"].apply(lambda x: f"Rp {format_rupiah(x)}")
            df_display.columns = ["Cluster ID", "Total Transaksi Out", "Nilai Transaksi Out", "Total Transaksi In", "Nilai Transaksi In"]
            st.dataframe(df_display, use_container_width=True)

            chart_type = st.sidebar.selectbox("Pilih Jenis Grafik", ["Treemap", "Bubble Chart"], key="chart_type")
            st.markdown("---")
            st.markdown('<div class="title-box">Treemap Plot by CounterParty</div>', unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)

            with st.spinner("Mengambil data untuk visualisasi..."):
                df_counterparty = fetch_counterparty_data(
                    table_name="alfred_linkaja", date_column="InitiateDate", start_date=start_date, end_date=end_date,
                    cluster_column="ClusterID", selected_clusters=selected_cluster_ids, transaction_scenario="Digipos B2B Transfer"
                )

                if not df_counterparty.empty:
                    if chart_type == "Bubble Chart":
                        fig = px.scatter(
                            df_counterparty, x="CounterParty", y="total_debit", size="total_debit",
                            hover_data=["CounterParty", "transaction_count", "total_debit"],
                            labels={"CounterParty": "Counter Party", "total_debit": "Total Debit (Rp)", "transaction_count": "Jumlah Transaksi"},
                            title="Infiltrasi In Cluster", height=600
                        )
                        fig.update_traces(marker=dict(color="blue", opacity=0.8, line=dict(width=2, color="white")))
                        fig.update_layout(yaxis_title="Total Debit (Rp)", xaxis_title="Counter Party", showlegend=False)
                    else:
                        fig = px.treemap(
                            df_counterparty, path=["CounterParty"], values="total_debit", color="total_debit",
                            hover_data=["CounterParty", "transaction_count", "total_debit"],
                            labels={"CounterParty": "Counter Party", "total_debit": "Total Debit (Rp)", "transaction_count": "Jumlah Transaksi"},
                            height=600
                        )
                    st.plotly_chart(fig, use_container_width=True)

                    # Timeseries Plot untuk Total Transaksi Infiltrasi
                    st.markdown("---")
                    st.markdown('<div class="title-box">Timeseries Plot - Total Transaksi Infiltrasi</div>', unsafe_allow_html=True)
                    st.markdown("<br>", unsafe_allow_html=True)

                    with st.spinner("Mengambil data untuk timeseries plot..."):
                        df_timeseries = fetch_timeseries_data(
                            table_name="alfred_linkaja", date_column="InitiateDate", start_date=start_date, end_date=end_date,
                            cluster_column="ClusterID", selected_clusters=selected_cluster_ids, transaction_scenario="Digipos B2B Transfer"
                        )

                        if not df_timeseries.empty:
                            fig_timeseries = go.Figure()
                            fig_timeseries.add_trace(
                                go.Scatter(
                                    x=df_timeseries["date"],
                                    y=df_timeseries["total_out_cluster"],
                                    mode="lines+markers+text",
                                    name="Total Transaksi Out Cluster",
                                    line=dict(color="blue"),
                                    marker=dict(size=8),
                                    text=df_timeseries["total_out_cluster"],
                                    textposition="top center",
                                    textfont=dict(size=10)
                                )
                            )
                            fig_timeseries.add_trace(
                                go.Scatter(
                                    x=df_timeseries["date"],
                                    y=df_timeseries["total_in_cluster"],
                                    mode="lines+markers+text",
                                    name="Total Transaksi In Cluster",
                                    line=dict(color="orange"),
                                    marker=dict(size=8),
                                    text=df_timeseries["total_in_cluster"],
                                    textposition="top center",
                                    textfont=dict(size=10)
                                )
                            )
                            fig_timeseries.update_layout(
                                title="Total Transaksi Infiltrasi Out dan In Cluster",
                                xaxis_title="Tanggal",
                                yaxis_title="Jumlah Transaksi",
                                height=600,
                                legend=dict(x=0, y=1.1, orientation="h"),
                                hovermode="x unified"
                            )
                            st.plotly_chart(fig_timeseries, use_container_width=True)
                        else:
                            st.warning("Tidak ada data timeseries yang tersedia untuk ditampilkan.")

                    # Timeseries Plot untuk Nilai Transaksi Infiltrasi
                    st.markdown("---")
                    st.markdown('<div class="title-box">Timeseries Plot - Nilai Transaksi Infiltrasi</div>', unsafe_allow_html=True)
                    st.markdown("<br>", unsafe_allow_html=True)

                    with st.spinner("Mengambil data untuk timeseries nilai plot..."):
                        df_timeseries_value = fetch_timeseries_value_data(
                            table_name="alfred_linkaja", date_column="InitiateDate", start_date=start_date, end_date=end_date,
                            cluster_column="ClusterID", selected_clusters=selected_cluster_ids, transaction_scenario="Digipos B2B Transfer"
                        )

                        if not df_timeseries_value.empty:
                            fig_timeseries_value = go.Figure()
                            fig_timeseries_value.add_trace(
                                go.Scatter(
                                    x=df_timeseries_value["date"],
                                    y=df_timeseries_value["value_out_cluster"],
                                    mode="lines+markers+text",
                                    name="Nilai Transaksi Out Cluster",
                                    line=dict(color="blue"),
                                    marker=dict(size=8),
                                    text=df_timeseries_value["value_out_cluster"].apply(lambda x: f"Rp {format_rupiah(x)}"),
                                    textposition="top center",
                                    textfont=dict(size=10)
                                )
                            )
                            fig_timeseries_value.add_trace(
                                go.Scatter(
                                    x=df_timeseries_value["date"],
                                    y=df_timeseries_value["value_in_cluster"],
                                    mode="lines+markers+text",
                                    name="Nilai Transaksi In Cluster",
                                    line=dict(color="orange"),
                                    marker=dict(size=8),
                                    text=df_timeseries_value["value_in_cluster"].apply(lambda x: f"Rp {format_rupiah(x)}"),
                                    textposition="top center",
                                    textfont=dict(size=10)
                                )
                            )
                            fig_timeseries_value.update_layout(
                                title="Nilai Transaksi Infiltrasi Out dan In Cluster",
                                xaxis_title="Tanggal",
                                yaxis_title="Nilai Transaksi (Rp)",
                                height=600,
                                legend=dict(x=0, y=1.1, orientation="h"),
                                hovermode="x unified"
                            )
                            st.plotly_chart(fig_timeseries_value, use_container_width=True)
                        else:
                            st.warning("Tidak ada data timeseries nilai yang tersedia untuk ditampilkan.")

                    # Mengambil data mentah untuk download
                    with st.spinner("Mengambil data mentah untuk download..."):
                        df_raw = fetch_raw_data(
                            table_name="alfred_linkaja", date_column="InitiateDate", start_date=start_date, end_date=end_date,
                            cluster_column="ClusterID", selected_clusters=selected_cluster_ids, transaction_scenario="Digipos B2B Transfer"
                        )

                        if not df_raw.empty:
                            st.markdown("<br>", unsafe_allow_html=True)
                            excel_data = to_excel(df_raw)
                            st.download_button(
                                label="Download Data Mentah sebagai Excel",
                                data=excel_data,
                                file_name=f"Raw_Infiltrasi_Data_{start_date}_to_{end_date}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        else:
                            st.warning("Tidak ada data mentah yang tersedia untuk diunduh.")
                else:
                    st.warning("Tidak ada data CounterParty yang tersedia untuk ditampilkan dalam grafik.")

if __name__ == "__main__":
    main()