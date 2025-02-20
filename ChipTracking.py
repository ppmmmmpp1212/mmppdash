import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date
import json

st.set_page_config(page_title="MMPP Chip Tracking", page_icon = "Screenshot 2025-02-20 153019.png", layout="wide")

# Judul aplikasi di tengah menggunakan CSS
st.markdown(
    """
    <h1 style='text-align: center;'>MMPP CHIP TRACKING</h1>
    """,
    unsafe_allow_html=True
)
st.markdown("---")
st.markdown("<br><br>", unsafe_allow_html=True)

# Fungsi untuk menginisialisasi BigQuery client dari secrets
def get_bigquery_client():
    try:
        # Ambil kredensial dari secrets
        credentials_json = st.secrets["bigquery"]["credentials"]
        credentials = service_account.Credentials.from_service_account_info(json.loads(credentials_json))
        
        # Inisialisasi BigQuery Client
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
        # Query BigQuery dengan filter berdasarkan kolom pencarian
        query = f"""
        SELECT *
        FROM `alfred-analytics-406004.analytics_alfred.{table_name}`
        WHERE CAST({search_column} AS STRING) LIKE '%{search_term}%'
        """
        
        # Eksekusi query dan konversi ke DataFrame
        df = client.query(query).to_dataframe()
        
        # Pastikan semua kolom dalam format yang sesuai
        for col in df.columns:
            if df[col].dtype == 'int64':
                df[col] = df[col].astype(str)  # Konversi integer ke string
            elif df[col].dtype == 'object':
                df[col] = df[col].fillna('')  # Ganti NaN dengan string kosong
        return df
    
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data dari BigQuery: {e}")
        return None

# Fungsi untuk format Rupiah
def format_rupiah(value):
    return f"{value:,.0f}".replace(",", ".")

# Sidebar untuk semua filter
with st.sidebar:
    # Input No Chip
    st.markdown("**Input No Chip**")
    search_term = st.text_input("Cari berdasarkan NoChip atau NoRS", "", label_visibility="collapsed")
    
    # Garis pemisah
    st.markdown("---")
    
    # Filter Transaksi TopUp LinkAja
    st.markdown("**Filter Transaksi TopUp LinkAja**")
    st.write("Tanggal")
    default_start = date(2023, 1, 1)
    default_end = datetime.now().date()
    linkaja_date_range = st.date_input("Pilih rentang tanggal", [default_start, default_end], key="linkaja_date", label_visibility="collapsed")
    if len(linkaja_date_range) == 2:
        linkaja_start_date, linkaja_end_date = linkaja_date_range
    else:
        linkaja_start_date, linkaja_end_date = default_start, default_end
    
    # Filter Debit di dalam Filter Transaksi TopUp LinkAja
    if search_term:
        with st.spinner("Mengambil data untuk filter Debit..."):
            df_linkaja_temp = fetch_bigquery_data("LinkAjaXPJP", search_term, "NoRS")
            if df_linkaja_temp is not None and not df_linkaja_temp.empty and "InitiateDate" in df_linkaja_temp.columns and "Debit" in df_linkaja_temp.columns:
                df_linkaja_temp["InitiateDate"] = pd.to_datetime(df_linkaja_temp["InitiateDate"])
                df_linkaja_temp = df_linkaja_temp[
                    (df_linkaja_temp["InitiateDate"].dt.date >= linkaja_start_date) & 
                    (df_linkaja_temp["InitiateDate"].dt.date <= linkaja_end_date)
                ]
                if not df_linkaja_temp.empty:
                    df_linkaja_temp["Debit"] = pd.to_numeric(df_linkaja_temp["Debit"], errors='coerce').fillna(0)
                    min_debit = float(df_linkaja_temp["Debit"].min())
                    max_debit = float(df_linkaja_temp["Debit"].max())
                    if min_debit == max_debit:
                        min_debit = float(0 if min_debit > 0 else min_debit - 1)
                        max_debit = float(max_debit + 1)
                    st.write("Debit")
                    debit_range = st.slider(
                        "Pilih Rentang Debit", 
                        min_debit, 
                        max_debit, 
                        (min_debit, max_debit), 
                        format="Rp %s",
                        key="debit_filter",
                        label_visibility="collapsed"
                    )
                else:
                    st.write("Debit")
                    debit_range = st.slider(
                        "Pilih Rentang Debit", 
                        0.0, 
                        1000.0, 
                        (0.0, 1000.0), 
                        format="Rp %s",
                        key="debit_filter",
                        label_visibility="collapsed",
                        disabled=True
                    )
            else:
                st.write("Debit")
                debit_range = st.slider(
                    "Pilih Rentang Debit", 
                    0.0, 
                    1000.0, 
                    (0.0, 1000.0), 
                    format="Rp %s",
                    key="debit_filter",
                    label_visibility="collapsed",
                    disabled=True
                )
    else:
        st.write("Debit")
        debit_range = st.slider(
            "Pilih Rentang Debit", 
            0.0, 
            1000.0, 
            (0.0, 1000.0), 
            format="Rp %s",
            key="debit_filter",
            label_visibility="collapsed",
            disabled=True
        )
    
    # Garis pemisah
    st.markdown("---")
    
    # Filter Transaksi pjpnonpjp
    st.markdown("**Filter Transaksi pjpnonpjp**")
    st.write("Tanggal")
    ngrs_date_range = st.date_input("Pilih rentang tanggal", [default_start, default_end], key="ngrs_date", label_visibility="collapsed")
    if len(ngrs_date_range) == 2:
        ngrs_start_date, ngrs_end_date = ngrs_date_range
    else:
        ngrs_start_date, ngrs_end_date = default_start, default_end

# Main App Logic
def main():
    # Definisikan variabel dari sidebar di awal untuk digunakan di scorecard
    if search_term:
        # Filter TransactionType (default semua jika belum dipilih)
        with st.sidebar:
            df_all_temp = fetch_bigquery_data("All_pjpnonpjp", search_term, "NoChip")
            if df_all_temp is not None and not df_all_temp.empty and "TransactionType" in df_all_temp.columns:
                transaction_types = df_all_temp["TransactionType"].unique().tolist()
                selected_transaction_types = st.multiselect(
                    "Pilih TransactionType", 
                    transaction_types, 
                    default=transaction_types, 
                    key="transaction_type_filter",
                    label_visibility="collapsed"
                )
            else:
                selected_transaction_types = []

            # Filter SpendAmount (default sementara, akan diperbarui nanti)
            if df_all_temp is not None and not df_all_temp.empty and "SpendAmount" in df_all_temp.columns:
                df_all_temp["SpendAmount"] = pd.to_numeric(df_all_temp["SpendAmount"], errors='coerce').fillna(0)
                min_spend = float(df_all_temp["SpendAmount"].min())
                max_spend = float(df_all_temp["SpendAmount"].max())
                if min_spend == max_spend:
                    min_spend = float(0 if min_spend > 0 else min_spend - 1)
                    max_spend = float(max_spend + 1)
                spend_range = st.slider(
                    "Pilih Rentang SpendAmount", 
                    min_spend, 
                    max_spend, 
                    (min_spend, max_spend), 
                    format="Rp %s",
                    key="spend_amount_filter",
                    label_visibility="collapsed"
                )
            else:
                spend_range = (0.0, 0.0)
    else:
        selected_transaction_types = []
        spend_range = (0.0, 0.0)

    # Scorecard di atas kedua visualisasi
    if search_term:  # Hanya tampilkan scorecard jika ada input pencarian
        with st.spinner("Mengambil data untuk Scorecard..."):
            df_all = fetch_bigquery_data("All_pjpnonpjp", search_term, "NoChip")
            if df_all is not None and not df_all.empty:
                # Konversi kolom tanggal ke datetime
                if "Completion" in df_all.columns:
                    df_all["Completion"] = pd.to_datetime(df_all["Completion"])
                    # Filter berdasarkan rentang tanggal NGRS
                    df_all_filtered = df_all[
                        (df_all["Completion"].dt.date >= ngrs_start_date) & 
                        (df_all["Completion"].dt.date <= ngrs_end_date)
                    ]

                    if not df_all_filtered.empty:
                        # Filter untuk TransactionType (jika ada)
                        if "TransactionType" in df_all_filtered.columns and selected_transaction_types:
                            df_all_filtered = df_all_filtered[df_all_filtered["TransactionType"].isin(selected_transaction_types)]

                        # Filter untuk SpendAmount (jika ada)
                        if "SpendAmount" in df_all_filtered.columns:
                            df_all_filtered["SpendAmount"] = pd.to_numeric(df_all_filtered["SpendAmount"], errors='coerce').fillna(0)
                            df_all_filtered = df_all_filtered[
                                (df_all_filtered["SpendAmount"] >= spend_range[0]) & 
                                (df_all_filtered["SpendAmount"] <= spend_range[1])
                            ]

                        if not df_all_filtered.empty:
                            # Tampilkan scorecard dengan styling
                            st.markdown(
                                """
                                <h3 style='text-align: center;'>Ringkasan Data NGRS</h3>
                                """,
                                unsafe_allow_html=True
                            )
                            st.markdown("<br>", unsafe_allow_html=True)  # Satu baris kosong sebelum scorecard

                            # Buat container CSS untuk scorecard dengan bayangan dan jarak
                            st.markdown(
                                """
                                <style>
                                .scorecard-container {
                                    display: flex;
                                    justify-content: space-between;
                                    gap: 20px; /* Jarak antar scorecard */
                                    margin-bottom: 20px; /* Jarak antar baris scorecard */
                                }
                                .scorecard {
                                    background-color: #f9f9f9;
                                    border-radius: 8px;
                                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); /* Bayangan */
                                    padding: 15px;
                                    flex: 1;
                                    text-align: center;
                                    min-width: 0; /* Pastikan tidak overflow */
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

                            # Ambil data LinkAjaXPJP untuk Total Debit dan Jumlah Transaksi
                            df_linkaja = fetch_bigquery_data("LinkAjaXPJP", search_term, "NoRS")
                            total_debit = 0  # Default value jika data tidak ada
                            linkaja_transaction_count = 0  # Default value untuk jumlah transaksi LinkAja
                            if df_linkaja is not None and not df_linkaja.empty and "InitiateDate" in df_linkaja.columns and "Debit" in df_linkaja.columns:
                                df_linkaja["InitiateDate"] = pd.to_datetime(df_linkaja["InitiateDate"])
                                df_linkaja_filtered = df_linkaja[
                                    (df_linkaja["InitiateDate"].dt.date >= linkaja_start_date) & 
                                    (df_linkaja["InitiateDate"].dt.date <= linkaja_end_date)
                                ]
                                if not df_linkaja_filtered.empty:
                                    df_linkaja_filtered["Debit"] = pd.to_numeric(df_linkaja_filtered["Debit"], errors='coerce').fillna(0)
                                    total_debit = df_linkaja_filtered["Debit"].sum()
                                    linkaja_transaction_count = len(df_linkaja_filtered)  # Jumlah transaksi dari LinkAja

                            # Baris pertama: 3 scorecard
                            st.markdown(
                                """
                                <div class="scorecard-container">
                                """,
                                unsafe_allow_html=True
                            )
                            col_score1, col_score2, col_score3 = st.columns(3)

                            # Scorecard 1: Daftar OutletID unik
                            outlet_ids = df_all_filtered["OutletID"].dropna().unique().tolist() if "OutletID" in df_all_filtered.columns else []
                            with col_score1:
                                st.markdown(
                                    f"""
                                    <div class="scorecard">
                                        <div class="metric-label">OutletID</div>
                                        <div class="metric-value">{', '.join(map(str, outlet_ids)) if len(outlet_ids) <= 2 else f"{len(outlet_ids)} (Multiple)" or "N/A"}</div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True
                                )

                            # Scorecard 2: Daftar OutletName unik
                            outlet_names = df_all_filtered["OutletName"].dropna().unique().tolist() if "OutletName" in df_all_filtered.columns else []
                            with col_score2:
                                st.markdown(
                                    f"""
                                    <div class="scorecard">
                                        <div class="metric-label">OutletName</div>
                                        <div class="metric-value">{', '.join(map(str, outlet_names)) if len(outlet_names) <= 2 else f"{len(outlet_names)} (Multiple)" or "N/A"}</div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True
                                )

                            # Scorecard 3: Daftar Cluster unik
                            clusters = df_all_filtered["Cluster"].dropna().unique().tolist() if "Cluster" in df_all_filtered.columns else []
                            with col_score3:
                                st.markdown(
                                    f"""
                                    <div class="scorecard">
                                        <div class="metric-label">Cluster</div>
                                        <div class="metric-value">{', '.join(map(str, clusters)) if len(clusters) <= 2 else f"{len(clusters)} (Multiple)" or "N/A"}</div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True
                                )

                            # Tutup container baris pertama
                            st.markdown(
                                """
                                </div>
                                """,
                                unsafe_allow_html=True
                            )

                            # Baris kedua: 4 scorecard
                            st.markdown(
                                """
                                <div class="scorecard-container">
                                """,
                                unsafe_allow_html=True
                            )
                            col_score4, col_score5, col_score6, col_score7 = st.columns(4)

                            # Scorecard 4: Jumlah TransactionAmount (NGRS)
                            transaction_count = len(df_all_filtered["TransactionAmount"]) if "TransactionAmount" in df_all_filtered.columns else 0
                            with col_score4:
                                st.markdown(
                                    f"""
                                    <div class="scorecard">
                                        <div class="metric-label">Jumlah Transaksi NGRS</div>
                                        <div class="metric-value">{transaction_count:,}</div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True
                                )

                            # Scorecard 5: Total SpendAmount
                            total_spend = df_all_filtered["SpendAmount"].sum() if "SpendAmount" in df_all_filtered.columns else 0
                            with col_score5:
                                st.markdown(
                                    f"""
                                    <div class="scorecard">
                                        <div class="metric-label">Total SpendAmount</div>
                                        <div class="metric-value">Rp {format_rupiah(total_spend)}</div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True
                                )

                            # Scorecard 6: Total Debit (dari LinkAjaXPJP)
                            with col_score6:
                                st.markdown(
                                    f"""
                                    <div class="scorecard">
                                        <div class="metric-label">Total Debit (LinkAja)</div>
                                        <div class="metric-value">Rp {format_rupiah(total_debit)}</div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True
                                )

                            # Scorecard 7: Jumlah Transaksi LinkAja (baru)
                            with col_score7:
                                st.markdown(
                                    f"""
                                    <div class="scorecard">
                                        <div class="metric-label">Jumlah Transaksi LinkAja</div>
                                        <div class="metric-value">{linkaja_transaction_count:,}</div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True
                                )

                            # Tutup container baris kedua
                            st.markdown(
                                """
                                </div>
                                """,
                                unsafe_allow_html=True
                            )
                        else:
                            st.warning("Tidak ada data setelah menerapkan filter untuk scorecard.")
                    else:
                        st.warning("Tidak ada data dalam rentang tanggal yang dipilih untuk scorecard.")
                else:
                    st.warning("Kolom 'Completion' tidak ditemukan di data untuk scorecard.")
            else:
                st.warning("Tidak ada data yang cocok untuk scorecard.")
    st.markdown("<br><br>", unsafe_allow_html=True)
    # Membagi layout menjadi dua kolom
    col1, col2 = st.columns(2)

    # Kolom Kiri: Tabel LinkAjaXPJP
    with col1:
        st.markdown(
            """
            <h3 style='text-align: center;'>Transaksi TopUp LinkAja</h3>
            """,
            unsafe_allow_html=True
        )
        
        if search_term:  # Hanya ambil data jika ada input pencarian
            with st.spinner("Mengambil data LinkAjaXPJP..."):
                df_linkaja = fetch_bigquery_data("LinkAjaXPJP", search_term, "NoRS")
                if df_linkaja is not None and not df_linkaja.empty:
                    # Konversi kolom tanggal ke datetime
                    if "InitiateDate" in df_linkaja.columns and "Debit" in df_linkaja.columns:
                        df_linkaja["InitiateDate"] = pd.to_datetime(df_linkaja["InitiateDate"])
                        # Filter berdasarkan rentang tanggal Link Aja
                        df_linkaja_filtered = df_linkaja[
                            (df_linkaja["InitiateDate"].dt.date >= linkaja_start_date) & 
                            (df_linkaja["InitiateDate"].dt.date <= linkaja_end_date)
                        ]

                        if not df_linkaja_filtered.empty:
                            # Filter Debit dari sidebar
                            df_linkaja_filtered["Debit"] = pd.to_numeric(df_linkaja_filtered["Debit"], errors='coerce').fillna(0)
                            df_linkaja_filtered = df_linkaja_filtered[
                                (df_linkaja_filtered["Debit"] >= debit_range[0]) & 
                                (df_linkaja_filtered["Debit"] <= debit_range[1])
                            ]

                            if not df_linkaja_filtered.empty:
                                # Hitung jumlah baris dan total Debit per tanggal
                                df_linkaja_agg = df_linkaja_filtered.groupby(df_linkaja_filtered["InitiateDate"].dt.date).agg(
                                    Count=('InitiateDate', 'size'),
                                    Total_Debit=('Debit', 'sum')
                                ).reset_index()
                                df_linkaja_agg = df_linkaja_agg.sort_values("InitiateDate")

                                # Buat plot dengan dua sumbu Y, format Rupiah untuk Total Debit
                                fig_linkaja = make_subplots(specs=[[{"secondary_y": True}]])
                                fig_linkaja.add_trace(
                                    go.Scatter(
                                        x=df_linkaja_agg["InitiateDate"], 
                                        y=df_linkaja_agg["Count"], 
                                        mode="lines+markers+text", 
                                        name="Jumlah Data",
                                        text=df_linkaja_agg["Count"], 
                                        textposition="top center"
                                    ),
                                    secondary_y=False
                                )
                                fig_linkaja.add_trace(
                                    go.Bar(
                                        x=df_linkaja_agg["InitiateDate"], 
                                        y=df_linkaja_agg["Total_Debit"], 
                                        name="Total Debit (Rp)", 
                                        opacity=0.6,
                                        text=df_linkaja_agg["Total_Debit"].apply(format_rupiah),
                                        textposition="auto"
                                    ),
                                    secondary_y=True
                                )
                                fig_linkaja.update_layout(
                                    xaxis_title="Tanggal",
                                    yaxis_title="Jumlah Data",
                                    yaxis2_title="Total Debit (Rp)",
                                    legend=dict(x=0, y=1.1, orientation="h")
                                )
                                st.plotly_chart(fig_linkaja, use_container_width=True)

                                # Tabel dengan Debit dalam format Rupiah
                                df_linkaja_display = df_linkaja_filtered.copy()
                                if "Debit" in df_linkaja_display.columns:
                                    df_linkaja_display["Debit"] = df_linkaja_display["Debit"].apply(format_rupiah)
                                st.dataframe(df_linkaja_display)
                                st.write(f"Total Data: {len(df_linkaja_filtered)}")
                            else:
                                st.warning("Tidak ada data setelah menerapkan filter Debit.")
                        else:
                            st.warning("Tidak ada data dalam rentang tanggal yang dipilih untuk LinkAjaXPJP.")
                    else:
                        st.warning("Kolom 'InitiateDate' atau 'Debit' tidak ditemukan di data LinkAjaXPJP.")
                else:
                    st.warning("Tidak ada data yang cocok untuk LinkAjaXPJP.")
        else:
            st.info("Masukkan nomor di kolom pencarian di sidebar untuk melihat data LinkAjaXPJP.")

    # Kolom Kanan: Tabel All_pjpnonpjp
    with col2:
        st.markdown(
            """
            <h3 style='text-align: center;'>Transaksi NGRS</h3>
            """,
            unsafe_allow_html=True
        )
        
        if search_term:  # Hanya ambil data jika ada input pencarian
            with st.spinner("Mengambil data All_pjpnonpjp..."):
                df_all = fetch_bigquery_data("All_pjpnonpjp", search_term, "NoChip")
                if df_all is not None and not df_all.empty:
                    # Konversi kolom tanggal ke datetime
                    if "Completion" in df_all.columns:
                        df_all["Completion"] = pd.to_datetime(df_all["Completion"])
                        # Filter berdasarkan rentang tanggal NGRS
                        df_all_filtered = df_all[
                            (df_all["Completion"].dt.date >= ngrs_start_date) & 
                            (df_all["Completion"].dt.date <= ngrs_end_date)
                        ]

                        if not df_all_filtered.empty:
                            # Filter untuk TransactionType (sudah didefinisikan di atas)
                            if "TransactionType" in df_all_filtered.columns and selected_transaction_types:
                                df_all_filtered = df_all_filtered[df_all_filtered["TransactionType"].isin(selected_transaction_types)]

                            # Filter untuk SpendAmount (sudah didefinisikan di atas)
                            if "SpendAmount" in df_all_filtered.columns:
                                df_all_filtered["SpendAmount"] = pd.to_numeric(df_all_filtered["SpendAmount"], errors='coerce').fillna(0)
                                df_all_filtered = df_all_filtered[
                                    (df_all_filtered["SpendAmount"] >= spend_range[0]) & 
                                    (df_all_filtered["SpendAmount"] <= spend_range[1])
                                ]

                            if not df_all_filtered.empty:
                                # Visualisasi dengan shadow box
                                df_completion = df_all_filtered.groupby(df_all_filtered["Completion"].dt.date).size().reset_index(name="Count")
                                df_completion = df_completion.sort_values("Completion")
                                fig_completion = go.Figure()
                                fig_completion.add_trace(go.Scatter(
                                    x=df_completion["Completion"], 
                                    y=df_completion["Count"], 
                                    mode="lines+markers+text", 
                                    name="Jumlah Data",
                                    text=df_completion["Count"], 
                                    textposition="top center"
                                ))
                                fig_completion.update_layout(
                                    xaxis_title="Tanggal", 
                                    yaxis_title="Jumlah Data"
                                )
                                st.plotly_chart(fig_completion, use_container_width=True)

                                # Tabel dengan SpendAmount dalam format Rupiah
                                df_display = df_all_filtered.copy()
                                if "SpendAmount" in df_display.columns:
                                    df_display["SpendAmount"] = df_display["SpendAmount"].apply(format_rupiah)
                                st.dataframe(df_display)
                                st.write(f"Total Data: {len(df_all_filtered)}")
                            else:
                                st.warning("Tidak ada data setelah menerapkan filter TransactionType atau SpendAmount.")
                        else:
                            st.warning("Tidak ada data dalam rentang tanggal yang dipilih untuk All_pjpnonpjp.")
                    else:
                        st.warning("Kolom 'Completion' tidak ditemukan di data All_pjpnonpjp.")
                else:
                    st.warning("Tidak ada data yang cocok untuk All_pjpnonpjp.")
        else:
            st.info("Masukkan nomor di kolom pencarian di sidebar untuk melihat data All_pjpnonpjp.")

# Jalankan aplikasi
if __name__ == "__main__":
    main()
