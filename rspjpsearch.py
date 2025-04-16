# rspjpsearch.py
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import json

# Styling untuk tampilan scorecard yang menarik
st.markdown("""
    <style>
    .main-title {
        font-size: 2.5em;
        color: #2E86C1;
        text-align: center;
        margin-bottom: 20px;
    }
    .search-box {
        background-color: #F8F9F9;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        margin-bottom: 20px;
    }
    .scorecard {
        background-color: #FFFFFF;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        margin-bottom: 20px;
        border-left: 5px solid #2E86C1;
    }
    .scorecard-title {
        font-size: 1.5em;
        color: #1A5276;
        margin-bottom: 15px;
        font-weight: bold;
    }
    .scorecard-item {
        display: flex;
        justify-content: space-between;
        padding: 8px 0;
        border-bottom: 1px solid #D6DBDF;
    }
    .scorecard-label {
        font-weight: bold;
        color: #34495E;
        width: 30%;
    }
    .scorecard-value {
        color: #17202A;
        width: 70%;
        word-wrap: break-word;
    }
    </style>
""", unsafe_allow_html=True)

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

# Fungsi untuk mencari data berdasarkan OutletID, NoRS, atau OutletName
@st.cache_data
def search_bigquery_data(outlet_id, no_rs, outlet_name):
    client = get_bigquery_client()
    if client is None:
        return None
    
    try:
        query = """
        SELECT *
        FROM `alfred-analytics-406004.analytics_alfred.PJPRS_Clean`
        WHERE 1=1
        """
        params = {}
        if outlet_id:
            query += " AND OutletID = @outlet_id"
            params["outlet_id"] = outlet_id
        if no_rs:
            query += " AND NoRS = @no_rs"
            params["no_rs"] = no_rs
        if outlet_name:
            query += " AND OutletName LIKE @outlet_name"
            params["outlet_name"] = f"%{outlet_name}%"
        
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter(key, "STRING", value) for key, value in params.items()
        ])
        
        df = client.query(query, job_config=job_config).to_dataframe()
        for col in df.columns:
            if df[col].dtype == 'int64':
                df[col] = df[col].astype(str)
            elif df[col].dtype == 'object':
                df[col] = df[col].fillna('')
        return df
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mencari data dari BigQuery: {e}")
        return None

# Fungsi untuk menampilkan hasil pencarian dalam format scorecard
def display_search_results(df):
    if df is None or df.empty:
        st.warning("Tidak ada data yang ditemukan berdasarkan kriteria pencarian.")
        return
    
    for idx, row in df.iterrows():
        st.markdown('<div class="scorecard">', unsafe_allow_html=True)
        st.markdown(f'<div class="scorecard-title">Data ke-{idx + 1}</div>', unsafe_allow_html=True)
        
        # Menampilkan setiap kolom sebagai item dalam scorecard
        for col in df.columns:
            st.markdown(
                f"""
                <div class="scorecard-item">
                    <div class="scorecard-label">{col}</div>
                    <div class="scorecard-value">{row[col]}</div>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        st.markdown('</div>', unsafe_allow_html=True)

# Main App
def main():
    st.markdown('<div class="main-title">Pencarian Profil Data PJPRS</div>', unsafe_allow_html=True)
    
    # Form Pencarian
    st.markdown('<div class="search-box">', unsafe_allow_html=True)
    st.subheader("Cari Data")
    with st.form(key="search_form"):
        col1, col2, col3 = st.columns(3)
        with col1:
            outlet_id = st.text_input("OutletID")
        with col2:
            no_rs = st.text_input("NoRS")
        with col3:
            outlet_name = st.text_input("OutletName")
        submit_button = st.form_submit_button(label="Cari")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Proses Pencarian
    if submit_button:
        if not outlet_id and not no_rs and not outlet_name:
            st.warning("Masukkan setidaknya satu kriteria pencarian.")
        else:
            with st.spinner("Mencari data di BigQuery..."):
                df = search_bigquery_data(outlet_id, no_rs, outlet_name)
                display_search_results(df)

if __name__ == "__main__":
    main()
