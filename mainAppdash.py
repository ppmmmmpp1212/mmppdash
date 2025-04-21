# main_app.py
import streamlit as st
from streamlit_option_menu import option_menu
from ChipTracking import main as chip_main  # Asumsi ada fungsi main() di ChipTracking.py
from linkajaall import main as linkaja_main  # Asumsi ada fungsi main() di linkajaall.py
from infiltrasi import main as infil_main  # Asumsi ada fungsi main() di linkajaall.py
from rspjpsearch import main as pjp_main

# Fungsi untuk menjalankan aplikasi
def run_app():
    # Konfigurasi halaman
    st.set_page_config(page_title="MMPP Analysis Dash",  layout="wide")

    # Menu sidebar menggunakan streamlit-option-menu
    with st.sidebar:
        selected = option_menu(
            menu_title="Main Menu",  # Judul menu
            options=["Chip Tracking", "Linkaja x NGRS", "Infiltrasi Analysis", "PJP RS Search"],  # Pilihan menu
            icons=["cpu", "wallet", "cpu", "cpu"],  # Ikon untuk setiap opsi
            menu_icon="cast",  # Ikon menu utama
            default_index=0,  # Opsi default yang dipilih
            styles={
                "container": {"padding": "0!important", "background-color": "#fafafa"},
                "icon": {"color": "orange", "font-size": "20px"},
                "nav-link": {
                    "font-size": "16px",
                    "text-align": "left",
                    "margin": "0px",
                    "--hover-color": "#eee",
                },
                "nav-link-selected": {"background-color": "#02ab21"},
            },
        )

    # Logika untuk memilih aplikasi
    if selected == "Chip Tracking":
        
        chip_main()  # Panggil fungsi main dari ChipTracking.py
   
    elif selected == "Linkaja x NGRS":
      
        linkaja_main()  # Panggil fungsi main dari linkajaall.py
   
    elif selected == "Infiltrasi Analysis" :
        infil_main()

    elif selected =="PJP RS Search" :
        pjp_main()

if __name__ == "__main__":
    run_app()
