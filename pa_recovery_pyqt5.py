import sys
import os
import threading
import requests
import pandas as pd
import time
import io
import pickle
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QProgressBar, QTextEdit, QFileDialog, QSizePolicy, QTabWidget,
    QGroupBox, QScrollArea, QFrame, QMenu, QDialog, QMessageBox  # Added QMessageBox
)
from PyQt5.QtCore import Qt, pyqtSignal, QObject
from PyQt5.QtGui import QFont, QIcon

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch

# Cache file names
CACHE_ALL_EXCEL = "cached_all_statewide.xlsx"
CACHE_COUNTY_EXCEL = "cached_selected_counties.xlsx"
CACHE_PICKLE = "cached_cred_data.pkl"
CACHE_META = "cache_metadata.txt"

COUNTY_URL = "https://resourcespage.pages.dev/pa_cities_counties.csv"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

CREDENTIALS = ["CRS", "CFRS", "CRSS"]
SELECTED_COUNTIES = [
    'Philadelphia', 'Berks', 'Bucks', 'Chester',
    'Delaware', 'Lancaster', 'Montgomery', 'Schuylkill'
]
REGION_4_COUNTIES = [c for c in SELECTED_COUNTIES if c != "Philadelphia"]
DELAY_BETWEEN_CREDENTIALS = 10  # seconds

# --- Scraping Functions ---
def get_city_county_df(output_lines):
    try:
        resp = requests.get(COUNTY_URL, headers=HEADERS)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        df['City'] = df['City'].str.strip().str.lower()
        output_lines.append("City-County CSV loaded.")
        return df
    except Exception as e:
        output_lines.append(f"❌ Failed to load city-county data: {e}")
        return pd.DataFrame()

def get_total_pages(base_url, output_lines):
    output_lines.append("📄 Determining total number of pages...")
    try:
        response = requests.get(base_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        output_lines.append(f"❌ Error: {e}")
        return 98
    soup = BeautifulSoup(response.content, 'html.parser')
    last_page_link = soup.find('a', title='Go to last page')
    if last_page_link and 'href' in last_page_link.attrs:
        try:
            return int(last_page_link['href'].split('page=')[-1]) + 1
        except:
            return 98
    return 98

def scrape_website(base_url, total_pages, output_lines, progress_callback=None, cred_tag=""):
    all_data = []
    scrape_index = 0
    for page in range(total_pages):
        url = f"{base_url}&page={page}"
        msg = f"🔍 [{cred_tag}] Scraping page {page + 1} of {total_pages}"
        output_lines.append(msg)
        if progress_callback:
            progress_callback(page+1, total_pages, msg)
        try:
            response = requests.get(url)
            response.raise_for_status()
        except:
            output_lines.append(f"⚠️ [{cred_tag}] Failed to load {url}")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', class_='table table-hover table-striped views-table views-view-table cols-3')
        if not table:
            continue

        rows = table.select('tbody > tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 3:
                continue

            name = cols[0].get_text(strip=True)
            city_raw = cols[1].get_text(strip=True).replace(", PA", "").strip().lower()

            cert_table = cols[2].find('table')
            if cert_table:
                cert_rows = cert_table.select('tbody > tr')
                for cert_row in cert_rows:
                    cert_cols = cert_row.find_all('td')
                    if len(cert_cols) == 5:
                        credential = cert_cols[0].get_text(strip=True)
                        number = cert_cols[1].get_text(strip=True)
                        issue = cert_cols[2].get_text(strip=True)
                        expire = cert_cols[3].get_text(strip=True)
                        status = cert_cols[4].get_text(strip=True)

                        all_data.append({
                            'SCRAPE ORDER': scrape_index,
                            'NAME': name,
                            'CITY': city_raw,
                            'CREDENTIAL': credential,
                            'NUMBER': number,
                            'ISSUE DATE': issue,
                            'EXP DATE': expire,
                            'STATUS': status
                        })
                        scrape_index += 1

        time.sleep(1)
    return pd.DataFrame(all_data)

# --- PyQt Signal Helper ---
class Communicate(QObject):
    progress = pyqtSignal(int, int, str)
    line = pyqtSignal(str)
    done = pyqtSignal(tuple)

# --- Report Window ---
class ReportWindow(QDialog):
    def __init__(self, all_cred_data, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Recovery Specialist Report")
        self.resize(960, 800)
        self.setMinimumSize(800, 600)
        self.setWindowIcon(QIcon("icon.ico"))
        self.all_cred_data = all_cred_data  # dict of {cred: df}

        # Ensure tab_keys is set before build_report_summary
        self.tab_keys = [
            "All", "Philadelphia", "Region 4",
            "Berks", "Bucks", "Chester", "Delaware", "Lancaster", "Montgomery", "Schuylkill"
        ]
        self.report_summary = self.build_report_summary()
        # Main layout
        layout = QVBoxLayout(self)

        # Top: Download button (menu)
        hbox = QHBoxLayout()
        hbox.addStretch()
        self.download_btn = QPushButton("DOWNLOAD REPORT")
        menu = QMenu(self)
        pdf_action = menu.addAction("Download as PDF")
        excel_action = menu.addAction("Download as Excel")
        self.download_btn.setMenu(menu)
        pdf_action.triggered.connect(self.download_pdf)
        excel_action.triggered.connect(self.download_excel)
        hbox.addWidget(self.download_btn)
        layout.addLayout(hbox)

        # Tabs
        self.tabs = QTabWidget(self)
        self.tabs.setDocumentMode(True)
        self.tabs.setTabPosition(QTabWidget.North)
        # Add tabs
        for key in self.tab_keys:
            tab = self.create_county_tab(key)
            self.tabs.addTab(tab, key)
        layout.addWidget(self.tabs)

    def build_report_summary(self):
        now = datetime.now()
        first_month = datetime(now.year-1, 7, 1) if now.month >= 7 else datetime(now.year-2, 7, 1)
        months = []
        dt = datetime(now.year, now.month, 1)
        while dt >= first_month:
            months.append(dt)
            dt -= relativedelta(months=1)
        months = [m for m in months]
        month_labels = [m.strftime("%B %Y") for m in months]

        def get_county_filter(df, tabkey):
            if tabkey == "All":
                return df
            if tabkey == "Region 4":
                return df[df['County'].isin(REGION_4_COUNTIES)]
            if tabkey in SELECTED_COUNTIES:
                return df[df['County'] == tabkey]
            if tabkey == "Philadelphia":
                return df[df['County'] == "Philadelphia"]
            return df

        summary = {}
        for tab in self.tab_keys:
            tab_info = {"total": {}, "months": {}}
            for cred in CREDENTIALS:
                df = self.all_cred_data.get(cred, pd.DataFrame())
                dff = get_county_filter(df, tab)
                tab_info["total"][cred] = len(dff)
            for m, mlabel in zip(months, month_labels):
                minfo = {"issued": {}, "expired": {}}
                for cred in CREDENTIALS:
                    df = self.all_cred_data.get(cred, pd.DataFrame())
                    dff = get_county_filter(df, tab)
                    issued = pd.to_datetime(dff["ISSUE DATE"], errors="coerce")
                    expired = pd.to_datetime(dff["EXP DATE"], errors="coerce")
                    issued_in_month = ((issued.dt.year == m.year) & (issued.dt.month == m.month))
                    expired_in_month = ((expired.dt.year == m.year) & (expired.dt.month == m.month))
                    minfo["issued"][cred] = int(issued_in_month.sum())
                    minfo["expired"][cred] = int(expired_in_month.sum())
                tab_info["months"][mlabel] = minfo
            summary[tab] = tab_info
        self._months_list = month_labels
        return summary

    def create_county_tab(self, tabkey):
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        content = QWidget()
        vbox = QVBoxLayout(content)

        summary = self.report_summary.get(tabkey, {})
        total = summary.get("total", {})
        months = summary.get("months", {})

        total_box = QGroupBox("Total Credentials")
        total_layout = QHBoxLayout(total_box)
        for cred in CREDENTIALS:
            lbl = QLabel(f"<b>{cred}:</b> {total.get(cred,0)}")
            total_layout.addWidget(lbl)
        total_layout.addStretch()
        vbox.addWidget(total_box)

        for mlabel in self._months_list:
            minfo = months.get(mlabel, {})
            card = QGroupBox(f"{mlabel}")
            card_layout = QVBoxLayout(card)
            issued_line = "  ".join([f"<b>{cred}</b>: {minfo.get('issued',{}).get(cred,0)}" for cred in CREDENTIALS])
            issued_lab = QLabel(f"Issued: {issued_line}")
            issued_lab.setStyleSheet("margin-bottom:3px;")
            card_layout.addWidget(issued_lab)
            expired_line = "  ".join([f"<b>{cred}</b>: {minfo.get('expired',{}).get(cred,0)}" for cred in CREDENTIALS])
            hr = QFrame()
            hr.setFrameShape(QFrame.HLine)
            hr.setFrameShadow(QFrame.Sunken)
            card_layout.addWidget(hr)
            expired_lab = QLabel(f"Expired: {expired_line}")
            card_layout.addWidget(expired_lab)
            vbox.addWidget(card)
        vbox.addStretch()
        scroll.setWidget(content)
        return scroll

    def download_pdf(self):
        file_path, _ = QFileDialog.getSaveFileName(self, "Save PDF Report", "", "PDF Files (*.pdf)")
        if not file_path:
            return
        try:
            export_report_to_pdf(self.report_summary, self._months_list, file_path)
        except Exception as e:
            msgbox = QDialog(self)
            v = QVBoxLayout(msgbox)
            v.addWidget(QLabel(f"Failed to export PDF: {e}"))
            msgbox.exec_()

    def download_excel(self):
        file_path, _ = QFileDialog.getSaveFileName(self, "Save Excel Report", "", "Excel Files (*.xlsx)")
        if not file_path:
            return
        try:
            export_report_to_excel(self.report_summary, self._months_list, file_path)
        except Exception as e:
            msgbox = QDialog(self)
            v = QVBoxLayout(msgbox)
            v.addWidget(QLabel(f"Failed to export Excel: {e}"))
            msgbox.exec_()

# --- Export helpers ---
def export_report_to_pdf(report_summary, months_list, file_path):
    c = canvas.Canvas(file_path, pagesize=letter)
    width, height = letter
    left = inch
    top = height - inch
    y = top
    c.setFont("Helvetica-Bold", 18)
    c.drawString(left, y, "Recovery Specialist Credential Report")
    y -= 32
    c.setFont("Helvetica", 12)
    for tab, summary in report_summary.items():
        c.setFont("Helvetica-Bold", 15)
        c.drawString(left, y, f"{tab} County")
        y -= 18
        c.setFont("Helvetica", 12)
        t = summary.get("total", {})
        c.drawString(left, y, f"Total: CRS: {t.get('CRS',0)}   CFRS: {t.get('CFRS',0)}   CRSS: {t.get('CRSS',0)}")
        y -= 18
        for mlabel in months_list:
            m = summary['months'][mlabel]
            c.setFont("Helvetica-Bold", 11)
            c.drawString(left, y, f"{mlabel}:")
            y -= 13
            c.setFont("Helvetica", 10)
            c.drawString(left+12, y, f"Issued - CRS: {m['issued']['CRS']}  CFRS: {m['issued']['CFRS']}  CRSS: {m['issued']['CRSS']}")
            y -= 11
            c.drawString(left+12, y, f"Expired - CRS: {m['expired']['CRS']}  CFRS: {m['expired']['CFRS']}  CRSS: {m['expired']['CRSS']}")
            y -= 15
            if y < inch:
                c.showPage()
                y = top
        y -= 28
        if y < inch:
            c.showPage()
            y = top
    c.save()

def export_report_to_excel(report_summary, months_list, file_path):
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        for tab, summary in report_summary.items():
            rows = []
            t = summary.get("total", {})
            rows.append(["Total", t.get("CRS",0), t.get("CFRS",0), t.get("CRSS",0)])
            rows.append(["", "", "", ""])
            rows.append(["Month", "Issued CRS", "Issued CFRS", "Issued CRSS", "Expired CRS", "Expired CFRS", "Expired CRSS"])
            for mlabel in months_list:
                m = summary['months'][mlabel]
                row = [
                    mlabel,
                    m['issued']['CRS'], m['issued']['CFRS'], m['issued']['CRSS'],
                    m['expired']['CRS'], m['expired']['CFRS'], m['expired']['CRSS']
                ]
                rows.append(row)
            df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name=tab[:30], header=False, index=False)

# --- Main App ---
class RecoverySpecialistApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Pennsylvania Recovery Specialist Data Tool")
        self.resize(1100, 800)
        self.setMinimumSize(700, 600)
        self.setWindowIcon(QIcon("icon.ico"))
        self.setStyleSheet("""
            QWidget { background: #fff; }
            #Header { background: rgb(234, 94, 100); }
            #HeaderLabel { color: white; font-size: 48px; font-weight: bold; font-family: Arial Black; letter-spacing: 2px; background: transparent; }
            QPushButton {
                background-color: #12b2e8;
                color: white;
                font-size: 20px;
                font-weight: bold;
                border: none;
                border-radius: 7px;
                min-width: 210px;
                min-height: 48px;
                max-width: 350px;
                margin-bottom: 12px;
                text-transform: uppercase;
                padding-left: 24px;
                padding-right: 24px;
            }
            QPushButton:hover {
                background-color: #000;
                color: white;
            }
            #DownloadAllBtn, #DownloadCountyBtn {
                background: #ea5e64;
                color: white;
                font-size: 24px;
                font-weight: bold;
                border-radius: 7px;
                min-width: 280px;
                min-height: 48px;
                margin-left: 10px;
                margin-right: 10px;
            }
            #DownloadAllBtn:hover, #DownloadCountyBtn:hover {
                background: #000;
            }
            QLabel#CredLabel {
                color: #000;
                font-size: 21px;
                font-weight: bold;
                margin-top: 24px;
                margin-bottom: 8px;
                letter-spacing: 2px;
                text-transform: uppercase;
            }
            #ShowOutputBtn {
                background: #12b2e8;
                color: white;
                font-size: 17px;
                font-weight: bold;
                border-radius: 7px;
                min-width: 150px;
                min-height: 38px;
                margin-left: 15px;
                margin-right: 10px;
            }
            #ShowOutputBtn:hover {
                background: #000;
            }
            QProgressBar {
                border-radius: 4px;
                background: #e2e2e2;
                height: 32px;
                font-size: 14px;
                font-family: Consolas;
                text-align: left;
            }
            QProgressBar::chunk {
                border-radius: 8px;
                background: #12b2e8;
            }
            QTextEdit#LogBox {
                background: #111;
                color: #fff;
                border-radius: 10px;
                font-size: 16px;
                font-family: Consolas;
                max-height: 320px;
            }
            QLabel#StatusLine {
                color: #000;
                font-size: 16px;
                font-family: Consolas;
            }
        """)

        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(0)
        main_layout.setContentsMargins(0,0,0,0)

        self.header = QWidget(self)
        self.header.setObjectName("Header")
        self.header.setFixedHeight(160)
        header_layout = QHBoxLayout(self.header)
        header_layout.setContentsMargins(0,0,0,0)
        header_label = QLabel("Pennsylvania Recovery Specialist Data Tool", self.header)
        header_label.setObjectName("HeaderLabel")
        header_label.setWordWrap(True)
        header_label.setAlignment(Qt.AlignCenter)
        font = QFont("Arial Black", 40, QFont.Bold)
        header_label.setFont(font)
        header_layout.addWidget(header_label)
        main_layout.addWidget(self.header)

        cred_label = QLabel("This tool will scrape all three credentials: CRS, CFRS, CRSS", self)
        cred_label.setObjectName("CredLabel")
        cred_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(cred_label)

        self.get_data_btn = QPushButton("GET DATA", self)
        self.get_data_btn.clicked.connect(self.start_scrape)
        self.get_data_btn.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        main_layout.addWidget(self.get_data_btn, alignment=Qt.AlignCenter)

        self.status_widget = QWidget(self)
        status_layout = QHBoxLayout(self.status_widget)
        status_layout.setContentsMargins(28, 10, 28, 0)
        self.progress = QProgressBar(self.status_widget)
        self.progress.setValue(0)
        self.progress.setTextVisible(False)
        self.progress.setFixedHeight(32)
        self.progress.setMinimumWidth(100)
        status_layout.addWidget(self.progress, stretch=2)

        self.show_output_btn = QPushButton("SHOW OUTPUT", self.status_widget)
        self.show_output_btn.setObjectName("ShowOutputBtn")
        self.show_output_btn.clicked.connect(self.toggle_log)
        status_layout.addWidget(self.show_output_btn, stretch=0)

        self.status_line = QLabel("", self.status_widget)
        self.status_line.setObjectName("StatusLine")
        self.status_line.setMinimumWidth(100)
        status_layout.addWidget(self.status_line, stretch=2)
        main_layout.addWidget(self.status_widget)

        self.log_box = QTextEdit(self)
        self.log_box.setObjectName("LogBox")
        self.log_box.setReadOnly(True)
        self.log_box.setVisible(False)
        main_layout.addWidget(self.log_box)

        btns_hbox = QHBoxLayout()
        self.download_all_btn = QPushButton("DOWNLOAD STATEWIDE DATA", self)
        self.download_all_btn.setObjectName("DownloadAllBtn")
        self.download_all_btn.clicked.connect(self.download_all)
        self.download_all_btn.setVisible(False)
        btns_hbox.addWidget(self.download_all_btn)

        self.download_county_btn = QPushButton("DOWNLOAD SELECTED COUNTIES DATA", self)
        self.download_county_btn.setObjectName("DownloadCountyBtn")
        self.download_county_btn.clicked.connect(self.download_county)
        self.download_county_btn.setVisible(False)
        btns_hbox.addWidget(self.download_county_btn)

        self.show_report_btn = QPushButton("SHOW REPORT", self)
        self.show_report_btn.setVisible(False)
        self.show_report_btn.clicked.connect(self.show_report_window)
        btns_hbox.addWidget(self.show_report_btn)

        self.download_report_btn = QPushButton("DOWNLOAD REPORT", self)
        self.download_report_btn.setVisible(False)
        self.report_menu = QMenu(self)
        self.download_pdf_action = self.report_menu.addAction("Download as PDF")
        self.download_excel_action = self.report_menu.addAction("Download as Excel")
        self.download_report_btn.setMenu(self.report_menu)
        btns_hbox.addWidget(self.download_report_btn)
        self.download_pdf_action.triggered.connect(self.download_report_as_pdf)
        self.download_excel_action.triggered.connect(self.download_report_as_excel)

        # --- CLEAR CACHE BUTTON WITH CONFIRMATION PROMPT ---
        self.clear_cache_btn = QPushButton("CLEAR CACHE", self)
        self.clear_cache_btn.setObjectName("ClearCacheBtn")
        self.clear_cache_btn.setStyleSheet(
            "background: #f3bd2e; color: black; font-weight: bold; min-width: 180px; min-height: 38px; border-radius: 7px; margin-left: 10px; margin-right: 10px;"
        )
        self.clear_cache_btn.clicked.connect(self.clear_cache)
        btns_hbox.addWidget(self.clear_cache_btn)
        # ---------------------------------------------------

        main_layout.addLayout(btns_hbox)

        self.full_output_lines = []
        self.all_excel_path = None
        self.county_excel_path = None
        self.log_expanded = False
        self.all_cred_data = {}  # {cred: df}
        self.report_summary = None
        self.months_list = None

        self.c = Communicate()
        self.c.progress.connect(self.set_progress)
        self.c.line.connect(self.append_log)
        self.c.done.connect(self.scrape_done)

        self.load_cache()

    def load_cache(self):
        if os.path.exists(CACHE_PICKLE):
            try:
                self.all_cred_data = pd.read_pickle(CACHE_PICKLE)
                self.all_excel_path = CACHE_ALL_EXCEL if os.path.exists(CACHE_ALL_EXCEL) else None
                self.county_excel_path = CACHE_COUNTY_EXCEL if os.path.exists(CACHE_COUNTY_EXCEL) else None
                self.show_report_btn.setVisible(True)
                self.download_report_btn.setVisible(True)
                self.download_all_btn.setVisible(bool(self.all_excel_path))
                self.download_county_btn.setVisible(bool(self.county_excel_path))
                self.append_log("✅ Loaded cached data.")
                if os.path.exists(CACHE_META):
                    with open(CACHE_META, "r") as f:
                        self.status_line.setText(f"Loaded cached data from {f.read().strip()}")
                return True
            except Exception as e:
                self.append_log(f"❌ Failed to load cache: {e}")
        return False

    def clear_cache(self):
        reply = QMessageBox.question(
            self,
            "Confirm Clear Cache",
            "Are you sure you want to clear all cached data? This action cannot be undone.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        if reply != QMessageBox.Yes:
            self.append_log("🟡 Cache clear cancelled by user.")
            return
        removed = []
        for f in [CACHE_PICKLE, CACHE_ALL_EXCEL, CACHE_COUNTY_EXCEL, CACHE_META]:
            try:
                if os.path.exists(f):
                    os.remove(f)
                    removed.append(f)
            except Exception as e:
                self.append_log(f"❌ Error removing {f}: {e}")
        self.all_cred_data = {}
        self.all_excel_path = None
        self.county_excel_path = None
        self.download_all_btn.setVisible(False)
        self.download_county_btn.setVisible(False)
        self.show_report_btn.setVisible(False)
        self.download_report_btn.setVisible(False)
        self.status_line.setText("Cache cleared. Please scrape new data.")
        self.append_log("🧹 Cache cleared.")

    def start_scrape(self):
        self.full_output_lines = []
        self.log_box.clear()
        self.log_box.setVisible(False)
        self.download_all_btn.setVisible(False)
        self.download_county_btn.setVisible(False)
        self.show_report_btn.setVisible(False)
        self.download_report_btn.setVisible(False)
        self.progress.setValue(0)
        self.status_line.setText("")
        self.show_output_btn.setText("SHOW OUTPUT")
        self.log_expanded = False
        self.get_data_btn.setDisabled(True)
        self.all_excel_path = None
        self.county_excel_path = None
        self.all_cred_data = {}
        self.report_summary = None
        self.months_list = None
        threading.Thread(target=self.scrape_worker, daemon=True).start()

    def scrape_worker(self):
        try:
            self.c.line.emit("--- Starting Combined Scrape for CRS, CFRS, CRSS ---")
            def progress_callback(current, total, msg):
                self.c.progress.emit(current, total, msg)
                self.c.line.emit(msg)

            city_county_df = get_city_county_df(self.full_output_lines)
            if city_county_df.empty:
                self.c.line.emit("❌ City-county data is required. Exiting.")
                self.c.done.emit((None, None))
                return

            all_cred_data = {}
            all_cred_county_data = {}

            for idx, cred in enumerate(CREDENTIALS):
                self.c.line.emit(f"=== Scraping credential: {cred} ({idx+1}/3) ===")
                base_url = f"https://www.pacertboard.org/credential-search?type={cred.lower()}"
                total_pages = get_total_pages(base_url, self.full_output_lines)
                df = scrape_website(
                    base_url, total_pages, self.full_output_lines,
                    progress_callback=lambda c, t, m, cred=cred: self.c.progress.emit(
                        int((idx + c/float(t))/len(CREDENTIALS)*100), 100, f"[{cred}] {m}"),
                    cred_tag=cred
                )
                if df.empty:
                    self.c.line.emit(f"⚠️ No data found for {cred}")
                    continue
                df['CITY'] = df['CITY'].str.strip().str.lower()
                df = df.merge(city_county_df, how='left', left_on='CITY', right_on='City')
                if 'City' in df.columns:
                    df.drop(columns=['City'], inplace=True)
                df = df[df['CREDENTIAL'].str.contains(cred, na=False)]
                df.sort_values(by='SCRAPE ORDER', inplace=True)
                all_cred_data[cred] = df

                county_df = df[df['County'].isin(SELECTED_COUNTIES)]
                all_cred_county_data[cred] = county_df

                if idx < len(CREDENTIALS) - 1:
                    self.c.line.emit(f"⏳ Waiting {DELAY_BETWEEN_CREDENTIALS}s before next credential scrape...")
                    time.sleep(DELAY_BETWEEN_CREDENTIALS)

            all_excel = "PA_recovery_specialists_all_statewide.xlsx"
            with pd.ExcelWriter(all_excel, engine='openpyxl') as writer:
                for cred, df in all_cred_data.items():
                    df.to_excel(writer, sheet_name=f"{cred} All PA", index=False)
            county_excel = "PA_recovery_specialists_selected_counties.xlsx"
            with pd.ExcelWriter(county_excel, engine='openpyxl') as writer:
                for cred, df in all_cred_county_data.items():
                    df.to_excel(writer, sheet_name=f"{cred} Selected Counties", index=False)

            self.all_cred_data = all_cred_data

            pd.to_pickle(all_cred_data, CACHE_PICKLE)
            with open(CACHE_META, "w") as f:
                f.write(datetime.now().strftime("%Y-%m-%d %H:%M"))
            os.replace(all_excel, CACHE_ALL_EXCEL)
            os.replace(county_excel, CACHE_COUNTY_EXCEL)

            self.c.line.emit("✅ Done! Both Excel files generated and ready to download.")
            self.c.done.emit((all_excel, county_excel))
        except Exception as e:
            self.c.line.emit(f"❌ Error: {e}")
            self.c.done.emit((None, None))

    def set_progress(self, current, total, msg):
        percent = int((current/total)*100) if total else 0
        self.progress.setValue(percent)
        if not self.log_expanded:
            self.status_line.setText(msg)

    def append_log(self, line):
        self.full_output_lines.append(line)
        if self.log_expanded:
            self.log_box.append(line)
        else:
            self.status_line.setText(line)

    def toggle_log(self):
        self.log_expanded = not self.log_expanded
        if self.log_expanded:
            self.log_box.setVisible(True)
            self.log_box.setPlainText('\n'.join(self.full_output_lines))
            self.log_box.moveCursor(self.log_box.textCursor().End)
            self.show_output_btn.setText("HIDE OUTPUT")
        else:
            self.log_box.setVisible(False)
            self.show_output_btn.setText("SHOW OUTPUT")
            if self.full_output_lines:
                self.status_line.setText(self.full_output_lines[-1])

    def scrape_done(self, file_tuple):
        self.get_data_btn.setDisabled(False)
        self.all_excel_path, self.county_excel_path = file_tuple
        self.download_all_btn.setVisible(bool(self.all_excel_path))
        self.download_county_btn.setVisible(bool(self.county_excel_path))
        self.show_report_btn.setVisible(bool(self.all_cred_data))
        self.download_report_btn.setVisible(bool(self.all_cred_data))
        if os.path.exists(CACHE_META):
            with open(CACHE_META, "r") as f:
                self.status_line.setText(f"Loaded/cached data from {f.read().strip()}")

    def download_all(self):
        self._download_file(self.all_excel_path, "Save statewide data file")

    def download_county(self):
        self._download_file(self.county_excel_path, "Save selected counties data file")

    def _download_file(self, file_path, dialog_title):
        if not file_path or not os.path.exists(file_path):
            self.append_log(f"File not found: {file_path}")
            return
        folder = QFileDialog.getExistingDirectory(self, dialog_title)
        if not folder:
            return
        try:
            dest = os.path.join(folder, os.path.basename(file_path))
            with open(file_path, "rb") as src, open(dest, "wb") as dst:
                dst.write(src.read())
            msg = f"File saved to: {dest}"
            self.append_log(msg)
        except Exception as e:
            msg = f"Failed to save {file_path}: {e}"
            self.append_log(msg)

    def show_report_window(self):
        if not self.all_cred_data:
            return
        dlg = ReportWindow(self.all_cred_data, self)
        dlg.exec_()

    def download_report_as_pdf(self):
        if not self.all_cred_data:
            return
        summary_win = ReportWindow(self.all_cred_data)
        file_path, _ = QFileDialog.getSaveFileName(self, "Save PDF Report", "", "PDF Files (*.pdf)")
        if not file_path:
            return
        export_report_to_pdf(summary_win.report_summary, summary_win._months_list, file_path)

    def download_report_as_excel(self):
        if not self.all_cred_data:
            return
        summary_win = ReportWindow(self.all_cred_data)
        file_path, _ = QFileDialog.getSaveFileName(self, "Save Excel Report", "", "Excel Files (*.xlsx)")
        if not file_path:
            return
        export_report_to_excel(summary_win.report_summary, summary_win._months_list, file_path)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon("icon.ico"))
    window = RecoverySpecialistApp()
    window.show()
    sys.exit(app.exec_())