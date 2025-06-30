import sys
import os
import threading
import requests
import pandas as pd
import time
import io
from bs4 import BeautifulSoup

from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QProgressBar, QTextEdit, QFileDialog, QSizePolicy
)
from PyQt5.QtCore import Qt, pyqtSignal, QObject
from PyQt5.QtGui import QFont, QIcon  # <-- Added QIcon here

COUNTY_URL = "https://resourcespage.pages.dev/pa_cities_counties.csv"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

CREDENTIALS = ["CRS", "CFRS", "CRSS"]
SELECTED_COUNTIES = [
    'Philadelphia', 'Berks', 'Bucks', 'Chester',
    'Delaware', 'Lancaster', 'Montgomery', 'Schuylkill'
]
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

# --- Main App ---
class RecoverySpecialistApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Pennsylvania Recovery Specialist Data Tool")
        self.resize(1100, 800)
        self.setMinimumSize(700, 600)
        self.setStyleSheet("""
            QWidget { background: #fff; }
            #Header { 
                background: rgb(234, 94, 100);
                text-transform: uppercase;
            }
            #HeaderLabel { color: white; font-size: 48px; font-weight: bold; font-family: Arial Black; letter-spacing: 2px; background: transparent; text-transform: uppercase; }
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

        # Layouts
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(0)
        main_layout.setContentsMargins(0,0,0,0)

        # Header
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

        # Credential selector info
        cred_label = QLabel("This tool will scrape all three credentials: CRS, CFRS, CRSS", self)
        cred_label.setObjectName("CredLabel")
        cred_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(cred_label)

        # Get Data Button
        self.get_data_btn = QPushButton("GET DATA", self)
        self.get_data_btn.clicked.connect(self.start_scrape)
        self.get_data_btn.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        main_layout.addWidget(self.get_data_btn, alignment=Qt.AlignCenter)

        # Status/progress and show output
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

        # Output log
        self.log_box = QTextEdit(self)
        self.log_box.setObjectName("LogBox")
        self.log_box.setReadOnly(True)
        self.log_box.setVisible(False)
        main_layout.addWidget(self.log_box)

        # Download Buttons
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

        main_layout.addLayout(btns_hbox)

        # State
        self.full_output_lines = []
        self.all_excel_path = None
        self.county_excel_path = None
        self.log_expanded = False

        # Threaded scraping
        self.c = Communicate()
        self.c.progress.connect(self.set_progress)
        self.c.line.connect(self.append_log)
        self.c.done.connect(self.scrape_done)

    def start_scrape(self):
        # Reset UI
        self.full_output_lines = []
        self.log_box.clear()
        self.log_box.setVisible(False)
        self.download_all_btn.setVisible(False)
        self.download_county_btn.setVisible(False)
        self.progress.setValue(0)
        self.status_line.setText("")
        self.show_output_btn.setText("SHOW OUTPUT")
        self.log_expanded = False
        self.get_data_btn.setDisabled(True)
        self.all_excel_path = None
        self.county_excel_path = None
        # Start thread
        threading.Thread(target=self.scrape_worker, daemon=True).start()

    def scrape_worker(self):
        try:
            self.c.line.emit("--- Starting Combined Scrape for CRS, CFRS, CRSS ---")
            # Progress helper
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

            # Output: write one Excel file per output type, each with three sheets
            all_excel = "PA_recovery_specialists_all_statewide.xlsx"
            with pd.ExcelWriter(all_excel, engine='openpyxl') as writer:
                for cred, df in all_cred_data.items():
                    df.to_excel(writer, sheet_name=f"{cred} All PA", index=False)
            county_excel = "PA_recovery_specialists_selected_counties.xlsx"
            with pd.ExcelWriter(county_excel, engine='openpyxl') as writer:
                for cred, df in all_cred_county_data.items():
                    df.to_excel(writer, sheet_name=f"{cred} Selected Counties", index=False)

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

if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon("icon.ico"))  # <-- This sets the icon for the taskbar/titlebar
    window = RecoverySpecialistApp()
    window.show()
    sys.exit(app.exec_())