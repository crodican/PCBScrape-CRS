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
from PyQt5.QtGui import QFont

COUNTY_URL = "https://resourcespage.pages.dev/pa_cities_counties.csv"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

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

def scrape_website(base_url, total_pages, output_lines, progress_callback=None):
    all_data = []
    scrape_index = 0

    for page in range(total_pages):
        url = f"{base_url}&page={page}"
        msg = f"🔍 Scraping page {page + 1} of {total_pages}"
        output_lines.append(msg)
        if progress_callback:
            progress_callback(page+1, total_pages, msg)
        try:
            response = requests.get(url)
            response.raise_for_status()
        except:
            output_lines.append(f"⚠️ Failed to load {url}")
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
            #DownloadBtn {
                background: #ea5e64;
                color: white;
                font-size: 24px;
                font-weight: bold;
                border-radius: 7px;
                min-width: 240px;
                min-height: 48px;
            }
            #DownloadBtn:hover {
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

        # Credential selector
        cred_label = QLabel("Select a Credential", self)
        cred_label.setObjectName("CredLabel")
        cred_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(cred_label)

        btns_widget = QWidget(self)
        btns_layout = QVBoxLayout(btns_widget)
        btns_layout.setAlignment(Qt.AlignHCenter)
        self.btns = []
        for cred in ["CRS", "CFRS", "CRSS"]:
            btn = QPushButton(f"GET {cred} DATA", btns_widget)
            btn.clicked.connect(lambda _, c=cred: self.start_scrape(c))
            btn.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
            self.btns.append(btn)
            btns_layout.addWidget(btn)
        main_layout.addWidget(btns_widget)

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

        # Download Button
        self.download_btn = QPushButton("DOWNLOAD DATA", self)
        self.download_btn.setObjectName("DownloadBtn")
        self.download_btn.clicked.connect(self.download_data)
        self.download_btn.setVisible(False)
        main_layout.addWidget(self.download_btn, alignment=Qt.AlignCenter)

        # State
        self.full_output_lines = []
        self.data_files = None
        self.log_expanded = False

        # Threaded scraping
        self.c = Communicate()
        self.c.progress.connect(self.set_progress)
        self.c.line.connect(self.append_log)
        self.c.done.connect(self.scrape_done)

    def start_scrape(self, cred):
        # Reset UI
        self.full_output_lines = []
        self.log_box.clear()
        self.log_box.setVisible(False)
        self.download_btn.setVisible(False)
        self.progress.setValue(0)
        self.status_line.setText("")
        self.show_output_btn.setText("SHOW OUTPUT")
        self.log_expanded = False
        for btn in self.btns:
            btn.setDisabled(True)
        self.data_files = None
        # Start thread
        threading.Thread(target=self.scrape_worker, args=(cred,), daemon=True).start()

    def scrape_worker(self, credential_choice):
        try:
            # Progress helper
            def progress_callback(current, total, msg):
                self.c.progress.emit(current, total, msg)
                self.c.line.emit(msg)
            target_credential = credential_choice
            base_url = f"https://www.pacertboard.org/credential-search?type={target_credential.lower()}"
            city_county_df = get_city_county_df(self.full_output_lines)
            if city_county_df.empty:
                self.c.line.emit("❌ City-county data is required. Exiting.")
                self.c.done.emit((None, None, None))
                return
            total_pages = get_total_pages(base_url, self.full_output_lines)
            df = scrape_website(base_url, total_pages, self.full_output_lines, progress_callback=progress_callback)
            df['CITY'] = df['CITY'].str.strip().str.lower()
            df = df.merge(city_county_df, how='left', left_on='CITY', right_on='City')
            if 'City' in df.columns:
                df.drop(columns=['City'], inplace=True)
            df = df[df['CREDENTIAL'].str.contains(target_credential, na=False)]
            df.sort_values(by='SCRAPE ORDER', inplace=True)
            # CSV output
            all_csv = f"{target_credential}_all_PA.csv"
            df.to_csv(all_csv, index=False)
            # Filtered by county
            target_counties = [
                'Philadelphia', 'Berks', 'Bucks', 'Chester',
                'Delaware', 'Lancaster', 'Montgomery', 'Schuylkill'
            ]
            county_df = df[df['County'].isin(target_counties)]
            filtered_csv = f"{target_credential}_filtered_by_county.csv"
            county_df.to_csv(filtered_csv, index=False)
            # Excel file with two sheets
            excel_file = f"{target_credential}_output.xlsx"
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='All PA', index=False)
                county_df.to_excel(writer, sheet_name='Selected Counties', index=False)
            self.c.line.emit("✅ Done! All files generated and ready to download.")
            self.c.done.emit((excel_file, all_csv, filtered_csv))
        except Exception as e:
            self.c.line.emit(f"❌ Error: {e}")
            self.c.done.emit((None, None, None))

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
        for btn in self.btns:
            btn.setDisabled(False)
        self.data_files = file_tuple
        if all(file_tuple):
            self.download_btn.setVisible(True)
        else:
            self.download_btn.setVisible(False)

    def download_data(self):
        if not self.data_files or not all(self.data_files):
            return
        folder = QFileDialog.getExistingDirectory(self, "Choose folder to save data files")
        if not folder:
            return
        for f in self.data_files:
            try:
                dest = os.path.join(folder, os.path.basename(f))
                with open(f, "rb") as src, open(dest, "wb") as dst:
                    dst.write(src.read())
            except Exception as e:
                msg = f"Failed to save {f}: {e}"
                self.append_log(msg)
        msg = "Files saved to: " + folder
        self.append_log(msg)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = RecoverySpecialistApp()
    window.show()
    sys.exit(app.exec_())