import tkinter as tk
from tkinter import ttk, messagebox, filedialog, scrolledtext
import threading
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import io
import os

# ---- Scraping Functions ----
COUNTY_URL = "https://resourcespage.pages.dev/pa_cities_counties.csv"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

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

# ---- TKinter UI ----
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Pennsylvania Recovery Specialist Data Tool")
        self.geometry("1100x800")
        self.configure(bg="white")
        self.resizable(True, True)  # <-- Window is now resizable!
        # State
        self.scraper_thread = None
        self.full_output_lines = []
        self.data_files = None

        # --- Header Bar ---
        header = tk.Frame(self, bg="#ea5e64", height=160)
        header.pack(side="top", fill="x")
        tk.Label(
            header, text="Pennsylvania Recovery Specialist Data Tool",
            fg="white", bg="#ea5e64", font=("Arial Black", 38, "bold")
        ).place(relx=0.5, rely=0.5, anchor="center")

        # --- Credential Selection ---
        cred_label = tk.Label(
            self, text="SELECT A CREDENTIAL",
            fg="black", bg="white", font=("Arial", 21, "bold")
        )
        cred_label.pack(pady=(25,10))

        btn_frame = tk.Frame(self, bg="white")
        btn_frame.pack()
        self.cred_btns = []
        for cred in ["CRS", "CFRS", "CRSS"]:
            btn = tk.Button(
                btn_frame, text=f"GET {cred} DATA", width=25, height=2,
                bg="#12b2e8", fg="white", activebackground="black", activeforeground="white",
                font=("Arial", 15, "bold"),
                bd=0, relief="flat",
                command=lambda c=cred: self.start_scrape(c)
            )
            btn.pack(pady=6)
            self.cred_btns.append(btn)

        # --- Status Bar and Show Output ---
        self.status_frame = tk.Frame(self, bg="white")
        self.status_frame.pack(fill="x", pady=(25,0),padx=30)
        self.status_canvas = tk.Canvas(self.status_frame, height=30, bg="#e2e2e2", highlightthickness=0)
        self.status_canvas.pack(side="left", fill="x", expand=True)
        self.status_rect = self.status_canvas.create_rectangle(0,0,0,30,fill="#12b2e8", outline="")
        self.status_label = tk.Label(self.status_frame, bg="white", fg="black", font=("Consolas", 13))
        self.status_label.pack(side="left", padx=10)
        self.show_output_btn = tk.Button(
            self.status_frame, text="SHOW OUTPUT", width=15,
            bg="#12b2e8", fg="white", activebackground="black", activeforeground="white",
            font=("Arial", 12, "bold"), bd=0, relief="flat",
            command=self.toggle_output
        )
        self.show_output_btn.pack(side="right", padx=6)
        self.output_shown = False

        # --- Output / Log ---
        self.output_box = scrolledtext.ScrolledText(self, height=12, font=("Consolas",13), bg="black", fg="white", wrap="word")
        self.output_box.pack(fill="x", padx=30, pady=(10,0))
        self.output_box.configure(state="disabled")
        self.output_box.pack_forget()

        # --- Download Button ---
        self.download_btn = tk.Button(
            self, text="DOWNLOAD DATA",
            width=18, height=2,
            bg="#ea5e64", fg="white", activebackground="black", activeforeground="white",
            font=("Arial", 16, "bold"), bd=0, relief="flat",
            command=self.download_data
        )
        self.download_btn.pack(pady=(30,0))
        self.download_btn.pack_forget()

    def start_scrape(self, cred):
        # Reset UI
        self.full_output_lines = []
        self.output_box.configure(state="normal")
        self.output_box.delete("1.0", "end")
        self.output_box.configure(state="disabled")
        self.output_box.pack_forget()
        self.download_btn.pack_forget()
        self.status_label.config(text="")
        self.set_progress(0)
        for btn in self.cred_btns:
            btn.config(state="disabled")
        self.show_output_btn.config(text="SHOW OUTPUT")
        self.output_shown = False
        self.data_files = None

        # Start thread
        self.scraper_thread = threading.Thread(target=self.do_scrape, args=(cred,))
        self.scraper_thread.start()

    def set_progress(self, percent):
        w = self.status_canvas.winfo_width()
        if w <= 0: w = 600
        self.status_canvas.coords(self.status_rect, 0,0,max(4,int(w*percent/100)),30)

    def update_status(self, percent, msg):
        self.set_progress(percent)
        self.status_label.config(text=msg)
        self.status_label.update()

    def append_output(self, line):
        self.full_output_lines.append(line)
        if not self.output_shown:
            # Show only last line in status_label
            self.status_label.config(text=line)
        else:
            self.output_box.configure(state="normal")
            self.output_box.insert("end", line + "\n")
            self.output_box.see("end")
            self.output_box.configure(state="disabled")

    def toggle_output(self):
        self.output_shown = not self.output_shown
        if self.output_shown:
            self.show_output_btn.config(text="HIDE OUTPUT")
            self.output_box.pack(fill="x", padx=30, pady=(10,0))
            self.output_box.configure(state="normal")
            self.output_box.delete("1.0", "end")
            self.output_box.insert("end", "\n".join(self.full_output_lines)+"\n")
            self.output_box.see("end")
            self.output_box.configure(state="disabled")
        else:
            self.show_output_btn.config(text="SHOW OUTPUT")
            self.output_box.pack_forget()
            if self.full_output_lines:
                self.status_label.config(text=self.full_output_lines[-1])

    def do_scrape(self, credential_choice):
        try:
            # Progress helper
            def progress_callback(current, total, msg):
                percent = int((current/total)*100)
                self.update_status(percent, msg)
                self.append_output(msg)
            # Scraping
            target_credential = credential_choice
            base_url = f"https://www.pacertboard.org/credential-search?type={target_credential.lower()}"

            city_county_df = get_city_county_df(self.full_output_lines)
            if city_county_df.empty:
                self.append_output("❌ City-county data is required. Exiting.")
                self.scrape_done()
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

            self.data_files = (excel_file, all_csv, filtered_csv)
            self.append_output("✅ Done! All files generated and ready to download.")
            self.update_status(100, "✅ Done! All files generated and ready to download.")
            self.download_btn.pack(pady=(30,0)) 
        except Exception as e:
            self.append_output(f"❌ Error: {e}")
        finally:
            self.scrape_done()

    def scrape_done(self):
        for btn in self.cred_btns:
            btn.config(state="normal")

    def download_data(self):
        if not self.data_files:
            messagebox.showerror("No Data", "No data to download.")
            return
        folder = filedialog.askdirectory(title="Choose folder to save data files")
        if not folder: return
        for f in self.data_files:
            try:
                dest = os.path.join(folder, os.path.basename(f))
                with open(f, "rb") as src, open(dest, "wb") as dst:
                    dst.write(src.read())
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save {f}: {e}")
        messagebox.showinfo("Saved", "Files saved to:\n" + folder)

if __name__ == "__main__":
    app = App()
    app.mainloop()