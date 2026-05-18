#!/usr/bin/env python3

import json
import os
import threading
import traceback
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText

from weekly_brand_credit_emailer import (
    DEFAULT_API_ENV_FILE,
    DEFAULT_AUTO_OUTPUT_ROOT,
    DEFAULT_INVENTORY_LINKS_FILE,
    DEFAULT_INVENTORY_INPUT_DIR,
    DEFAULT_LINKS_FILE,
    DEFAULT_REPORTS_DIR,
    DEFAULT_SALES_SOURCE,
    DEFAULT_WEEKLY_DRIVE_PARENT,
    WEEKLY_BRAND_EMAILS,
    load_inventory_manifest,
    run_weekly_brand_credit_emailer,
)


CONFIG_FILE = "weekly_brand_credit_emailer_gui.json"


def load_gui_config():
    if not os.path.exists(CONFIG_FILE):
        return {}

    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def save_gui_config(payload):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


class WeeklyBrandCreditEmailerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Weekly Brand Credit Emailer")
        self.root.geometry("1040x820")
        self.root.minsize(960, 720)

        self.style = ttk.Style(self.root)
        if "clam" in self.style.theme_names():
            self.style.theme_use("clam")

        self.reports_dir_var = tk.StringVar(value=DEFAULT_REPORTS_DIR)
        self.links_file_var = tk.StringVar(value=DEFAULT_LINKS_FILE)
        self.inventory_links_file_var = tk.StringVar(value=DEFAULT_INVENTORY_LINKS_FILE)
        self.auto_output_root_var = tk.StringVar(value=DEFAULT_AUTO_OUTPUT_ROOT)
        self.inventory_input_dir_var = tk.StringVar(value=DEFAULT_INVENTORY_INPUT_DIR)
        self.drive_parent_folder_var = tk.StringVar(value=DEFAULT_WEEKLY_DRIVE_PARENT)
        self.env_file_var = tk.StringVar(value=DEFAULT_API_ENV_FILE)
        self.sales_source_var = tk.StringVar(value=DEFAULT_SALES_SOURCE)
        self.test_email_var = tk.StringVar()
        self.no_attachments_var = tk.BooleanVar(value=False)
        self.auto_generate_var = tk.BooleanVar(value=True)
        self.skip_sales_pull_var = tk.BooleanVar(value=False)
        self.skip_inventory_refresh_var = tk.BooleanVar(value=False)
        self.no_drive_upload_var = tk.BooleanVar(value=False)

        self.brand_enabled_vars = {}
        self.brand_link_vars = {}
        self.run_thread = None

        self._load_config()
        self._build_ui()
        self._load_inventory_links_into_form()

    def _load_config(self):
        cfg = load_gui_config()
        self.reports_dir_var.set(cfg.get("reports_dir", DEFAULT_REPORTS_DIR))
        self.links_file_var.set(cfg.get("links_file", DEFAULT_LINKS_FILE))
        self.inventory_links_file_var.set(cfg.get("inventory_links_file", DEFAULT_INVENTORY_LINKS_FILE))
        self.auto_output_root_var.set(cfg.get("auto_output_root", DEFAULT_AUTO_OUTPUT_ROOT))
        self.inventory_input_dir_var.set(cfg.get("inventory_input_dir", DEFAULT_INVENTORY_INPUT_DIR))
        self.drive_parent_folder_var.set(cfg.get("drive_parent_folder", DEFAULT_WEEKLY_DRIVE_PARENT))
        self.env_file_var.set(cfg.get("env_file", DEFAULT_API_ENV_FILE))
        self.sales_source_var.set(cfg.get("sales_source", DEFAULT_SALES_SOURCE))
        self.test_email_var.set(cfg.get("test_email", ""))
        self.no_attachments_var.set(bool(cfg.get("no_attachments", False)))
        self.auto_generate_var.set(bool(cfg.get("auto_generate", True)))
        self.skip_sales_pull_var.set(bool(cfg.get("skip_sales_pull", False)))
        self.skip_inventory_refresh_var.set(bool(cfg.get("skip_inventory_refresh", False)))
        self.no_drive_upload_var.set(bool(cfg.get("no_drive_upload", False)))

        selected = set(cfg.get("selected_brands", [cfg["brand"] for cfg in WEEKLY_BRAND_EMAILS]))
        for brand_cfg in WEEKLY_BRAND_EMAILS:
            self.brand_enabled_vars[brand_cfg["brand"]] = tk.BooleanVar(value=brand_cfg["brand"] in selected)
            self.brand_link_vars[brand_cfg["brand"]] = tk.StringVar()

    def _save_config(self):
        payload = {
            "reports_dir": self.reports_dir_var.get().strip(),
            "links_file": self.links_file_var.get().strip(),
            "inventory_links_file": self.inventory_links_file_var.get().strip(),
            "auto_output_root": self.auto_output_root_var.get().strip(),
            "inventory_input_dir": self.inventory_input_dir_var.get().strip(),
            "drive_parent_folder": self.drive_parent_folder_var.get().strip(),
            "env_file": self.env_file_var.get().strip(),
            "sales_source": self.sales_source_var.get().strip(),
            "test_email": self.test_email_var.get().strip(),
            "no_attachments": self.no_attachments_var.get(),
            "auto_generate": self.auto_generate_var.get(),
            "skip_sales_pull": self.skip_sales_pull_var.get(),
            "skip_inventory_refresh": self.skip_inventory_refresh_var.get(),
            "no_drive_upload": self.no_drive_upload_var.get(),
            "selected_brands": [
                brand_cfg["brand"]
                for brand_cfg in WEEKLY_BRAND_EMAILS
                if self.brand_enabled_vars[brand_cfg["brand"]].get()
            ],
        }
        save_gui_config(payload)

    def _build_ui(self):
        outer = ttk.Frame(self.root, padding=18)
        outer.pack(fill="both", expand=True)
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(4, weight=1)

        header = ttk.Frame(outer)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 14))
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="Weekly Brand Credit Emailer", font=("Helvetica", 18, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(
            header,
            text="Generate Hashish and Treesap weekly deal/inventory reports, upload Drive folders, then preview or send.",
        ).grid(row=1, column=0, sticky="w", pady=(4, 0))

        path_frame = ttk.LabelFrame(outer, text="Paths", padding=12)
        path_frame.grid(row=1, column=0, sticky="ew")
        path_frame.columnconfigure(1, weight=1)

        self._build_path_row(path_frame, 0, "Reports Dir", self.reports_dir_var, self._browse_reports_dir)
        self._build_path_row(path_frame, 1, "links.txt", self.links_file_var, self._browse_links_file)
        self._build_path_row(path_frame, 2, "Inventory JSON", self.inventory_links_file_var, self._browse_inventory_file)
        self._build_path_row(path_frame, 3, "Auto Output Root", self.auto_output_root_var, self._browse_auto_output_root)
        self._build_path_row(path_frame, 4, "Input Exports Dir", self.inventory_input_dir_var, self._browse_inventory_input_dir)
        self._build_path_row(path_frame, 5, ".env File", self.env_file_var, self._browse_env_file)

        brand_frame = ttk.LabelFrame(outer, text="Brands", padding=12)
        brand_frame.grid(row=2, column=0, sticky="ew", pady=(14, 0))
        brand_frame.columnconfigure(2, weight=1)

        ttk.Label(brand_frame, text="Send").grid(row=0, column=0, sticky="w")
        ttk.Label(brand_frame, text="Brand / External Recipient").grid(row=0, column=1, sticky="w", padx=(10, 12))
        ttk.Label(brand_frame, text="Inventory Folder Link").grid(row=0, column=2, sticky="w")

        for idx, brand_cfg in enumerate(WEEKLY_BRAND_EMAILS, start=1):
            brand = brand_cfg["brand"]
            recipient_text = ", ".join(brand_cfg["to"])

            ttk.Checkbutton(
                brand_frame,
                variable=self.brand_enabled_vars[brand],
            ).grid(row=idx, column=0, sticky="w")

            ttk.Label(
                brand_frame,
                text=f"{brand} -> {recipient_text}",
            ).grid(row=idx, column=1, sticky="w", padx=(10, 12), pady=6)

            ttk.Entry(
                brand_frame,
                textvariable=self.brand_link_vars[brand],
            ).grid(row=idx, column=2, sticky="ew", pady=6)

        automation_frame = ttk.LabelFrame(outer, text="Automation", padding=12)
        automation_frame.grid(row=3, column=0, sticky="ew", pady=(14, 0))
        automation_frame.columnconfigure(1, weight=1)

        ttk.Checkbutton(
            automation_frame,
            text="Full automatic: pull/generate reports, upload Drive folders, then email",
            variable=self.auto_generate_var,
        ).grid(row=0, column=0, columnspan=3, sticky="w")

        ttk.Label(automation_frame, text="Sales Source").grid(row=1, column=0, sticky="w", pady=(10, 0))
        ttk.Combobox(
            automation_frame,
            textvariable=self.sales_source_var,
            values=("api", "browser"),
            state="readonly",
            width=12,
        ).grid(row=1, column=1, sticky="w", padx=(10, 0), pady=(10, 0))

        ttk.Label(automation_frame, text="Drive Parent").grid(row=2, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(automation_frame, textvariable=self.drive_parent_folder_var).grid(
            row=2, column=1, columnspan=2, sticky="ew", padx=(10, 0), pady=(10, 0)
        )

        ttk.Checkbutton(
            automation_frame,
            text="Use existing sales exports",
            variable=self.skip_sales_pull_var,
        ).grid(row=3, column=0, sticky="w", pady=(10, 0))
        ttk.Checkbutton(
            automation_frame,
            text="Use existing inventory exports",
            variable=self.skip_inventory_refresh_var,
        ).grid(row=3, column=1, sticky="w", padx=(10, 0), pady=(10, 0))
        ttk.Checkbutton(
            automation_frame,
            text="Do not upload to Drive",
            variable=self.no_drive_upload_var,
        ).grid(row=3, column=2, sticky="w", padx=(10, 0), pady=(10, 0))

        options_frame = ttk.LabelFrame(outer, text="Send Options", padding=12)
        options_frame.grid(row=4, column=0, sticky="nsew", pady=(14, 0))
        options_frame.columnconfigure(1, weight=1)
        options_frame.rowconfigure(2, weight=1)

        ttk.Label(options_frame, text="Test Email").grid(row=0, column=0, sticky="w")
        ttk.Entry(options_frame, textvariable=self.test_email_var).grid(row=0, column=1, sticky="ew", padx=(10, 0))

        ttk.Checkbutton(
            options_frame,
            text="Do not attach the XLSX report",
            variable=self.no_attachments_var,
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(10, 10))

        button_row = ttk.Frame(options_frame)
        button_row.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 10))
        button_row.columnconfigure(4, weight=1)

        self.reload_button = ttk.Button(button_row, text="Reload Saved Links", command=self._load_inventory_links_into_form)
        self.reload_button.grid(row=0, column=0, sticky="w")

        self.preview_button = ttk.Button(button_row, text="Preview", command=lambda: self._start_run(dry_run=True))
        self.preview_button.grid(row=0, column=1, padx=(8, 0))

        self.send_button = ttk.Button(button_row, text="Send Emails", command=lambda: self._start_run(dry_run=False))
        self.send_button.grid(row=0, column=2, padx=(8, 0))

        self.log_widget = ScrolledText(options_frame, height=14, wrap="word")
        self.log_widget.grid(row=3, column=0, columnspan=2, sticky="nsew")
        self.log_widget.configure(state="disabled")

    def _build_path_row(self, parent, row_idx, label, variable, browse_command):
        ttk.Label(parent, text=label).grid(row=row_idx, column=0, sticky="w", pady=6)
        ttk.Entry(parent, textvariable=variable).grid(row=row_idx, column=1, sticky="ew", padx=(10, 10), pady=6)
        ttk.Button(parent, text="Browse", command=browse_command).grid(row=row_idx, column=2, sticky="e", pady=6)

    def _browse_reports_dir(self):
        folder = filedialog.askdirectory()
        if folder:
            self.reports_dir_var.set(folder)

    def _browse_links_file(self):
        path = filedialog.askopenfilename(filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")])
        if path:
            self.links_file_var.set(path)

    def _browse_inventory_file(self):
        path = filedialog.askopenfilename(filetypes=[("JSON Files", "*.json"), ("All Files", "*.*")])
        if path:
            self.inventory_links_file_var.set(path)
            self._load_inventory_links_into_form()

    def _browse_auto_output_root(self):
        folder = filedialog.askdirectory()
        if folder:
            self.auto_output_root_var.set(folder)

    def _browse_inventory_input_dir(self):
        folder = filedialog.askdirectory()
        if folder:
            self.inventory_input_dir_var.set(folder)

    def _browse_env_file(self):
        path = filedialog.askopenfilename(filetypes=[("Env Files", ".env"), ("All Files", "*.*")])
        if path:
            self.env_file_var.set(path)

    def _load_inventory_links_into_form(self):
        manifest = load_inventory_manifest(self.inventory_links_file_var.get().strip())
        folders = manifest.get("folders", {})

        for brand_cfg in WEEKLY_BRAND_EMAILS:
            brand = brand_cfg["brand"]
            folder_info = folders.get(brand_cfg["inventory_folder"], {})
            if isinstance(folder_info, dict):
                self.brand_link_vars[brand].set(folder_info.get("link", ""))
            elif isinstance(folder_info, str):
                self.brand_link_vars[brand].set(folder_info)
            else:
                self.brand_link_vars[brand].set("")

        self._append_log(f"Loaded inventory links from {self.inventory_links_file_var.get().strip() or DEFAULT_INVENTORY_LINKS_FILE}")

    def _append_log(self, message):
        self.log_widget.configure(state="normal")
        self.log_widget.insert("end", f"{message}\n")
        self.log_widget.see("end")
        self.log_widget.configure(state="disabled")

    def _threadsafe_log(self, message):
        self.root.after(0, lambda msg=message: self._append_log(msg))

    def _set_busy(self, busy):
        state = "disabled" if busy else "normal"
        self.reload_button.configure(state=state)
        self.preview_button.configure(state=state)
        self.send_button.configure(state=state)

    def _start_run(self, dry_run):
        if self.run_thread and self.run_thread.is_alive():
            messagebox.showwarning("Busy", "A send is already in progress.")
            return

        selected_brands = [
            brand_cfg["brand"]
            for brand_cfg in WEEKLY_BRAND_EMAILS
            if self.brand_enabled_vars[brand_cfg["brand"]].get()
        ]
        if not selected_brands:
            messagebox.showwarning("No Brands Selected", "Select at least one brand.")
            return

        reports_dir = self.reports_dir_var.get().strip()
        links_file = self.links_file_var.get().strip()
        inventory_links_file = self.inventory_links_file_var.get().strip()
        auto_generate = self.auto_generate_var.get()
        if not reports_dir:
            reports_dir = DEFAULT_REPORTS_DIR
        if not links_file:
            links_file = DEFAULT_LINKS_FILE
        if not inventory_links_file:
            inventory_links_file = DEFAULT_INVENTORY_LINKS_FILE

        if not auto_generate and (not reports_dir or not links_file or not inventory_links_file):
            messagebox.showerror("Missing Paths", "Reports directory, links.txt, and inventory JSON are required.")
            return

        if auto_generate:
            if not self.auto_output_root_var.get().strip() or not self.inventory_input_dir_var.get().strip():
                messagebox.showerror("Missing Automation Paths", "Auto output root and input exports directory are required.")
                return
            if self.sales_source_var.get().strip() == "api" and not self.env_file_var.get().strip():
                messagebox.showerror("Missing .env", "An .env file is required when Sales Source is api.")
                return
            if not self.drive_parent_folder_var.get().strip() and not self.no_drive_upload_var.get():
                messagebox.showerror("Missing Drive Parent", "Drive parent folder is required unless Drive upload is disabled.")
                return

        inventory_overrides = {}
        if not auto_generate or self.no_drive_upload_var.get():
            for brand_cfg in WEEKLY_BRAND_EMAILS:
                brand = brand_cfg["brand"]
                link = self.brand_link_vars[brand].get().strip()
                if link:
                    inventory_overrides[brand_cfg["inventory_folder"]] = link

        self._save_config()
        self._append_log("")
        self._append_log(f"Starting {'preview' if dry_run else 'send'} run...")
        self._set_busy(True)

        self.run_thread = threading.Thread(
            target=self._run_worker,
            kwargs={
                "selected_brands": selected_brands,
                "reports_dir": reports_dir,
                "links_file": links_file,
                "inventory_links_file": inventory_links_file,
                "inventory_overrides": inventory_overrides,
                "dry_run": dry_run,
                "test_email": self.test_email_var.get().strip() or None,
                "no_attachments": self.no_attachments_var.get(),
                "auto_generate": auto_generate,
                "sales_source": self.sales_source_var.get().strip() or DEFAULT_SALES_SOURCE,
                "env_file": self.env_file_var.get().strip() or DEFAULT_API_ENV_FILE,
                "auto_output_root": self.auto_output_root_var.get().strip() or DEFAULT_AUTO_OUTPUT_ROOT,
                "inventory_input_dir": self.inventory_input_dir_var.get().strip() or DEFAULT_INVENTORY_INPUT_DIR,
                "skip_sales_pull": self.skip_sales_pull_var.get(),
                "skip_inventory_refresh": self.skip_inventory_refresh_var.get(),
                "include_inventory_order_reports": True,
                "include_inventory_cost": True,
                "no_drive_upload": self.no_drive_upload_var.get(),
                "drive_parent_folder": self.drive_parent_folder_var.get().strip() or DEFAULT_WEEKLY_DRIVE_PARENT,
            },
            daemon=True,
        )
        self.run_thread.start()

    def _run_worker(self, **kwargs):
        try:
            result = run_weekly_brand_credit_emailer(
                prompt_for_missing=False,
                status_callback=self._threadsafe_log,
                **kwargs,
            )
            self.root.after(0, lambda: self._finish_run(result, kwargs["dry_run"]))
        except Exception:
            error_text = traceback.format_exc()
            self.root.after(0, lambda: self._fail_run(error_text))

    def _finish_run(self, result, dry_run):
        self._set_busy(False)
        self._load_inventory_links_into_form()

        sends = result.get("sends", 0)
        failures = result.get("failures", [])
        if failures and sends == 0:
            messagebox.showwarning("No Emails Sent", "\n".join(failures))
            return

        mode = "Preview" if dry_run else "Send"
        summary = f"{mode} finished.\n\nSuccessful emails: {sends}"
        prepare = result.get("prepare") or {}
        if prepare:
            summary += f"\n\nWeek: {prepare.get('week_start')} to {prepare.get('week_end')}"
            summary += f"\nReports: {result.get('reports_dir')}"
            summary += f"\nLinks: {result.get('links_file')}"
        if failures:
            summary += "\n\nSkipped:\n" + "\n".join(failures)
        messagebox.showinfo("Completed", summary)

    def _fail_run(self, error_text):
        self._set_busy(False)
        self._append_log(error_text)
        messagebox.showerror("Error", error_text)


def launch_app():
    root = tk.Tk()
    WeeklyBrandCreditEmailerApp(root)
    root.mainloop()


if __name__ == "__main__":
    launch_app()
