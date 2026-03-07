#!/usr/bin/env python3
import json
import queue
import threading
import time
import tkinter as tk
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from tkinter import font as tkfont
from tkinter import messagebox, ttk
from typing import Dict, List, Optional, Sequence, Tuple

import brand_meeting_packet as bmp

try:
    import deals
except Exception:
    deals = None


@dataclass(frozen=True)
class BrandOption:
    name: str
    folder_name: str = ""
    rep: str = ""
    location: str = ""
    days: str = ""
    emails: Tuple[str, ...] = ()
    source: str = "fallback"
    scheduled_today: bool = False


CUSTOM_BRANDS_PATH = Path(__file__).with_name("brand_meeting_gui_custom_brands.json")


class BrandMeetingPacketGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Brand Meeting Packet Studio")
        self.root.geometry("1380x920")
        self.root.minsize(1180, 760)

        self.colors = {
            "bg": "#F3EEE4",
            "card": "#FCF8F0",
            "card_alt": "#F7F1E5",
            "hero": "#193A36",
            "hero_chip": "#244B45",
            "hero_border": "#D4A761",
            "border": "#D7CCBB",
            "text": "#1F2A27",
            "muted": "#5B6965",
            "accent": "#2F6B5D",
            "accent_dark": "#234F45",
            "ghost": "#E7DDCE",
            "ghost_dark": "#DBCFBC",
            "success": "#1F7A4C",
            "warn": "#A5661B",
            "error": "#B54034",
            "log_bg": "#F6F0E5",
            "input_bg": "#FFFFFF",
        }

        self.log_queue: "queue.Queue[tuple[str, object]]" = queue.Queue()
        self.worker_running = False
        self.last_log_message = ""
        self.max_log_rows = 260
        self.selected_brand_names: set[str] = set()
        self.filtered_brand_options: List[BrandOption] = []
        self.brand_jump_buffer = ""
        self.brand_jump_timestamp = 0.0

        self.date_preset_var = tk.StringVar(value="Last 60 days")
        self.custom_start_var = tk.StringVar()
        self.custom_end_var = tk.StringVar()
        self.output_dir_var = tk.StringVar(value=str(bmp.DEFAULT_OUTPUT_ROOT))

        self.include_store_var = tk.BooleanVar(value=True)
        self.include_appendix_var = tk.BooleanVar(value=True)
        self.include_charts_var = tk.BooleanVar(value=True)
        self.include_kickback_var = tk.BooleanVar(value=False)
        self.email_var = tk.BooleanVar(value=True)
        self.xlsx_var = tk.BooleanVar(value=False)
        self.force_refresh_var = tk.BooleanVar(value=False)
        self.brand_search_var = tk.StringVar()
        self.custom_brand_var = tk.StringVar()

        self.status_var = tk.StringVar(value="Ready")
        self.activity_var = tk.StringVar(value="Choose brands and run a packet.")
        self.brand_count_var = tk.StringVar(value="0 brands selected")
        self.store_count_var = tk.StringVar(value="0 stores selected")
        self.window_summary_var = tk.StringVar(value="Last 60 days")
        self.cache_mode_var = tk.StringVar(value="Smart cache reuse on")
        self.window_preview_var = tk.StringVar(value="")
        self.brand_selection_var = tk.StringVar(value="No brands selected")
        self.brand_details_var = tk.StringVar(value="Pick a brand to see details.")
        self.store_summary_var = tk.StringVar(value="")
        self.setup_summary_var = tk.StringVar(value="No brands selected yet.")
        self.selected_queue_var = tk.StringVar(value="Nothing queued yet.")
        self.brand_browser_summary_var = tk.StringVar(value="")

        self.brand_options = self._load_brand_options()
        self.brand_lookup = {item.name: item for item in self.brand_options}
        self.brand_lookup_lower = {item.name.lower(): item for item in self.brand_options}

        self._configure_theme()
        self._build_ui()
        self._bind_events()
        self._set_custom_date_state()
        self._refresh_brand_list()
        self._update_store_summary()
        self._update_brand_details()
        self._update_header_summary()
        self.root.after(120, self._drain_log_queue)

    def _configure_theme(self) -> None:
        self.root.configure(bg=self.colors["bg"])

        title_font = tkfont.Font(family="Helvetica", size=22, weight="bold")
        section_font = tkfont.Font(family="Helvetica", size=13, weight="bold")
        label_font = tkfont.Font(family="Helvetica", size=11)
        small_font = tkfont.Font(family="Helvetica", size=10)
        chip_font = tkfont.Font(family="Helvetica", size=10, weight="bold")

        self.fonts = {
            "title": title_font,
            "section": section_font,
            "label": label_font,
            "small": small_font,
            "chip": chip_font,
        }

        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure(".", font=label_font)
        style.configure("Card.TCheckbutton", background=self.colors["card"], foreground=self.colors["text"])
        style.map("Card.TCheckbutton", background=[("active", self.colors["card"])])
        style.configure(
            "Accent.TButton",
            padding=(14, 10),
            font=("Helvetica", 11, "bold"),
            background=self.colors["accent"],
            foreground="#FFFFFF",
            borderwidth=0,
        )
        style.map(
            "Accent.TButton",
            background=[("active", self.colors["accent_dark"]), ("disabled", self.colors["border"])],
            foreground=[("disabled", "#FBF8F2")],
        )
        style.configure(
            "Secondary.TButton",
            padding=(12, 9),
            font=("Helvetica", 10, "bold"),
            background=self.colors["ghost"],
            foreground=self.colors["text"],
            borderwidth=0,
        )
        style.map(
            "Secondary.TButton",
            background=[("active", self.colors["ghost_dark"]), ("disabled", self.colors["border"])],
            foreground=[("disabled", self.colors["muted"])],
        )
        style.configure(
            "Ghost.TButton",
            padding=(10, 7),
            font=("Helvetica", 10),
            background=self.colors["card_alt"],
            foreground=self.colors["text"],
            borderwidth=0,
        )
        style.map("Ghost.TButton", background=[("active", self.colors["ghost_dark"])])
        style.configure("Studio.TNotebook", background=self.colors["bg"], borderwidth=0, tabmargins=(0, 0, 0, 0))
        style.configure(
            "Studio.TNotebook.Tab",
            padding=(18, 10),
            font=("Helvetica", 10, "bold"),
            background=self.colors["ghost"],
            foreground=self.colors["text"],
            borderwidth=0,
        )
        style.map(
            "Studio.TNotebook.Tab",
            background=[("selected", self.colors["card"]), ("active", self.colors["ghost_dark"])],
            foreground=[("selected", self.colors["accent_dark"])],
        )
        style.configure(
            "TEntry",
            fieldbackground=self.colors["input_bg"],
            foreground=self.colors["text"],
            padding=6,
            bordercolor=self.colors["border"],
            insertcolor=self.colors["text"],
        )
        style.configure(
            "TCombobox",
            fieldbackground=self.colors["input_bg"],
            padding=6,
            bordercolor=self.colors["border"],
        )
        style.configure(
            "TProgressbar",
            troughcolor=self.colors["card_alt"],
            background=self.colors["accent"],
            lightcolor=self.colors["accent"],
            darkcolor=self.colors["accent"],
            bordercolor=self.colors["card_alt"],
        )

    def _make_card(self, parent: tk.Widget, title: str, subtitle: str = "") -> tuple[tk.Frame, tk.Frame, tk.Frame]:
        card = tk.Frame(
            parent,
            bg=self.colors["card"],
            highlightbackground=self.colors["border"],
            highlightthickness=1,
            bd=0,
        )
        header = tk.Frame(card, bg=self.colors["card"])
        header.pack(fill="x", padx=18, pady=(16, 8))
        tk.Label(
            header,
            text=title,
            bg=self.colors["card"],
            fg=self.colors["text"],
            font=self.fonts["section"],
            anchor="w",
        ).pack(anchor="w")
        if subtitle:
            tk.Label(
                header,
                text=subtitle,
                bg=self.colors["card"],
                fg=self.colors["muted"],
                font=self.fonts["small"],
                anchor="w",
            ).pack(anchor="w", pady=(4, 0))
        body = tk.Frame(card, bg=self.colors["card"])
        body.pack(fill="both", expand=True, padx=18, pady=(0, 18))
        return card, header, body

    def _build_ui(self) -> None:
        outer = tk.Frame(self.root, bg=self.colors["bg"], padx=22, pady=18)
        outer.pack(fill="both", expand=True)

        hero = tk.Frame(outer, bg=self.colors["hero"], padx=22, pady=20, highlightthickness=1, highlightbackground=self.colors["hero"])
        hero.pack(fill="x")
        hero.grid_columnconfigure(0, weight=1)

        tk.Label(
            hero,
            text="Brand Meeting Packet Studio",
            bg=self.colors["hero"],
            fg="#F7F2E9",
            font=self.fonts["title"],
            anchor="w",
        ).grid(row=0, column=0, sticky="w")
        tk.Label(
            hero,
            text="A guided workspace for first-time users: choose brands, confirm dates and stores, then run. Saved data is reused automatically unless you force a refresh.",
            bg=self.colors["hero"],
            fg="#D8E1DE",
            font=self.fonts["label"],
            justify="left",
            anchor="w",
            wraplength=720,
        ).grid(row=1, column=0, sticky="w", pady=(6, 0))

        summary = tk.Frame(hero, bg=self.colors["hero"])
        summary.grid(row=0, column=1, rowspan=2, sticky="e")
        self._make_hero_chip(summary, self.status_var, 0, 0)
        self._make_hero_chip(summary, self.brand_count_var, 0, 1)
        self._make_hero_chip(summary, self.store_count_var, 1, 0)
        self._make_hero_chip(summary, self.cache_mode_var, 1, 1)
        self._make_hero_chip(summary, self.window_summary_var, 2, 0, columnspan=2)

        content = tk.Frame(outer, bg=self.colors["bg"])
        content.pack(fill="both", expand=True, pady=(18, 0))

        self.notebook = ttk.Notebook(content, style="Studio.TNotebook")
        self.notebook.pack(fill="both", expand=True)

        self.overview_tab = tk.Frame(self.notebook, bg=self.colors["bg"], padx=6, pady=10)
        self.brands_tab = tk.Frame(self.notebook, bg=self.colors["bg"], padx=6, pady=10)
        self.data_tab = tk.Frame(self.notebook, bg=self.colors["bg"], padx=6, pady=10)
        self.activity_tab = tk.Frame(self.notebook, bg=self.colors["bg"], padx=6, pady=10)

        self.notebook.add(self.overview_tab, text="Overview")
        self.notebook.add(self.brands_tab, text="Brands")
        self.notebook.add(self.data_tab, text="Data & Output")
        self.notebook.add(self.activity_tab, text="Activity")

        self._build_overview_tab(self.overview_tab)
        self._build_brands_tab(self.brands_tab)
        self._build_data_tab(self.data_tab)
        self._build_activity_tab(self.activity_tab)

    def _build_overview_tab(self, parent: tk.Widget) -> None:
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_columnconfigure(1, weight=1)
        parent.grid_rowconfigure(1, weight=1)

        guide_card, _, guide_body = self._make_card(
            parent,
            "Quick Start",
            "If you are new to the tool, move left to right: choose brands, confirm settings, then run from here.",
        )
        guide_card.grid(row=0, column=0, sticky="nsew", padx=(0, 12), pady=(0, 12))
        self._make_step_row(
            guide_body,
            1,
            "Choose one or more brands",
            "Open the Brands tab to search, jump with the keyboard, and queue multiple brands for one batch run.",
            button_text="Open Brands",
            command=lambda: self.notebook.select(self.brands_tab),
        ).pack(fill="x", pady=(0, 10))
        self._make_step_row(
            guide_body,
            2,
            "Check dates, stores, and output",
            "Use the Data & Output tab to set the date window, confirm stores, and decide whether you want charts, appendix pages, XLSX, or force refresh.",
            button_text="Open Data",
            command=lambda: self.notebook.select(self.data_tab),
        ).pack(fill="x", pady=(0, 10))
        self._make_step_row(
            guide_body,
            3,
            "Run the packet",
            "Most of the time, Download + Build + Email is all you need. It only refreshes missing inputs unless force refresh is enabled.",
        ).pack(fill="x")

        snapshot_card, _, snapshot_body = self._make_card(
            parent,
            "Current Setup",
            "This is the exact configuration that will be used when you click run.",
        )
        snapshot_card.grid(row=0, column=1, sticky="nsew", pady=(0, 12))
        snapshot_body.grid_columnconfigure(0, weight=1)
        snapshot_body.grid_columnconfigure(1, weight=1)

        self._make_summary_tile(snapshot_body, "Brands Queued", self.brand_count_var).grid(row=0, column=0, sticky="ew", padx=(0, 8), pady=(0, 8))
        self._make_summary_tile(snapshot_body, "Stores Selected", self.store_count_var).grid(row=0, column=1, sticky="ew", pady=(0, 8))
        self._make_summary_tile(snapshot_body, "Window", self.window_summary_var).grid(row=1, column=0, sticky="ew", padx=(0, 8), pady=(0, 8))
        self._make_summary_tile(snapshot_body, "Cache Mode", self.cache_mode_var).grid(row=1, column=1, sticky="ew", pady=(0, 8))

        tk.Label(
            snapshot_body,
            text="Run summary",
            bg=self.colors["card"],
            fg=self.colors["text"],
            font=self.fonts["small"],
            anchor="w",
        ).grid(row=2, column=0, columnspan=2, sticky="w", pady=(4, 4))
        tk.Label(
            snapshot_body,
            textvariable=self.setup_summary_var,
            bg=self.colors["card_alt"],
            fg=self.colors["muted"],
            font=self.fonts["small"],
            justify="left",
            wraplength=520,
            anchor="w",
            padx=12,
            pady=10,
            highlightbackground=self.colors["border"],
            highlightthickness=1,
        ).grid(row=3, column=0, columnspan=2, sticky="ew")

        actions_card, _, actions_body = self._make_card(
            parent,
            "Run Actions",
            "The main buttons live here so the home tab stays simple for a new user.",
        )
        actions_card.grid(row=1, column=0, sticky="nsew", padx=(0, 12))
        actions_body.grid_columnconfigure(0, weight=1)
        actions_body.grid_columnconfigure(1, weight=1)

        self.btn_all = ttk.Button(
            actions_body,
            text="Download + Build + Email",
            style="Accent.TButton",
            command=self._on_full_run,
        )
        self.btn_all.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 10))

        self.btn_download = ttk.Button(
            actions_body,
            text="Download Data",
            style="Secondary.TButton",
            command=self._on_download_sales,
        )
        self.btn_download.grid(row=1, column=0, sticky="ew", padx=(0, 6), pady=(0, 8))

        self.btn_build = ttk.Button(
            actions_body,
            text="Build Packet",
            style="Secondary.TButton",
            command=self._on_build_pdf,
        )
        self.btn_build.grid(row=1, column=1, sticky="ew", padx=(6, 0), pady=(0, 8))

        self.btn_build_email = ttk.Button(
            actions_body,
            text="Build + Email",
            style="Secondary.TButton",
            command=self._on_build_email_no_download,
        )
        self.btn_build_email.grid(row=2, column=0, columnspan=2, sticky="ew")

        self.progress = ttk.Progressbar(actions_body, mode="indeterminate")
        self.progress.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(14, 8))

        tk.Label(
            actions_body,
            textvariable=self.status_var,
            bg=self.colors["card"],
            fg=self.colors["text"],
            font=self.fonts["chip"],
            anchor="w",
        ).grid(row=4, column=0, columnspan=2, sticky="w")
        tk.Label(
            actions_body,
            textvariable=self.activity_var,
            bg=self.colors["card"],
            fg=self.colors["muted"],
            font=self.fonts["small"],
            justify="left",
            wraplength=520,
            anchor="w",
        ).grid(row=5, column=0, columnspan=2, sticky="w", pady=(6, 0))

        help_card, _, help_body = self._make_card(
            parent,
            "What Each Button Does",
            "Short explanations so you do not have to remember the workflow.",
        )
        help_card.grid(row=1, column=1, sticky="nsew")
        self._make_help_row(
            help_body,
            "Download Data",
            "Creates or fills missing sales inputs for the selected brands and stores. Good when you want to prep files first.",
        ).pack(fill="x", pady=(0, 10))
        self._make_help_row(
            help_body,
            "Build Packet",
            "Uses the saved files already in the selected run folder. Best when the data is already there and you only want a new PDF/XLSX.",
        ).pack(fill="x", pady=(0, 10))
        self._make_help_row(
            help_body,
            "Build + Email",
            "Builds from saved files only, then emails the result. No new download unless you run the full button instead.",
        ).pack(fill="x", pady=(0, 10))
        self._make_help_row(
            help_body,
            "Download + Build + Email",
            "Smart full run. Reuses the current run cache first and only downloads missing data unless Force Refresh is enabled in the Data tab.",
        ).pack(fill="x")

    def _build_brands_tab(self, parent: tk.Widget) -> None:
        parent.grid_columnconfigure(0, weight=3)
        parent.grid_columnconfigure(1, weight=2)
        parent.grid_rowconfigure(0, weight=1)

        brand_card, _, brand_body = self._make_card(
            parent,
            "Brand Browser",
            "Search, multi-select, or use the keyboard to jump directly to a brand. This tab is your queue builder.",
        )
        brand_card.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
        brand_body.grid_rowconfigure(3, weight=1)
        brand_body.grid_columnconfigure(0, weight=1)

        search_row = tk.Frame(brand_body, bg=self.colors["card"])
        search_row.grid(row=0, column=0, sticky="ew")
        search_row.grid_columnconfigure(0, weight=1)

        self.brand_search_entry = ttk.Entry(search_row, textvariable=self.brand_search_var)
        self.brand_search_entry.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        ttk.Button(search_row, text="Today", style="Ghost.TButton", command=self._select_today_brands).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(search_row, text="Visible", style="Ghost.TButton", command=self._select_all_visible_brands).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(search_row, text="Clear", style="Ghost.TButton", command=self._clear_brand_selection).grid(row=0, column=3)

        add_row = tk.Frame(brand_body, bg=self.colors["card"])
        add_row.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        add_row.grid_columnconfigure(0, weight=1)
        self.custom_brand_entry = ttk.Entry(add_row, textvariable=self.custom_brand_var)
        self.custom_brand_entry.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        ttk.Button(add_row, text="Add Brand", style="Secondary.TButton", command=self._add_custom_brand_from_entry).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(add_row, text="Remove Selected", style="Ghost.TButton", command=self._remove_selected_custom_brands).grid(row=0, column=2)
        tk.Label(
            add_row,
            text="Not on the list? Add it here, for example: Papa's Herb. Remove Selected only deletes custom brands added in this GUI.",
            bg=self.colors["card"],
            fg=self.colors["muted"],
            font=self.fonts["small"],
            anchor="w",
        ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(6, 0))

        tk.Label(
            brand_body,
            textvariable=self.brand_browser_summary_var,
            bg=self.colors["card"],
            fg=self.colors["muted"],
            font=self.fonts["small"],
            anchor="w",
        ).grid(row=2, column=0, sticky="w", pady=(10, 8))

        brand_list_wrap = tk.Frame(brand_body, bg=self.colors["card"])
        brand_list_wrap.grid(row=3, column=0, sticky="nsew")
        brand_list_wrap.grid_rowconfigure(0, weight=1)
        brand_list_wrap.grid_columnconfigure(0, weight=1)

        self.brand_list = tk.Listbox(
            brand_list_wrap,
            selectmode=tk.MULTIPLE,
            exportselection=False,
            activestyle="none",
            bg=self.colors["input_bg"],
            fg=self.colors["text"],
            font=self.fonts["label"],
            selectbackground=self.colors["accent"],
            selectforeground="#FFFFFF",
            highlightthickness=1,
            highlightbackground=self.colors["border"],
            relief="flat",
        )
        self.brand_list.grid(row=0, column=0, sticky="nsew")
        brand_scroll = ttk.Scrollbar(brand_list_wrap, orient="vertical", command=self.brand_list.yview)
        brand_scroll.grid(row=0, column=1, sticky="ns")
        self.brand_list.configure(yscrollcommand=brand_scroll.set)

        side = tk.Frame(parent, bg=self.colors["bg"])
        side.grid(row=0, column=1, sticky="nsew")
        side.grid_columnconfigure(0, weight=1)
        side.grid_rowconfigure(0, weight=1)

        queue_card, _, queue_body = self._make_card(
            side,
            "Queued Brands",
            "Everything listed here will run in order with the same date window, stores, and options.",
        )
        queue_card.grid(row=0, column=0, sticky="nsew", pady=(0, 12))
        tk.Label(
            queue_body,
            textvariable=self.selected_queue_var,
            bg=self.colors["card_alt"],
            fg=self.colors["text"],
            font=self.fonts["small"],
            justify="left",
            anchor="nw",
            wraplength=360,
            padx=12,
            pady=10,
            highlightbackground=self.colors["border"],
            highlightthickness=1,
        ).pack(fill="both", expand=True)

        detail_card, _, detail_body = self._make_card(
            side,
            "Brand Details & Shortcuts",
            "Helpful if you only have one brand selected or you are still learning the keyboard controls.",
        )
        detail_card.grid(row=1, column=0, sticky="ew")
        tk.Label(
            detail_body,
            textvariable=self.brand_details_var,
            bg=self.colors["card_alt"],
            fg=self.colors["muted"],
            font=self.fonts["small"],
            justify="left",
            wraplength=360,
            anchor="w",
            padx=12,
            pady=10,
            highlightbackground=self.colors["border"],
            highlightthickness=1,
        ).pack(fill="x", pady=(0, 12))
        self._make_help_row(detail_body, "Ctrl+F", "Focus the brand search box from anywhere in the window.").pack(fill="x", pady=(0, 8))
        self._make_help_row(detail_body, "Type on the list", "Jump to a brand name quickly without reaching for the search box.").pack(fill="x", pady=(0, 8))
        self._make_help_row(detail_body, "Space or Enter", "Toggle the highlighted brand on or off in the queue.").pack(fill="x")

    def _build_data_tab(self, parent: tk.Widget) -> None:
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_columnconfigure(1, weight=1)
        parent.grid_rowconfigure(1, weight=1)

        run_card, _, run_body = self._make_card(
            parent,
            "Date Window & Output Folder",
            "These settings apply to every brand in the queue.",
        )
        run_card.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 12))
        run_body.grid_columnconfigure(1, weight=1)
        run_body.grid_columnconfigure(3, weight=1)

        self._field_label(run_body, "Date Preset").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        self.date_combo = ttk.Combobox(
            run_body,
            textvariable=self.date_preset_var,
            values=["Last 60 days", "Last 30 days", "Custom"],
            state="readonly",
            width=18,
        )
        self.date_combo.grid(row=0, column=1, sticky="ew", pady=(0, 8))

        self._field_label(run_body, "Custom Start").grid(row=0, column=2, sticky="w", padx=(16, 8), pady=(0, 8))
        self.custom_start_entry = ttk.Entry(run_body, textvariable=self.custom_start_var)
        self.custom_start_entry.grid(row=0, column=3, sticky="ew", pady=(0, 8))

        self._field_label(run_body, "Custom End").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        self.custom_end_entry = ttk.Entry(run_body, textvariable=self.custom_end_var)
        self.custom_end_entry.grid(row=1, column=1, sticky="ew", pady=(0, 8))

        self._field_label(run_body, "Output Folder").grid(row=1, column=2, sticky="w", padx=(16, 8), pady=(0, 8))
        ttk.Entry(run_body, textvariable=self.output_dir_var).grid(row=1, column=3, sticky="ew", pady=(0, 8))

        tk.Label(
            run_body,
            textvariable=self.window_preview_var,
            bg=self.colors["card"],
            fg=self.colors["muted"],
            font=self.fonts["small"],
            anchor="w",
        ).grid(row=2, column=0, columnspan=4, sticky="w", pady=(4, 0))

        store_card, _, store_body = self._make_card(
            parent,
            "Stores",
            "Pick the stores to include. Clearing everything now really means no stores selected.",
        )
        store_card.grid(row=1, column=0, sticky="nsew", padx=(0, 12))
        store_body.grid_rowconfigure(2, weight=1)
        store_body.grid_columnconfigure(0, weight=1)

        tk.Label(
            store_body,
            textvariable=self.store_summary_var,
            bg=self.colors["card"],
            fg=self.colors["muted"],
            font=self.fonts["small"],
            anchor="w",
        ).grid(row=0, column=0, sticky="w", pady=(0, 10))

        store_list_wrap = tk.Frame(store_body, bg=self.colors["card"])
        store_list_wrap.grid(row=1, column=0, sticky="nsew")
        store_list_wrap.grid_rowconfigure(0, weight=1)
        store_list_wrap.grid_columnconfigure(0, weight=1)

        self.store_list = tk.Listbox(
            store_list_wrap,
            selectmode=tk.MULTIPLE,
            exportselection=False,
            activestyle="none",
            height=10,
            bg=self.colors["input_bg"],
            fg=self.colors["text"],
            font=self.fonts["label"],
            selectbackground=self.colors["accent"],
            selectforeground="#FFFFFF",
            highlightthickness=1,
            highlightbackground=self.colors["border"],
            relief="flat",
        )
        self.store_list.grid(row=0, column=0, sticky="nsew")
        store_scroll = ttk.Scrollbar(store_list_wrap, orient="vertical", command=self.store_list.yview)
        store_scroll.grid(row=0, column=1, sticky="ns")
        self.store_list.configure(yscrollcommand=store_scroll.set)

        self.store_rows = []
        for store_name, abbr in bmp.store_abbr_map.items():
            label = f"{abbr}  {store_name}"
            self.store_rows.append(label)
            self.store_list.insert(tk.END, label)
        for index in range(len(self.store_rows)):
            self.store_list.selection_set(index)

        store_buttons = tk.Frame(store_body, bg=self.colors["card"])
        store_buttons.grid(row=2, column=0, sticky="w", pady=(10, 0))
        ttk.Button(store_buttons, text="All Stores", style="Ghost.TButton", command=self._select_all_stores).pack(side="left", padx=(0, 8))
        ttk.Button(store_buttons, text="Clear", style="Ghost.TButton", command=self._clear_store_selection).pack(side="left")

        options_card, _, options_body = self._make_card(
            parent,
            "Packet Options",
            "These control the output format and whether the tool should reuse saved files or force a refresh.",
        )
        options_card.grid(row=1, column=1, sticky="nsew")
        options_body.grid_columnconfigure(0, weight=1)

        ttk.Checkbutton(
            options_body,
            text="Include store sections",
            variable=self.include_store_var,
            style="Card.TCheckbutton",
            command=self._update_header_summary,
        ).grid(row=0, column=0, sticky="w", pady=2)
        ttk.Checkbutton(
            options_body,
            text="Include product appendix",
            variable=self.include_appendix_var,
            style="Card.TCheckbutton",
            command=self._update_header_summary,
        ).grid(row=1, column=0, sticky="w", pady=2)
        ttk.Checkbutton(
            options_body,
            text="Include charts",
            variable=self.include_charts_var,
            style="Card.TCheckbutton",
            command=self._update_header_summary,
        ).grid(row=2, column=0, sticky="w", pady=2)
        ttk.Checkbutton(
            options_body,
            text="Include kickback adjustments",
            variable=self.include_kickback_var,
            style="Card.TCheckbutton",
            command=self._update_header_summary,
        ).grid(row=3, column=0, sticky="w", pady=2)
        ttk.Checkbutton(
            options_body,
            text="Email after full run",
            variable=self.email_var,
            style="Card.TCheckbutton",
            command=self._update_header_summary,
        ).grid(row=4, column=0, sticky="w", pady=2)
        ttk.Checkbutton(
            options_body,
            text="Generate XLSX workbook",
            variable=self.xlsx_var,
            style="Card.TCheckbutton",
            command=self._update_header_summary,
        ).grid(row=5, column=0, sticky="w", pady=2)
        ttk.Checkbutton(
            options_body,
            text="Force refresh downloads",
            variable=self.force_refresh_var,
            style="Card.TCheckbutton",
            command=self._update_header_summary,
        ).grid(row=6, column=0, sticky="w", pady=(8, 8))

        self._make_help_row(
            options_body,
            "Smart cache reuse",
            "Default mode. Existing sales and catalog files in the run folder are reused first so repeat runs are fast and do not keep downloading the same data.",
        ).grid(row=7, column=0, sticky="ew", pady=(8, 8))
        self._make_help_row(
            options_body,
            "Force refresh downloads",
            "Use this only when you know the saved files are stale and want fresh exports. Build-only modes still stay build-only and reuse saved files.",
        ).grid(row=8, column=0, sticky="ew")

    def _build_activity_tab(self, parent: tk.Widget) -> None:
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_columnconfigure(1, weight=1)
        parent.grid_rowconfigure(1, weight=1)

        status_card, _, status_body = self._make_card(
            parent,
            "Live Status",
            "A clean snapshot of what the app is doing right now.",
        )
        status_card.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 12))
        status_body.grid_columnconfigure(0, weight=1)
        status_body.grid_columnconfigure(1, weight=1)

        self._make_summary_tile(status_body, "Current Status", self.status_var).grid(row=0, column=0, sticky="ew", padx=(0, 8), pady=(0, 8))
        self._make_summary_tile(status_body, "Current Activity", self.activity_var).grid(row=0, column=1, sticky="ew", pady=(0, 8))
        tk.Label(
            status_body,
            textvariable=self.setup_summary_var,
            bg=self.colors["card_alt"],
            fg=self.colors["muted"],
            font=self.fonts["small"],
            justify="left",
            wraplength=960,
            anchor="w",
            padx=12,
            pady=10,
            highlightbackground=self.colors["border"],
            highlightthickness=1,
        ).grid(row=1, column=0, columnspan=2, sticky="ew")

        log_card, log_header, log_body = self._make_card(
            parent,
            "Activity Feed",
            "Short status only. File-level archive noise and QA cache writes are hidden so this stays readable.",
        )
        log_card.grid(row=1, column=0, sticky="nsew", padx=(0, 12))
        log_body.grid_rowconfigure(0, weight=1)
        log_body.grid_columnconfigure(0, weight=1)
        ttk.Button(log_header, text="Clear Feed", style="Ghost.TButton", command=self._clear_logs).pack(side="right")

        self.log_list = tk.Listbox(
            log_body,
            activestyle="none",
            bg=self.colors["log_bg"],
            fg=self.colors["text"],
            font=self.fonts["small"],
            selectbackground=self.colors["ghost_dark"],
            selectforeground=self.colors["text"],
            highlightthickness=1,
            highlightbackground=self.colors["border"],
            relief="flat",
        )
        self.log_list.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(log_body, orient="vertical", command=self.log_list.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_list.configure(yscrollcommand=log_scroll.set)

        notes_card, _, notes_body = self._make_card(
            parent,
            "Reading The Feed",
            "Use this tab when a run is in progress or if something does not look right.",
        )
        notes_card.grid(row=1, column=1, sticky="nsew")
        self._make_help_row(notes_body, "Green entries", "A step finished successfully, like a PDF build or a completed email.").pack(fill="x", pady=(0, 10))
        self._make_help_row(notes_body, "Amber entries", "Something needs attention, such as missing stores or an incomplete comparison window.").pack(fill="x", pady=(0, 10))
        self._make_help_row(notes_body, "Red entries", "The run stopped because of an error. The app will also show a popup with the same message.").pack(fill="x", pady=(0, 10))
        self._make_help_row(notes_body, "Best workflow", "Stay on Overview while choosing settings, then jump here automatically once a run starts if you want to watch progress.").pack(fill="x")

    def _make_hero_chip(
        self,
        parent: tk.Widget,
        variable: tk.StringVar,
        row: int,
        column: int,
        columnspan: int = 1,
    ) -> None:
        chip = tk.Label(
            parent,
            textvariable=variable,
            bg=self.colors["hero_chip"],
            fg="#F8F2E8",
            font=self.fonts["chip"],
            padx=12,
            pady=7,
            highlightbackground=self.colors["hero_border"],
            highlightthickness=1,
        )
        chip.grid(row=row, column=column, columnspan=columnspan, sticky="e", padx=6, pady=6)

    def _field_label(self, parent: tk.Widget, text: str) -> tk.Label:
        return tk.Label(
            parent,
            text=text,
            bg=self.colors["card"],
            fg=self.colors["text"],
            font=self.fonts["small"],
            anchor="w",
        )

    def _make_summary_tile(self, parent: tk.Widget, title: str, variable: tk.StringVar) -> tk.Frame:
        tile = tk.Frame(
            parent,
            bg=self.colors["card_alt"],
            highlightbackground=self.colors["border"],
            highlightthickness=1,
            padx=12,
            pady=10,
        )
        tk.Label(
            tile,
            text=title,
            bg=self.colors["card_alt"],
            fg=self.colors["muted"],
            font=self.fonts["small"],
            anchor="w",
        ).pack(anchor="w")
        tk.Label(
            tile,
            textvariable=variable,
            bg=self.colors["card_alt"],
            fg=self.colors["text"],
            font=self.fonts["chip"],
            justify="left",
            wraplength=320,
            anchor="w",
        ).pack(anchor="w", pady=(4, 0))
        return tile

    def _make_step_row(
        self,
        parent: tk.Widget,
        number: int,
        title: str,
        description: str,
        button_text: Optional[str] = None,
        command=None,
    ) -> tk.Frame:
        row = tk.Frame(
            parent,
            bg=self.colors["card_alt"],
            highlightbackground=self.colors["border"],
            highlightthickness=1,
            padx=12,
            pady=10,
        )
        badge = tk.Label(
            row,
            text=str(number),
            bg=self.colors["accent"],
            fg="#FFFFFF",
            font=self.fonts["chip"],
            width=3,
            pady=6,
        )
        badge.pack(side="left", padx=(0, 12))
        text_col = tk.Frame(row, bg=self.colors["card_alt"])
        text_col.pack(side="left", fill="both", expand=True)
        tk.Label(
            text_col,
            text=title,
            bg=self.colors["card_alt"],
            fg=self.colors["text"],
            font=self.fonts["chip"],
            anchor="w",
        ).pack(anchor="w")
        tk.Label(
            text_col,
            text=description,
            bg=self.colors["card_alt"],
            fg=self.colors["muted"],
            font=self.fonts["small"],
            justify="left",
            wraplength=420,
            anchor="w",
        ).pack(anchor="w", pady=(4, 0))
        if button_text and command is not None:
            ttk.Button(row, text=button_text, style="Ghost.TButton", command=command).pack(side="right", padx=(12, 0))
        return row

    def _make_help_row(self, parent: tk.Widget, title: str, description: str) -> tk.Frame:
        row = tk.Frame(
            parent,
            bg=self.colors["card_alt"],
            highlightbackground=self.colors["border"],
            highlightthickness=1,
            padx=12,
            pady=10,
        )
        tk.Label(
            row,
            text=title,
            bg=self.colors["card_alt"],
            fg=self.colors["text"],
            font=self.fonts["chip"],
            anchor="w",
        ).pack(anchor="w")
        tk.Label(
            row,
            text=description,
            bg=self.colors["card_alt"],
            fg=self.colors["muted"],
            font=self.fonts["small"],
            justify="left",
            wraplength=420,
            anchor="w",
        ).pack(anchor="w", pady=(4, 0))
        return row

    def _bind_events(self) -> None:
        self.date_combo.bind("<<ComboboxSelected>>", lambda _event: self._on_window_changed())
        self.brand_search_var.trace_add("write", lambda *_args: self._refresh_brand_list())
        self.custom_start_var.trace_add("write", lambda *_args: self._update_header_summary())
        self.custom_end_var.trace_add("write", lambda *_args: self._update_header_summary())
        self.output_dir_var.trace_add("write", lambda *_args: self._update_header_summary())

        self.brand_list.bind("<<ListboxSelect>>", self._on_brand_selection_changed)
        self.brand_list.bind("<KeyPress>", self._on_brand_list_keypress)
        self.brand_list.bind("<space>", self._toggle_active_brand_selection)
        self.brand_list.bind("<Return>", self._toggle_active_brand_selection)
        self.brand_list.bind("<Control-a>", self._select_all_visible_brands_event)

        self.brand_search_entry.bind("<Escape>", self._clear_search_and_focus_list)
        self.custom_brand_entry.bind("<Return>", self._add_custom_brand_event)
        self.root.bind("<Control-f>", self._focus_brand_search)

        self.store_list.bind("<<ListboxSelect>>", lambda _event: self._update_store_summary())

    def _load_brand_options(self) -> List[BrandOption]:
        brand_map: Dict[str, BrandOption] = {}
        today_name = datetime.now().strftime("%A")
        config_path = Path(__file__).with_name("brand_config.json")

        if config_path.exists():
            try:
                config = json.loads(config_path.read_text(encoding="utf-8"))
            except Exception:
                config = {}
            for item in config.get("brands", []):
                synonyms = item.get("brand_synonyms", [])
                if isinstance(synonyms, str):
                    synonyms = [synonyms]
                if not synonyms and item.get("brand"):
                    synonyms = [part.strip() for part in str(item["brand"]).split("/") if part.strip()]

                days = [str(day).strip() for day in item.get("days", []) if str(day).strip()]
                emails = tuple(str(email).strip() for email in item.get("emails", []) if str(email).strip())
                folder_name = str(item.get("folder_name", "")).strip()
                rep = str(item.get("rep", "")).strip()
                location = str(item.get("location", "")).strip()
                days_display = ", ".join(days)
                scheduled_today = today_name in days if days else False

                for syn in synonyms:
                    name = str(syn).strip()
                    if not name:
                        continue
                    brand_map.setdefault(
                        name.lower(),
                        BrandOption(
                            name=name,
                            folder_name=folder_name or name,
                            rep=rep,
                            location=location,
                            days=days_display,
                            emails=emails,
                            source="brand_config",
                            scheduled_today=scheduled_today,
                        ),
                    )

        if deals is not None and isinstance(getattr(deals, "brand_criteria", None), dict):
            for key in deals.brand_criteria.keys():
                name = str(key).strip()
                if not name:
                    continue
                brand_map.setdefault(name.lower(), BrandOption(name=name, source="deals"))

        for name in self._load_custom_brand_names():
            brand_map.setdefault(
                name.lower(),
                BrandOption(
                    name=name,
                    folder_name=name,
                    source="custom",
                ),
            )

        if not brand_map:
            fallback = [
                "Cold Fire",
                "Connected",
                "Raw Garden",
                "Stiiizy",
                "West Coast Cure",
            ]
            for name in fallback:
                brand_map[name.lower()] = BrandOption(name=name, source="fallback")

        return sorted(
            brand_map.values(),
            key=lambda item: (not item.scheduled_today, item.name.lower()),
        )

    def _load_custom_brand_names(self) -> List[str]:
        if not CUSTOM_BRANDS_PATH.exists():
            return []
        try:
            payload = json.loads(CUSTOM_BRANDS_PATH.read_text(encoding="utf-8"))
        except Exception:
            return []

        if isinstance(payload, dict):
            raw_names = payload.get("brands", [])
        elif isinstance(payload, list):
            raw_names = payload
        else:
            raw_names = []

        out: List[str] = []
        seen: set[str] = set()
        for value in raw_names:
            name = str(value).strip()
            if not name:
                continue
            lowered = name.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            out.append(name)
        return out

    def _save_custom_brand_names(self) -> None:
        custom_names = sorted(
            {
                item.name
                for item in self.brand_options
                if str(item.source).strip().lower() == "custom" and item.name.strip()
            },
            key=str.casefold,
        )
        payload = {"brands": custom_names}
        CUSTOM_BRANDS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _brand_matches_query(self, option: BrandOption, query: str) -> bool:
        if not query:
            return True
        haystack = " ".join(
            [
                option.name,
                option.folder_name,
                option.rep,
                option.location,
                option.days,
                " ".join(option.emails),
            ]
        ).lower()
        return query in haystack

    def _capture_visible_brand_selection(self) -> None:
        if not hasattr(self, "brand_list"):
            return
        visible_names = {item.name for item in self.filtered_brand_options}
        self.selected_brand_names.difference_update(visible_names)
        for idx in self.brand_list.curselection():
            if 0 <= idx < len(self.filtered_brand_options):
                self.selected_brand_names.add(self.filtered_brand_options[idx].name)

    def _refresh_brand_list(self, ensure_selection: bool = False) -> None:
        if hasattr(self, "brand_list"):
            self._capture_visible_brand_selection()

        query = self.brand_search_var.get().strip().lower()
        self.filtered_brand_options = [item for item in self.brand_options if self._brand_matches_query(item, query)]

        if not hasattr(self, "brand_list"):
            return

        self.brand_list.delete(0, tk.END)
        for item in self.filtered_brand_options:
            label = item.name
            if item.scheduled_today:
                label = f"{label}  • today"
            self.brand_list.insert(tk.END, label)

        restored = False
        first_selected_idx: Optional[int] = None
        for idx, item in enumerate(self.filtered_brand_options):
            if item.name in self.selected_brand_names:
                self.brand_list.selection_set(idx)
                restored = True
                if first_selected_idx is None:
                    first_selected_idx = idx

        if ensure_selection and not self.selected_brand_names and self.filtered_brand_options:
            self.selected_brand_names.add(self.filtered_brand_options[0].name)
            self.brand_list.selection_set(0)
            restored = True
            first_selected_idx = 0

        if self.filtered_brand_options:
            target_idx = 0 if first_selected_idx is None else first_selected_idx
            self.brand_list.activate(target_idx)
            self.brand_list.see(target_idx)

        if not restored:
            self.brand_selection_var.set("No brands selected")

        self._update_brand_browser_summary()
        self._update_brand_details()
        self._update_header_summary()

    def _update_brand_browser_summary(self) -> None:
        visible_count = len(self.filtered_brand_options)
        selected_count = len(self.selected_brand_names)
        query = self.brand_search_var.get().strip()
        if query:
            self.brand_browser_summary_var.set(f"Showing {visible_count} matching brands  •  {selected_count} queued")
        else:
            self.brand_browser_summary_var.set(f"Showing {visible_count} brands  •  {selected_count} queued")

    def _select_all_visible_brands(self) -> None:
        for item in self.filtered_brand_options:
            self.selected_brand_names.add(item.name)
        self._restore_brand_selection()

    def _select_all_visible_brands_event(self, _event: tk.Event) -> str:
        self._select_all_visible_brands()
        return "break"

    def _select_today_brands(self) -> None:
        today_names = [item.name for item in self.brand_options if item.scheduled_today]
        if not today_names:
            messagebox.showinfo("No Scheduled Brands", "No brands are marked for today in brand_config.json.")
            return
        self.brand_search_var.set("")
        self.selected_brand_names.update(today_names)
        self._restore_brand_selection()

    def _add_custom_brand_event(self, _event: tk.Event) -> str:
        self._add_custom_brand_from_entry()
        return "break"

    def _add_custom_brand_from_entry(self) -> None:
        raw_name = self.custom_brand_var.get().strip()
        if not raw_name:
            messagebox.showwarning("Missing Brand", "Type a brand name before clicking Add Brand.")
            return

        existing = self.brand_lookup_lower.get(raw_name.lower())
        if existing is not None:
            self.selected_brand_names.add(existing.name)
            self.custom_brand_var.set("")
            self.brand_search_var.set(existing.name)
            self._refresh_brand_list()
            self._restore_brand_selection()
            self.activity_var.set(f"{existing.name} is already available and has been added to the queue.")
            return

        option = BrandOption(
            name=raw_name,
            folder_name=raw_name,
            source="custom",
        )
        self.brand_options.append(option)
        self.brand_options = sorted(
            self.brand_options,
            key=lambda item: (not item.scheduled_today, item.name.lower()),
        )
        self.brand_lookup[option.name] = option
        self.brand_lookup_lower[option.name.lower()] = option
        self.selected_brand_names.add(option.name)
        self.custom_brand_var.set("")
        self.brand_search_var.set(option.name)
        self._save_custom_brand_names()
        self._refresh_brand_list()
        self._restore_brand_selection()
        self.activity_var.set(f"Added custom brand: {option.name}")

    def _remove_selected_custom_brands(self) -> None:
        selected_names = self._selected_brand_names_list()
        if not selected_names:
            messagebox.showwarning("No Brands Selected", "Select one or more brands to remove.")
            return

        custom_names: List[str] = []
        protected_names: List[str] = []
        for name in selected_names:
            option = self.brand_lookup.get(name)
            if option is not None and str(option.source).strip().lower() == "custom":
                custom_names.append(name)
            else:
                protected_names.append(name)

        if not custom_names:
            messagebox.showinfo(
                "Built-in Brands",
                "Only custom brands added in this GUI can be removed here. Built-in brands from brand_config.json or deals can still be cleared from the queue.",
            )
            return

        if len(custom_names) == 1:
            prompt = f"Remove custom brand '{custom_names[0]}' from the saved GUI list?"
        else:
            prompt = f"Remove {len(custom_names)} custom brands from the saved GUI list?"
        if protected_names:
            prompt += f"\n\nBuilt-in brands will be kept: {', '.join(protected_names[:4])}"
            if len(protected_names) > 4:
                prompt += f", +{len(protected_names) - 4} more"

        if not messagebox.askyesno("Remove Brand", prompt):
            return

        remove_set = set(custom_names)
        self.brand_options = [item for item in self.brand_options if item.name not in remove_set]
        for name in custom_names:
            self.brand_lookup.pop(name, None)
            self.brand_lookup_lower.pop(name.lower(), None)
            self.selected_brand_names.discard(name)

        self._save_custom_brand_names()
        self._refresh_brand_list()
        self._restore_brand_selection()

        if protected_names:
            self.activity_var.set(
                f"Removed {len(custom_names)} custom brand(s). Built-in brands stayed in the queue."
            )
        else:
            self.activity_var.set(f"Removed {len(custom_names)} custom brand(s).")

    def _clear_brand_selection(self) -> None:
        self.selected_brand_names.clear()
        if hasattr(self, "brand_list"):
            self.brand_list.selection_clear(0, tk.END)
        self._update_brand_details()
        self._update_header_summary()

    def _restore_brand_selection(self) -> None:
        if not hasattr(self, "brand_list"):
            return
        self.brand_list.selection_clear(0, tk.END)
        first_idx: Optional[int] = None
        for idx, item in enumerate(self.filtered_brand_options):
            if item.name in self.selected_brand_names:
                self.brand_list.selection_set(idx)
                if first_idx is None:
                    first_idx = idx
        if first_idx is not None:
            self.brand_list.activate(first_idx)
            self.brand_list.see(first_idx)
        self._update_brand_details()
        self._update_header_summary()

    def _active_brand_option(self) -> Optional[BrandOption]:
        if not self.filtered_brand_options:
            return None
        try:
            idx = int(self.brand_list.index("active"))
        except Exception:
            idx = 0
        if idx < 0 or idx >= len(self.filtered_brand_options):
            idx = 0
        return self.filtered_brand_options[idx]

    def _selected_brand_names_list(self) -> List[str]:
        self._capture_visible_brand_selection()
        return sorted(self.selected_brand_names, key=str.casefold)

    def _selected_store_codes(self) -> List[str]:
        idxs = self.store_list.curselection()
        out: List[str] = []
        for idx in idxs:
            label = self.store_rows[idx]
            abbr = label.split(" ", 1)[0].strip().upper()
            if abbr:
                out.append(abbr)
        return bmp.order_store_codes(out)

    def _select_all_stores(self) -> None:
        self.store_list.selection_set(0, tk.END)
        self._update_store_summary()

    def _clear_store_selection(self) -> None:
        self.store_list.selection_clear(0, tk.END)
        self._update_store_summary()

    def _update_store_summary(self) -> None:
        stores = self._selected_store_codes()
        if stores:
            self.store_summary_var.set(f"{len(stores)} selected  •  {', '.join(stores)}")
        else:
            self.store_summary_var.set("No stores selected")
        self._update_header_summary()

    def _on_brand_selection_changed(self, _event: tk.Event) -> None:
        self._capture_visible_brand_selection()
        self._update_brand_details()
        self._update_header_summary()

    def _update_brand_details(self) -> None:
        names = self._selected_brand_names_list()
        if not names:
            self.brand_selection_var.set("No brands selected")
            self.selected_queue_var.set("Nothing queued yet.\nOpen the Brands tab and select one or more brands.")
            active = self._active_brand_option()
            if active is not None:
                preview = [f"Preview: {active.name}"]
                if active.rep:
                    preview.append(f"Rep: {active.rep}")
                if active.location:
                    preview.append(f"Stores: {active.location}")
                if active.days:
                    preview.append(f"Schedule: {active.days}")
                self.brand_details_var.set("\n".join(preview))
            else:
                self.brand_details_var.set("Use Ctrl+F to search and the list to select one or more brands.")
            return

        self.brand_selection_var.set(f"{len(names)} brand{'s' if len(names) != 1 else ''} selected")
        queue_lines = [f"{idx}. {name}" for idx, name in enumerate(names[:10], start=1)]
        if len(names) > 10:
            queue_lines.append(f"+{len(names) - 10} more")
        self.selected_queue_var.set("\n".join(queue_lines))

        if len(names) == 1:
            item = self.brand_lookup.get(names[0])
            if item is None:
                self.brand_details_var.set(names[0])
                return
            lines = [item.name]
            if item.rep:
                lines.append(f"Rep: {item.rep}")
            if item.folder_name and item.folder_name != item.name:
                lines.append(f"Folder: {item.folder_name}")
            if item.location:
                lines.append(f"Stores: {item.location}")
            if item.days:
                lines.append(f"Schedule: {item.days}")
            if item.emails:
                preview = ", ".join(item.emails[:2])
                if len(item.emails) > 2:
                    preview = f"{preview}, +{len(item.emails) - 2} more"
                lines.append(f"Emails: {preview}")
            lines.append(f"Source: {item.source}")
            self.brand_details_var.set("\n".join(lines))
            return

        preview = ", ".join(names[:6])
        if len(names) > 6:
            preview = f"{preview}, +{len(names) - 6} more"
        self.brand_details_var.set(f"Batch run queue:\n{preview}")

    def _update_header_summary(self) -> None:
        brand_count = len(self._selected_brand_names_list())
        store_count = len(self._selected_store_codes())
        if brand_count:
            self.brand_count_var.set(f"{brand_count} brand{'s' if brand_count != 1 else ''} selected")
        else:
            self.brand_count_var.set("No brands selected")
        if store_count:
            self.store_count_var.set(f"{store_count} store{'s' if store_count != 1 else ''} selected")
        else:
            self.store_count_var.set("No stores selected")
        self.cache_mode_var.set("Force refresh on" if self.force_refresh_var.get() else "Smart cache reuse on")
        self._update_brand_browser_summary()

        try:
            start_day, end_day = self._resolve_window()
            self.window_summary_var.set(f"{start_day.isoformat()} to {end_day.isoformat()}")
            self.window_preview_var.set(
                f"Resolved window: {start_day.isoformat()} to {end_day.isoformat()}  •  Output root: {Path(self.output_dir_var.get().strip() or '.').expanduser()}"
            )
            email_state = "On" if self.email_var.get() else "Off"
            xlsx_state = "On" if self.xlsx_var.get() else "Off"
            next_step = ""
            if brand_count == 0:
                next_step = "  •  Next: open Brands and queue at least one brand."
            elif store_count == 0:
                next_step = "  •  Next: pick at least one store in Data & Output."
            self.setup_summary_var.set(
                f"Brands: {brand_count}  •  Stores: {store_count}  •  Window: {start_day.isoformat()} to {end_day.isoformat()}  •  "
                f"Email: {email_state}  •  XLSX: {xlsx_state}  •  Cache: {'Force refresh' if self.force_refresh_var.get() else 'Smart reuse'}"
                f"{next_step}"
            )
        except Exception:
            self.window_summary_var.set("Select a valid date window")
            self.window_preview_var.set("Custom dates must be valid YYYY-MM-DD values with an end date on or after the start date.")
            self.setup_summary_var.set("Fix the date window before running. Brands and stores can still be selected now.")

    def _on_window_changed(self) -> None:
        self._set_custom_date_state()
        self._update_header_summary()

    def _set_custom_date_state(self) -> None:
        custom = self.date_preset_var.get() == "Custom"
        state = "normal" if custom else "disabled"
        self.custom_start_entry.configure(state=state)
        self.custom_end_entry.configure(state=state)

    def _clear_search_and_focus_list(self, _event: tk.Event) -> str:
        self.brand_search_var.set("")
        self.brand_list.focus_set()
        return "break"

    def _focus_brand_search(self, _event: tk.Event) -> str:
        self.brand_search_entry.focus_set()
        self.brand_search_entry.selection_range(0, tk.END)
        return "break"

    def _toggle_active_brand_selection(self, _event: tk.Event) -> str:
        if not self.filtered_brand_options:
            return "break"
        try:
            idx = int(self.brand_list.index("active"))
        except Exception:
            idx = 0
        if idx < 0 or idx >= len(self.filtered_brand_options):
            return "break"
        name = self.filtered_brand_options[idx].name
        if idx in self.brand_list.curselection():
            self.brand_list.selection_clear(idx)
            self.selected_brand_names.discard(name)
        else:
            self.brand_list.selection_set(idx)
            self.selected_brand_names.add(name)
        self._update_brand_details()
        self._update_header_summary()
        return "break"

    def _on_brand_list_keypress(self, event: tk.Event) -> Optional[str]:
        if event.state & 0x4:
            return None
        if event.keysym in {"Shift_L", "Shift_R", "Control_L", "Control_R", "Alt_L", "Alt_R"}:
            return None
        if not event.char or not event.char.isprintable() or event.char.isspace():
            return None

        now = time.monotonic()
        if now - self.brand_jump_timestamp > 0.9:
            self.brand_jump_buffer = ""
        self.brand_jump_timestamp = now
        self.brand_jump_buffer += event.char.lower()

        if not self._jump_to_brand(self.brand_jump_buffer) and len(self.brand_jump_buffer) > 1:
            self.brand_jump_buffer = event.char.lower()
            self._jump_to_brand(self.brand_jump_buffer)
        return "break"

    def _jump_to_brand(self, query: str) -> bool:
        if not query or not self.filtered_brand_options:
            return False
        names = [item.name.lower() for item in self.filtered_brand_options]
        try:
            start_idx = int(self.brand_list.index("active"))
        except Exception:
            start_idx = -1

        for offset in range(1, len(names) + 1):
            idx = (start_idx + offset) % len(names)
            if names[idx].startswith(query):
                self.brand_list.activate(idx)
                self.brand_list.see(idx)
                self.brand_list.selection_set(idx)
                self.selected_brand_names.add(self.filtered_brand_options[idx].name)
                self._update_brand_details()
                self._update_header_summary()
                return True
        return False

    def _resolve_window(self) -> tuple[date, date]:
        preset = self.date_preset_var.get()
        if preset == "Last 30 days":
            return bmp.compute_default_window(30)
        if preset == "Custom":
            start_txt = self.custom_start_var.get().strip()
            end_txt = self.custom_end_var.get().strip()
            if not start_txt or not end_txt:
                raise ValueError("Custom range requires both start and end dates.")
            start_day = bmp.parse_iso_date(start_txt)
            end_day = bmp.parse_iso_date(end_txt)
            if end_day < start_day:
                raise ValueError("Custom end date must be on or after the start date.")
            return start_day, end_day
        return bmp.compute_default_window(60)

    def _build_options(self, run_export: bool, email_results: bool, run_catalog_export: bool = True) -> bmp.PacketOptions:
        return bmp.PacketOptions(
            run_export=run_export,
            run_catalog_export=run_catalog_export,
            include_store_sections=self.include_store_var.get(),
            include_product_appendix=self.include_appendix_var.get(),
            include_charts=self.include_charts_var.get(),
            include_kickback_adjustments=self.include_kickback_var.get(),
            email_results=email_results,
            generate_xlsx=self.xlsx_var.get(),
            top_n=20,
            force_refresh_data=self.force_refresh_var.get(),
        )

    def _queue_event(self, kind: str, payload: object = "") -> None:
        self.log_queue.put((kind, payload))

    def _queue_log(self, text: str) -> None:
        self._queue_event("log", text)

    def _queue_activity(self, text: str) -> None:
        self._queue_event("activity", text)

    def _drain_log_queue(self) -> None:
        while True:
            try:
                kind, payload = self.log_queue.get_nowait()
            except queue.Empty:
                break

            if kind == "log":
                self._append_log(str(payload))
            elif kind == "activity":
                self.activity_var.set(str(payload))
            elif kind == "status":
                self.status_var.set(str(payload))
            elif kind == "worker_done":
                info = payload if isinstance(payload, dict) else {}
                self.worker_running = False
                self._set_buttons_enabled(True)
                self.progress.stop()
                self.status_var.set("Ready" if info.get("success", False) else "Needs Attention")
                message = str(info.get("message", "Run finished"))
                self.activity_var.set(message)
            elif kind == "error_dialog":
                messagebox.showerror("Error", str(payload))

        self.root.after(120, self._drain_log_queue)

    def _simplify_log(self, text: str) -> tuple[Optional[str], str]:
        raw = " ".join(str(text).strip().split())
        if not raw:
            return None, "info"

        if raw.startswith("[QA]") or "getCatalog.py output (tail)" in raw or "stderr (tail)" in raw:
            return None, "info"
        if raw.startswith("[ARCHIVE]"):
            return None, "info"
        if raw.startswith("[WINDOW]") or raw.startswith("[STORES]") or raw.startswith("[BRAND]") or raw.startswith("[SUPPLY]"):
            return None, "info"

        level = "info"
        if raw.startswith("[WARN]"):
            level = "warn"
        elif raw.startswith("[ERROR]"):
            level = "error"
        elif raw.startswith("[PDF]") or raw.startswith("[EMAIL]") or raw == "Done ✅":
            level = "success"

        if raw.startswith("[START] Building Brand Meeting Packet for "):
            brand = raw.split(" for ", 1)[-1].strip("'")
            return f"Starting packet for {brand}", "info"
        if raw.startswith("[RUN] Build-only mode:"):
            return "Building packet from cached files only", "info"
        if raw.startswith("[RUN] Build + Email (No Download):"):
            return "Building and emailing from cached files only", "info"
        if raw.startswith("[SALES] Reusing"):
            return "Using saved sales files from this run", "info"
        if raw.startswith("[SALES] Seeded"):
            return raw.replace("[SALES] ", ""), "info"
        if raw.startswith("[SALES] Missing cached exports for"):
            msg = raw.replace("[SALES] Missing cached exports for ", "")
            msg = msg.replace(". Refreshing sales export.", "")
            return f"Refreshing missing sales data for {msg}", "warn"
        if raw.startswith("[SALES] Exporting acquisition window"):
            return raw.replace("[SALES] Exporting acquisition window ", "Downloading sales data for "), "info"
        if raw.startswith("[EXPORT] Running sales export for"):
            return raw.replace("[EXPORT] Running sales export for ", "Sales export window "), "info"
        if raw.startswith("[EXPORT] Sales export completed."):
            return "Sales export finished", "success"
        if raw.startswith("[DONE] Sales exports archived in"):
            return "Sales files are ready", "success"
        if raw.startswith("[CATALOG] Reusing"):
            return "Using saved inventory files from this run", "info"
        if raw.startswith("[CATALOG] Running getCatalog.py export..."):
            return "Refreshing inventory data", "info"
        if raw.startswith("[CATALOG] Catalog export completed."):
            return "Inventory refresh finished", "success"
        if raw.startswith("[CATALOG] Force refresh enabled."):
            return "Force refresh is replacing the saved inventory cache", "warn"
        if raw.startswith("[CATALOG] No cached catalog files in this run."):
            return "No saved inventory files in this run folder; using what is available in files/", "warn"
        if raw.startswith("[WARN] Missing sales export for"):
            return raw.replace("[WARN] ", ""), "warn"
        if raw.startswith("[WARN] Prior comparable window is not fully covered"):
            return "Prior comparison window is incomplete, so some delta views were disabled", "warn"
        if raw.startswith("[WARN] No matching brand sales rows found"):
            return "No matching sales rows were found for the selected brand and window", "warn"
        if raw.startswith("[STORE] Skipping"):
            return raw.replace("[STORE] ", ""), "warn"
        if raw.startswith("[PDF] Created"):
            return "PDF packet created", "success"
        if raw.startswith("[XLSX] Created"):
            return "Workbook created", "success"
        if raw.startswith("[EMAIL] Sent packet to"):
            return "Packet email sent", "success"
        if raw == "Done ✅":
            return "Run finished", "success"

        cleaned = raw
        for prefix in ("[START] ", "[DONE] ", "[RUN] ", "[WARN] ", "[ERROR] ", "[CATALOG] ", "[SALES] ", "[EXPORT] ", "[PDF] ", "[XLSX] ", "[EMAIL] "):
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix):]
                break
        return cleaned, level

    def _append_log(self, text: str) -> None:
        simple_text, level = self._simplify_log(text)
        if not simple_text:
            return
        if simple_text == self.last_log_message:
            return

        self.last_log_message = simple_text
        stamp = datetime.now().strftime("%H:%M:%S")
        line = f"{stamp}  {simple_text}"
        self.log_list.insert(tk.END, line)
        last_idx = self.log_list.size() - 1

        color = {
            "info": self.colors["text"],
            "warn": self.colors["warn"],
            "error": self.colors["error"],
            "success": self.colors["success"],
        }.get(level, self.colors["text"])
        self.log_list.itemconfig(last_idx, fg=color)

        while self.log_list.size() > self.max_log_rows:
            self.log_list.delete(0)
        self.log_list.see(tk.END)

    def _clear_logs(self) -> None:
        self.log_list.delete(0, tk.END)
        self.last_log_message = ""

    def _set_buttons_enabled(self, enabled: bool) -> None:
        state = "normal" if enabled else "disabled"
        self.btn_download.configure(state=state)
        self.btn_build.configure(state=state)
        self.btn_build_email.configure(state=state)
        self.btn_all.configure(state=state)

    def _run_background(self, fn, start_message: str, finish_message: str) -> None:
        if self.worker_running:
            messagebox.showwarning("Busy", "A task is already running.")
            return

        self.worker_running = True
        self._set_buttons_enabled(False)
        self.progress.start(10)
        self.status_var.set("Running")
        self.activity_var.set(start_message)
        if hasattr(self, "notebook") and hasattr(self, "activity_tab"):
            self.notebook.select(self.activity_tab)

        def _worker() -> None:
            success = False
            try:
                fn()
                success = True
            except Exception as exc:
                err_msg = str(exc)
                self._queue_log(f"[ERROR] {err_msg}")
                self._queue_event("error_dialog", err_msg)
            finally:
                self._queue_event(
                    "worker_done",
                    {
                        "success": success,
                        "message": finish_message if success else "Run stopped with an error",
                    },
                )

        threading.Thread(target=_worker, daemon=True).start()

    def _run_across_brands(self, mode_name: str, handler) -> None:
        brands = self._selected_brand_names_list()
        if not brands:
            raise ValueError("Select at least one brand.")

        start_day, end_day = self._resolve_window()
        stores = self._selected_store_codes()
        if not stores:
            raise ValueError("Select at least one store.")
        output_root = Path(self.output_dir_var.get().strip() or bmp.DEFAULT_OUTPUT_ROOT).expanduser().resolve()

        self._queue_log(f"[START] {mode_name} for {len(brands)} brand(s)")
        for idx, brand in enumerate(brands, start=1):
            self._queue_activity(f"{mode_name}: {brand} ({idx}/{len(brands)})")
            handler(brand, start_day, end_day, stores, output_root)
            self._queue_log(f"[DONE] {brand} complete ({idx}/{len(brands)})")

    def _download_sales_for_brand(
        self,
        brand: str,
        start_day: date,
        end_day: date,
        stores: Sequence[str],
        output_root: Path,
    ) -> None:
        paths = bmp.build_run_paths(output_root, brand, start_day, end_day)
        n_days = bmp.window_days(start_day, end_day)
        prior_report_end = start_day - timedelta(days=1)
        prior_report_start = prior_report_end - timedelta(days=n_days - 1)
        acquisition_start = min(start_day, prior_report_start)
        acquisition_end = end_day

        sales_paths, missing, _did_export = bmp.prepare_sales_exports(
            paths=paths,
            brand=brand,
            selected_store_codes=stores,
            acquisition_start=acquisition_start,
            acquisition_end=acquisition_end,
            allow_export=True,
            force_refresh=self.force_refresh_var.get(),
            logger=self._queue_log,
        )
        if missing:
            self._queue_log(f"[WARN] Missing sales export for {', '.join(missing)}")
        else:
            self._queue_log(f"[DONE] Sales exports archived in {paths.raw_sales_dir}")
        if not sales_paths:
            raise ValueError(f"No usable sales exports were found for {brand}.")

    def _on_download_sales(self) -> None:
        def _task() -> None:
            self._run_across_brands("Preparing sales data", self._download_sales_for_brand)

        self._run_background(
            _task,
            start_message="Preparing cached or fresh sales data...",
            finish_message="Sales data is ready",
        )

    def _on_build_pdf(self) -> None:
        def _task() -> None:
            def _build(brand: str, start_day: date, end_day: date, stores: Sequence[str], output_root: Path) -> None:
                options = self._build_options(
                    run_export=False,
                    email_results=False,
                    run_catalog_export=False,
                )
                self._queue_log("[RUN] Build-only mode: using existing files (no export/download).")
                bmp.generate_brand_meeting_packet(
                    brand=brand,
                    start_day=start_day,
                    end_day=end_day,
                    selected_store_codes=stores,
                    output_root=output_root,
                    options=options,
                    logger=self._queue_log,
                )

            self._run_across_brands("Building packets", _build)

        self._run_background(
            _task,
            start_message="Building packet PDFs from saved inputs...",
            finish_message="Packet build finished",
        )

    def _on_build_email_no_download(self) -> None:
        def _task() -> None:
            def _build_email(brand: str, start_day: date, end_day: date, stores: Sequence[str], output_root: Path) -> None:
                options = self._build_options(
                    run_export=False,
                    email_results=True,
                    run_catalog_export=False,
                )
                self._queue_log("[RUN] Build + Email (No Download): using existing files only.")
                bmp.generate_brand_meeting_packet(
                    brand=brand,
                    start_day=start_day,
                    end_day=end_day,
                    selected_store_codes=stores,
                    output_root=output_root,
                    options=options,
                    logger=self._queue_log,
                )

            self._run_across_brands("Building and emailing packets", _build_email)

        self._run_background(
            _task,
            start_message="Building and emailing from saved inputs...",
            finish_message="Build and email finished",
        )

    def _on_full_run(self) -> None:
        def _task() -> None:
            def _full_run(brand: str, start_day: date, end_day: date, stores: Sequence[str], output_root: Path) -> None:
                options = self._build_options(
                    run_export=True,
                    email_results=self.email_var.get(),
                    run_catalog_export=True,
                )
                bmp.generate_brand_meeting_packet(
                    brand=brand,
                    start_day=start_day,
                    end_day=end_day,
                    selected_store_codes=stores,
                    output_root=output_root,
                    options=options,
                    logger=self._queue_log,
                )

            self._run_across_brands("Running full packets", _full_run)

        self._run_background(
            _task,
            start_message="Running cache-aware download, build, and email...",
            finish_message="Full run finished",
        )


if __name__ == "__main__":
    root = tk.Tk()
    app = BrandMeetingPacketGUI(root)
    root.mainloop()
