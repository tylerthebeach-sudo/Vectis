"""
Vectis v1.1
Finance Time Entry Processor — JobTime Export → Sage 300 Timecard Import
"""
import sys, os, threading, logging
from datetime import date, datetime, timedelta

import customtkinter as ctk
from tkinter import filedialog, messagebox, ttk
import tkinter as tk
import pandas as pd

from config import AppConfig
from processing import (
    read_input_file, detect_columns, validate_input, flag_anomalies,
    flag_dist_anomalies, find_unmapped_paycodes,
    flag_unmapped_paycodes_rows, get_excluded_rows,
    process_timesheet, export_to_excel, load_distribution_map,
)

# ── Logging ────────────────────────────────────────────────────────────────────
log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vectis.log")
logging.basicConfig(filename=log_path, level=logging.DEBUG, encoding="utf-8",
                    format="%(asctime)s %(levelname)s %(message)s")

# ── Theme ──────────────────────────────────────────────────────────────────────
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

ACCENT   = "#2B6CB0"
ACCENT2  = "#1A4A8A"
BG_DARK  = "#1A1A2E"
BG_MID   = "#16213E"
BG_CARD  = "#0F3460"
TEXT_WH  = "#FFFFFF"
TEXT_DIM = "#A0AEC0"
OK_GRN   = "#48BB78"
WARN_YLW = "#ECC94B"
ERR_RED  = "#FC8181"

_OVERRIDE_PWD = "vectis1"   # password to force-export despite distribution errors


# ══════════════════════════════════════════════════════════════════════════════
#  Pay Code Mapping Row
# ══════════════════════════════════════════════════════════════════════════════

class MappingRow:
    """One row in the pay code mapping table: JobTime Pay Code → Sage EARNDED code."""
    def __init__(self, parent, row_num: int, data: dict, on_delete):
        self.parent    = parent
        self.row_num   = row_num
        self.on_delete = on_delete

        default_linenum = str(data.get("linenum", row_num * 1000))
        self.jt_var      = tk.StringVar(value=data.get("jobtime_code", ""))
        self.earnded_var = tk.StringVar(value=str(data.get("earnded", "")))
        self.linenum_var = tk.StringVar(value=default_linenum)
        self.ena_var     = tk.BooleanVar(value=data.get("enabled", True))

        self.handle = tk.Label(parent, text="⠿", cursor="fleur",
                               bg=BG_CARD, fg=TEXT_DIM,
                               font=("Segoe UI", 14), width=2)
        self.jt_entry      = ctk.CTkEntry(parent, textvariable=self.jt_var,
                                           width=140, height=28, font=("Segoe UI", 11))
        self.earnded_entry = ctk.CTkEntry(parent, textvariable=self.earnded_var,
                                           width=62, height=28, font=("Segoe UI", 11),
                                           placeholder_text="100")
        self.linenum_entry = ctk.CTkEntry(parent, textvariable=self.linenum_var,
                                           width=62, height=28, font=("Segoe UI", 11),
                                           placeholder_text="1000")
        self.ena_chk = ctk.CTkCheckBox(parent, variable=self.ena_var, text="", width=28)
        self.del_btn = ctk.CTkButton(parent, text="✕", width=24, height=28,
                                     fg_color="#C53030", hover_color="#9B2C2C",
                                     command=self._delete, font=("Segoe UI", 11))

        self.widgets = [self.handle, self.jt_entry, self.earnded_entry,
                        self.linenum_entry, self.ena_chk, self.del_btn]
        self._grid()

    def _grid(self):
        r = self.row_num
        kw = {"padx": 2, "pady": 2}
        self.handle.grid       (row=r, column=0, **kw)
        self.jt_entry.grid     (row=r, column=1, sticky="ew", **kw)
        self.earnded_entry.grid(row=r, column=2, sticky="ew", **kw)
        self.linenum_entry.grid(row=r, column=3, sticky="ew", **kw)
        self.ena_chk.grid      (row=r, column=4, **kw)
        self.del_btn.grid      (row=r, column=5, **kw)

    def _delete(self):
        for w in self.widgets:
            w.destroy()
        self.on_delete(self)

    def hide(self):
        self._visible = False
        for w in self.widgets:
            try:
                w.grid_remove()
            except Exception:
                pass

    def show(self):
        self._visible = True
        self._grid()

    def regrid(self, new_row: int):
        self.row_num = new_row
        if getattr(self, '_visible', True):
            self._grid()

    def to_dict(self) -> dict:
        try:
            linenum = int(self.linenum_var.get().strip())
        except ValueError:
            linenum = self.row_num * 1000
        return {
            "jobtime_code": self.jt_var.get().strip(),
            "earnded":      self.earnded_var.get().strip(),
            "linenum":      linenum,
            "enabled":      self.ena_var.get(),
        }


# ══════════════════════════════════════════════════════════════════════════════
#  Main Application
# ══════════════════════════════════════════════════════════════════════════════

class CSVDaddyApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.config_mgr         = AppConfig()
        self.raw_df             = None
        self.col_map            = {}
        self.dist_map:  dict         = {}
        self.mapping_rows: list[MappingRow] = []
        self._preview_header_df = None
        self._preview_detail_df = None
        self._data_anomaly_map:    dict = {}   # bad data: non-numeric hours, missing fields
        self._dist_anomaly_map:    dict = {}   # missing from distribution file → blocks export
        self._unmapped_anomaly_map: dict = {}  # pay code not in mapping → silently excluded
        self._anomaly_map:         dict = {}   # merged display map (all types)
        self._anomaly_items: dict = {}
        self._anomaly_popup = None
        self._raw_display_df: pd.DataFrame | None = None
        self._drag_mr: object = None
        self._drag_ghost: tk.Toplevel | None = None
        self._drag_insert_idx: int = 0
        self._drag_line: tk.Frame | None = None
        self._preview_excluded_df: pd.DataFrame | None = None

        self.preview_mode_var   = tk.StringVar(value="Timecard_Header")
        self.raw_search_var     = tk.StringVar()
        self.preview_search_var = tk.StringVar()

        self.title("Vectis — Finance Time Entry Processor")
        self.geometry("1340x880")
        self.minsize(1100, 700)
        self.configure(fg_color=BG_DARK)

        self._build_ui()
        self.raw_search_var.trace_add("write",     self._on_raw_search_change)
        self.preview_search_var.trace_add("write", self._on_preview_search_change)
        self._load_config_into_ui()
        self.update_status("Ready. Select a JobTime file to begin.", "ok")

    # ══════════════════════════════════════════════════════════════════════════
    #  UI CONSTRUCTION
    # ══════════════════════════════════════════════════════════════════════════

    def _build_ui(self):
        # Title bar
        tb = ctk.CTkFrame(self, fg_color=BG_MID, height=60, corner_radius=0)
        tb.pack(fill="x", side="top")
        tb.pack_propagate(False)
        ctk.CTkLabel(tb, text="🗂️  Vectis", font=("Segoe UI", 22, "bold"),
                     text_color=TEXT_WH).pack(side="left", padx=20, pady=10)
        ctk.CTkLabel(tb, text="JobTime → Sage 300 Timecard Processor",
                     font=("Segoe UI", 13), text_color=TEXT_DIM).pack(side="left", padx=5)

        # Body
        body = ctk.CTkFrame(self, fg_color=BG_DARK, corner_radius=0)
        body.pack(fill="both", expand=True)
        body.columnconfigure(0, weight=0, minsize=500)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(0, weight=1)

        left = ctk.CTkScrollableFrame(body, fg_color=BG_MID, corner_radius=0, width=480)
        left.grid(row=0, column=0, sticky="nsew")
        self._build_left(left)

        right = ctk.CTkFrame(body, fg_color=BG_DARK, corner_radius=0)
        right.grid(row=0, column=1, sticky="nsew")
        self._build_right(right)

        self._build_action_bar()

    def _section(self, parent, title: str) -> ctk.CTkFrame:
        outer = ctk.CTkFrame(parent, fg_color=BG_CARD, corner_radius=10)
        outer.pack(fill="x", padx=12, pady=(8, 0))
        ctk.CTkLabel(outer, text=title, font=("Segoe UI", 12, "bold"),
                     text_color=TEXT_WH).pack(anchor="w", padx=12, pady=(8, 2))
        inner = ctk.CTkFrame(outer, fg_color="transparent")
        inner.pack(fill="x", padx=8, pady=(0, 8))
        return inner

    def _lbl(self, parent, text, row, col):
        ctk.CTkLabel(parent, text=text, font=("Segoe UI", 11), text_color=TEXT_DIM
                     ).grid(row=row, column=col, sticky="w", padx=(0, 6), pady=3)

    # ── Left panel ────────────────────────────────────────────────────────────

    def _build_left(self, parent):
        # File selection
        sec = self._section(parent, "📂  Input File")
        self.file_label = ctk.CTkLabel(sec, text="No file selected",
                                        font=("Segoe UI", 13, "bold"), text_color=TEXT_WH,
                                        wraplength=350, justify="left")
        self.file_label.pack(anchor="w", pady=(4, 2))
        file_btn_row = ctk.CTkFrame(sec, fg_color="transparent")
        file_btn_row.pack(fill="x", pady=(0, 4))
        file_btn_row.columnconfigure(0, weight=1)
        self.file_btn = ctk.CTkButton(
            file_btn_row, text="Select JobTime File…", command=self.browse_file,
            fg_color=OK_GRN, hover_color="#2F855A",
            text_color="#000000", font=("Segoe UI", 12, "bold"), height=36, corner_radius=8,
        )
        self.file_btn.grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ctk.CTkButton(
            file_btn_row, text="✕ Clear", command=self.clear_input_file,
            fg_color="#C53030", hover_color="#9B2C2C",
            font=("Segoe UI", 11, "bold"), height=36, width=72, corner_radius=8,
        ).grid(row=0, column=1)

        # Distribution mapping file — custom header so we can add ⓘ button
        _dist_outer = ctk.CTkFrame(parent, fg_color=BG_CARD, corner_radius=10)
        _dist_outer.pack(fill="x", padx=12, pady=(8, 0))
        _dist_hdr = ctk.CTkFrame(_dist_outer, fg_color="transparent")
        _dist_hdr.pack(fill="x", padx=12, pady=(8, 2))
        ctk.CTkLabel(_dist_hdr, text="🗂️  Distribution Mapping File",
                     font=("Segoe UI", 12, "bold"), text_color=TEXT_WH).pack(side="left")
        ctk.CTkButton(
            _dist_hdr, text="ℹ", command=self._show_dist_info,
            fg_color=OK_GRN, hover_color="#2F855A",
            text_color="#000000", font=("Segoe UI", 11, "bold"),
            height=22, width=28, corner_radius=6,
        ).pack(side="left", padx=(8, 0))
        sec_dist = ctk.CTkFrame(_dist_outer, fg_color="transparent")
        sec_dist.pack(fill="x", padx=8, pady=(0, 8))

        self.dist_label = ctk.CTkLabel(sec_dist, text="No file selected",
                                        font=("Segoe UI", 11), text_color=TEXT_DIM,
                                        wraplength=350, justify="left")
        self.dist_label.pack(anchor="w", pady=(4, 2))
        dist_btn_row = ctk.CTkFrame(sec_dist, fg_color="transparent")
        dist_btn_row.pack(fill="x", pady=(0, 4))
        dist_btn_row.columnconfigure(0, weight=1)
        ctk.CTkButton(
            dist_btn_row, text="Select Distribution File…", command=self.browse_dist_file,
            fg_color=OK_GRN, hover_color="#2F855A",
            text_color="#000000", font=("Segoe UI", 11, "bold"), height=30, corner_radius=8,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ctk.CTkButton(
            dist_btn_row, text="✕ Remove", command=self.clear_dist_file,
            fg_color="#C53030", hover_color="#9B2C2C",
            font=("Segoe UI", 11, "bold"), height=30, width=80, corner_radius=8,
        ).grid(row=0, column=1)

        # Period settings
        sec2 = self._section(parent, "📅  Pay Period Settings")
        sec2.columnconfigure(1, weight=1)

        self._lbl(sec2, "Period End Date:", 0, 0)
        self.perend_var = tk.StringVar()
        ctk.CTkEntry(sec2, textvariable=self.perend_var, height=30,
                     placeholder_text="YYYY-MM-DD", font=("Segoe UI", 11)
                     ).grid(row=0, column=1, sticky="ew", pady=3)

        self._lbl(sec2, "Timecard # (run):", 1, 0)
        self.tc_code_var = tk.StringVar()
        ctk.CTkEntry(sec2, textvariable=self.tc_code_var, height=30,
                     placeholder_text="e.g. PP04", font=("Segoe UI", 11)
                     ).grid(row=1, column=1, sticky="ew", pady=3)

        self._lbl(sec2, "Description:", 2, 0)
        self.tc_desc_var = tk.StringVar()
        ctk.CTkEntry(sec2, textvariable=self.tc_desc_var, height=30,
                     placeholder_text="e.g. Jan24-Feb06", font=("Segoe UI", 11)
                     ).grid(row=2, column=1, sticky="ew", pady=3)


        # Date filter
        sec3 = self._section(parent, "🗓️  Date Filter  (optional)")
        sec3.columnconfigure(1, weight=1)
        self._lbl(sec3, "From:", 0, 0)
        self.date_from_var = tk.StringVar()
        ctk.CTkEntry(sec3, textvariable=self.date_from_var, height=28,
                     placeholder_text="YYYY-MM-DD  (blank = all)", font=("Segoe UI", 11)
                     ).grid(row=0, column=1, sticky="ew", pady=2)
        self._lbl(sec3, "To:", 1, 0)
        self.date_to_var = tk.StringVar()
        ctk.CTkEntry(sec3, textvariable=self.date_to_var, height=28,
                     placeholder_text="YYYY-MM-DD  (blank = all)", font=("Segoe UI", 11)
                     ).grid(row=1, column=1, sticky="ew", pady=2)

        # Pay code mappings
        sec4 = self._section(parent, "🔁  Pay Code Mappings  (JobTime → Sage 300)")

        hdr_row = ctk.CTkFrame(sec4, fg_color="transparent")
        hdr_row.pack(fill="x")
        for col_idx, (txt, w) in enumerate([
            ("", 20), ("JobTime Pay Code", 140), ("Sage EARNDED", 62),
            ("Linenum", 62), ("On", 28), ("", 24)
        ]):
            ctk.CTkLabel(hdr_row, text=txt, font=("Segoe UI", 10, "bold"),
                         text_color=TEXT_DIM, width=w
                         ).grid(row=0, column=col_idx, padx=3, pady=(0, 2), sticky="w")

        self.map_frame = ctk.CTkScrollableFrame(sec4, fg_color="transparent", height=160)
        self.map_frame.pack(fill="x")

        btn_row = ctk.CTkFrame(sec4, fg_color="transparent")
        btn_row.pack(fill="x", pady=(4, 0))
        ctk.CTkButton(btn_row, text="+ Add Row", command=self._add_mapping_row,
                      height=28, width=90, fg_color=OK_GRN, hover_color="#276749",
                      font=("Segoe UI", 11, "bold"), text_color="#000000"
                      ).pack(side="left", padx=3)
        ctk.CTkButton(btn_row, text="💾 Save Settings", command=self.save_settings,
                      height=28, fg_color=ACCENT, hover_color=ACCENT2,
                      font=("Segoe UI", 11)).pack(side="right", padx=3)

    # ── Right panel (tabbed) ──────────────────────────────────────────────────

    def _build_right(self, parent):
        parent.rowconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)

        self.tabview = ctk.CTkTabview(
            parent, fg_color=BG_DARK, corner_radius=0,
            segmented_button_fg_color=BG_MID,
            segmented_button_selected_color=OK_GRN,
            segmented_button_selected_hover_color="#2F855A",
            segmented_button_unselected_color=BG_MID,
            segmented_button_unselected_hover_color=BG_CARD,
            text_color=TEXT_WH,
            text_color_disabled=TEXT_DIM,
        )
        self.tabview.grid(row=0, column=0, sticky="nsew")

        self._build_raw_tab    (self.tabview.add("📄  Raw Input"))
        self._build_preview_tab(self.tabview.add("📊  Export Preview"))

        # Round the tab buttons to match the rest of the UI
        try:
            self.tabview._segmented_button.configure(corner_radius=8, border_width=0)
        except Exception:
            pass

    def _make_treeview(self, parent) -> ttk.Treeview:
        style = ttk.Style()
        style.theme_use("default")
        style.configure("Daddy.Treeview",
                        background="#FFFFFF", foreground="#000000",
                        fieldbackground="#FFFFFF", rowheight=30, font=("Segoe UI", 13))
        style.configure("Daddy.Treeview.Heading",
                        background="#0F3460", foreground="#FFFFFF",
                        font=("Segoe UI", 13, "bold"), relief="flat")
        style.map("Daddy.Treeview",
                  background=[("selected", "#2B6CB0")],
                  foreground=[("selected", "#FFFFFF")])
        tree = ttk.Treeview(parent, style="Daddy.Treeview",
                            show="headings", selectmode="browse")
        vsb = ttk.Scrollbar(parent, orient="vertical",   command=tree.yview)
        hsb = ttk.Scrollbar(parent, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid (row=0, column=1, sticky="ns")
        hsb.grid (row=1, column=0, sticky="ew")
        return tree

    def _build_raw_tab(self, tab):
        tab.rowconfigure(2, weight=1)
        tab.columnconfigure(0, weight=1)

        top = ctk.CTkFrame(tab, fg_color="transparent", height=28)
        top.grid(row=0, column=0, sticky="ew", padx=8, pady=(4, 0))
        top.grid_propagate(False)
        self.summary_label = ctk.CTkLabel(
            top, text="No file loaded", font=("Segoe UI", 11), text_color=TEXT_DIM)
        self.summary_label.pack(side="right")

        self.raw_match_label = self._build_search_bar(tab, row=1, var=self.raw_search_var)

        tv = ctk.CTkFrame(tab, fg_color=BG_DARK, corner_radius=0)
        tv.grid(row=2, column=0, sticky="nsew", padx=8, pady=(2, 8))
        tv.rowconfigure(0, weight=1)
        tv.columnconfigure(0, weight=1)
        self.tree = self._make_treeview(tv)

    def _build_preview_tab(self, tab):
        tab.rowconfigure(3, weight=1)
        tab.columnconfigure(0, weight=1)

        # Control bar
        ctrl = ctk.CTkFrame(tab, fg_color="transparent")
        ctrl.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 0))

        self.gen_preview_btn = ctk.CTkButton(
            ctrl, text="▶  Generate Preview",
            command=self._start_export_preview,
            fg_color=OK_GRN, hover_color="#2F855A",
            text_color="#000000", font=("Segoe UI", 12, "bold"), height=34, width=190, corner_radius=8,
        )
        self.gen_preview_btn.pack(side="left")

        ctk.CTkLabel(ctrl, text="No file is saved — preview only.",
                     font=("Segoe UI", 10), text_color=TEXT_WH
                     ).pack(side="left", padx=8)

        # Header / Detail toggle (var lives in __init__)
        ctk.CTkSegmentedButton(
            ctrl,
            values=["Timecard_Header", "Timecard_Detail", "Excluded Rows"],
            variable=self.preview_mode_var,
            command=self._switch_preview_mode,
            font=("Segoe UI", 11, "bold"),
            fg_color=BG_CARD, selected_color=OK_GRN, text_color=TEXT_WH,
            selected_hover_color="#2F855A", unselected_color=BG_CARD,
            unselected_hover_color=BG_MID, corner_radius=8,
        ).pack(side="right", padx=(0, 6))

        # Summary card
        self.preview_summary_frame = ctk.CTkFrame(
            tab, fg_color=BG_CARD, corner_radius=8, height=46)
        self.preview_summary_frame.grid(row=1, column=0, sticky="ew", padx=8, pady=(6, 0))
        self.preview_summary_frame.grid_propagate(False)
        self.preview_summary_label = ctk.CTkLabel(
            self.preview_summary_frame,
            text="Click  ▶ Generate Preview  to see what will be exported.",
            font=("Segoe UI", 11), text_color=TEXT_WH, justify="left",
        )
        self.preview_summary_label.pack(anchor="w", padx=14, pady=12)

        self.preview_match_label = self._build_search_bar(tab, row=2, var=self.preview_search_var)

        # Preview treeview
        tv = ctk.CTkFrame(tab, fg_color=BG_DARK, corner_radius=0)
        tv.grid(row=3, column=0, sticky="nsew", padx=8, pady=(2, 8))
        tv.rowconfigure(0, weight=1)
        tv.columnconfigure(0, weight=1)
        self.preview_tree = self._make_treeview(tv)

    def _build_search_bar(self, tab, row: int, var: tk.StringVar) -> ctk.CTkLabel:
        """Build a search bar row. Returns the match-count label."""
        PLACEHOLDER = "Search"
        outer = ctk.CTkFrame(tab, fg_color="transparent")
        outer.grid(row=row, column=0, sticky="ew", padx=8, pady=(4, 0))
        outer.columnconfigure(0, weight=1)

        pill = tk.Frame(outer, bg="#FFFFFF", highlightbackground="#CCCCCC",
                        highlightthickness=1, bd=0)
        pill.grid(row=0, column=0, sticky="ew", ipady=4)
        pill.columnconfigure(1, weight=1)

        tk.Label(pill, text="🔍", bg="#FFFFFF", fg="#888888",
                 font=("Segoe UI", 11)).grid(row=0, column=0, padx=(8, 2))

        entry = tk.Entry(pill, bg="#FFFFFF", fg="#888888",
                         insertbackground="#000000",
                         relief="flat", bd=0, font=("Segoe UI", 12))
        entry.grid(row=0, column=1, sticky="ew", padx=(0, 8), pady=2)
        entry.insert(0, PLACEHOLDER)

        in_placeholder = [True]
        syncing        = [False]

        def _show_placeholder():
            in_placeholder[0] = True
            entry.delete(0, "end")
            entry.insert(0, PLACEHOLDER)
            entry.config(fg="#888888")

        def _on_focus_in(_):
            if in_placeholder[0]:
                in_placeholder[0] = False
                entry.delete(0, "end")
                entry.config(fg="#000000")

        def _on_focus_out(_):
            if not entry.get().strip():
                _show_placeholder()

        def _on_key(_):
            if not in_placeholder[0]:
                syncing[0] = True
                var.set(entry.get())
                syncing[0] = False

        def _on_var_change(*_):
            if syncing[0]:
                return
            if var.get() == "":
                _show_placeholder()

        entry.bind("<FocusIn>",    _on_focus_in)
        entry.bind("<FocusOut>",   _on_focus_out)
        entry.bind("<KeyRelease>", _on_key)
        entry.bind("<Escape>",     lambda _: (var.set(""), _show_placeholder()))
        entry.bind("<Control-a>",  lambda _: (entry.select_range(0, "end"), "break")[1])
        var.trace_add("write", _on_var_change)

        match_lbl = ctk.CTkLabel(outer, text="", font=("Segoe UI", 10),
                                 text_color=TEXT_DIM, width=80, anchor="e")
        match_lbl.grid(row=0, column=1, padx=(6, 0))

        self.bind("<Control-f>", lambda _: entry.focus_set())
        return match_lbl

    # ── Action bar ────────────────────────────────────────────────────────────

    def _build_action_bar(self):
        bar = ctk.CTkFrame(self, fg_color=BG_MID, height=56, corner_radius=0)
        bar.pack(fill="x", side="bottom")
        bar.pack_propagate(False)

        self.status_dot = ctk.CTkLabel(bar, text="●", font=("Segoe UI", 16),
                                        text_color=OK_GRN)
        self.status_dot.pack(side="left", padx=(14, 4), pady=12)
        self.status_label = ctk.CTkLabel(bar, text="Ready",
                                          font=("Segoe UI", 11), text_color=TEXT_DIM)
        self.status_label.pack(side="left", pady=12)

        self.process_btn = ctk.CTkButton(
            bar, text="⚙  Process & Export →",
            command=self._start_process,
            fg_color=OK_GRN, hover_color="#2F855A",
            text_color="#000000", font=("Segoe UI", 13, "bold"),
            height=38, width=200, corner_radius=8,
        )
        self.process_btn.pack(side="right", padx=14, pady=9)

        _BTN_GRN  = OK_GRN
        _BTN_HOVER = "#2F855A"

        ctk.CTkButton(
            bar, text="📄  Log",
            command=self._open_log,
            fg_color=_BTN_GRN, hover_color=_BTN_HOVER,
            text_color="#000000", font=("Segoe UI", 11, "bold"),
            height=38, width=80, corner_radius=8,
        ).pack(side="right", padx=(0, 4), pady=9)

        ctk.CTkButton(
            bar, text="⚠  Issues",
            command=self._show_issues_window,
            fg_color=_BTN_GRN, hover_color=_BTN_HOVER,
            text_color="#000000", font=("Segoe UI", 11, "bold"),
            height=38, width=100, corner_radius=8,
        ).pack(side="right", padx=(0, 4), pady=9)

        self.log_info_btn = ctk.CTkButton(
            bar, text="ℹ",
            command=self._show_log_info,
            fg_color=_BTN_GRN, hover_color=_BTN_HOVER,
            text_color="#000000", font=("Segoe UI", 11, "bold"),
            height=38, width=42, corner_radius=8,
        )
        self.log_info_btn.pack(side="right", padx=(0, 4), pady=9)

    # ══════════════════════════════════════════════════════════════════════════
    #  CONFIG ↔ UI
    # ══════════════════════════════════════════════════════════════════════════

    def _load_config_into_ui(self):
        # Period fields stay blank until a file is loaded and auto-populates them
        for m in self.config_mgr.pay_code_mappings:
            self._add_mapping_row(m)
        dist_path = self.config_mgr.get("dist_file_path", "")
        if dist_path and os.path.exists(dist_path):
            self.dist_label.configure(
                text=os.path.basename(dist_path), text_color=TEXT_WH)
            self._load_dist_map(dist_path)
        elif dist_path:
            self.dist_label.configure(
                text=f"⚠ Not found: {os.path.basename(dist_path)}", text_color=WARN_YLW)

    def _collect_config_from_ui(self):
        self.config_mgr.set("perend_date",   self.perend_var.get().strip())
        self.config_mgr.set("timecard_code", self.tc_code_var.get().strip())
        self.config_mgr.set("timecard_desc", self.tc_desc_var.get().strip())
        self.config_mgr.pay_code_mappings = [r.to_dict() for r in self.mapping_rows]

    def _add_mapping_row(self, data: dict | None = None):
        if data is None:
            data = {"jobtime_code": "", "earnded": "", "enabled": True}
        mr = MappingRow(self.map_frame, len(self.mapping_rows) + 1, data,
                        self._remove_mapping_row)
        self.mapping_rows.append(mr)
        mr.handle.bind("<Button-1>",        lambda e, m=mr: self._drag_start(e, m))
        mr.handle.bind("<B1-Motion>",       lambda e:       self._drag_motion(e))
        mr.handle.bind("<ButtonRelease-1>", lambda _:       self._drag_end())

    def _remove_mapping_row(self, mr: MappingRow):
        self.mapping_rows.remove(mr)
        for idx, r in enumerate(self.mapping_rows, 1):
            r.regrid(idx)

    def _drag_start(self, event, mr: MappingRow):
        self._drag_mr = mr
        self._drag_insert_idx = self.mapping_rows.index(mr)

        # Dim the source row as a placeholder — nothing moves until drop
        mr.handle.configure(fg=BG_MID, bg=BG_CARD)
        for entry in (mr.jt_entry, mr.earnded_entry, mr.linenum_entry):
            try:
                entry.configure(fg_color="#1E2124", text_color="#444444")
            except Exception:
                pass

        # Ghost uses plain tk.Labels (CTkEntry won't render in detached Toplevel)
        ghost = tk.Toplevel(self)
        ghost.overrideredirect(True)
        ghost.attributes("-topmost", True)
        ghost.configure(bg=ACCENT2)
        inner = tk.Frame(ghost, bg=ACCENT2, bd=0)
        inner.pack(padx=3, pady=3)
        lbl_cfg = dict(bg=ACCENT2, fg=TEXT_WH, font=("Segoe UI", 11), pady=4)
        tk.Label(inner, text="⠿",                         width=2,  **lbl_cfg).grid(row=0, column=0, padx=2)
        tk.Label(inner, text=mr.jt_var.get() or "—",      width=20, anchor="w", **lbl_cfg).grid(row=0, column=1, padx=2)
        tk.Label(inner, text=mr.earnded_var.get() or "—", width=8,  **lbl_cfg).grid(row=0, column=2, padx=2)
        tk.Label(inner, text=mr.linenum_var.get() or "—", width=8,  **lbl_cfg).grid(row=0, column=3, padx=2)
        ghost.geometry(f"+{event.x_root + 12}+{event.y_root - 14}")
        ghost.update_idletasks()
        self._drag_ghost = ghost

        # Blue insertion line placed on self (avoids CTkScrollableFrame internals)
        self._drag_line = tk.Frame(self, bg=ACCENT2, height=2)

    def _drag_motion(self, event):
        if not self._drag_mr:
            return

        if self._drag_ghost:
            self._drag_ghost.geometry(f"+{event.x_root + 12}+{event.y_root - 14}")

        # Compute insertion index — don't move any rows
        mouse_y = event.y_root
        new_idx = len(self.mapping_rows) - 1
        for i, r in enumerate(self.mapping_rows):
            if r is self._drag_mr:
                continue
            mid = r.handle.winfo_rooty() + r.handle.winfo_height() // 2
            if mouse_y < mid:
                new_idx = i
                break
        self._drag_insert_idx = new_idx

        # Move insertion line to show drop target
        if self._drag_line:
            rows = [r for r in self.mapping_rows if r is not self._drag_mr]
            if new_idx == 0 or not rows:
                ref = self.mapping_rows[0] if self.mapping_rows else None
                line_root_y = ref.handle.winfo_rooty() if ref else event.y_root
            else:
                above = [r for r in self.mapping_rows[:new_idx] if r is not self._drag_mr]
                ref = above[-1] if above else self.mapping_rows[0]
                line_root_y = ref.handle.winfo_rooty() + ref.handle.winfo_height()

            win_y    = line_root_y - self.winfo_rooty()
            win_x    = self._drag_mr.handle.winfo_rootx() - self.winfo_rootx()
            line_w   = sum(w.winfo_width() for w in self._drag_mr.widgets)
            self._drag_line.place(x=win_x, y=win_y, width=line_w, height=2)

    def _drag_end(self):
        if not self._drag_mr:
            return
        mr = self._drag_mr

        if self._drag_ghost:
            self._drag_ghost.destroy()
            self._drag_ghost = None
        if self._drag_line:
            self._drag_line.place_forget()
            self._drag_line.destroy()
            self._drag_line = None

        # Now do the actual reorder in one shot
        cur_idx = self.mapping_rows.index(mr)
        tgt = self._drag_insert_idx
        if tgt != cur_idx:
            self.mapping_rows.pop(cur_idx)
            self.mapping_rows.insert(tgt, mr)
            for i, r in enumerate(self.mapping_rows, 1):
                r.regrid(i)

        mr.handle.configure(fg=TEXT_DIM, bg=BG_CARD)
        for entry in (mr.jt_entry, mr.earnded_entry, mr.linenum_entry):
            try:
                entry.configure(fg_color=["#F9F9FA", "#343638"], text_color=["#1A1A1A", "#E0E0E0"])
            except Exception:
                pass
        self._drag_mr = None

    def _sync_paycodes_from_file(self, df: pd.DataFrame):
        """Add any pay codes found in the file that aren't already in the mapping table."""
        pc_col = self.col_map.get('paycode')
        if not pc_col or pc_col not in df.columns:
            return
        file_codes = sorted(
            {str(v).strip() for v in df[pc_col].dropna() if str(v).strip() and str(v).strip() != 'nan'}
        )
        existing = {r.jt_var.get().strip() for r in self.mapping_rows}
        next_linenum = (max((r.to_dict()['linenum'] for r in self.mapping_rows), default=0) + 1000)
        added = 0
        for code in file_codes:
            if code not in existing:
                self._add_mapping_row({
                    "jobtime_code": code,
                    "earnded":      "",
                    "linenum":      next_linenum,
                    "enabled":      True,
                })
                next_linenum += 1000
                added += 1
        return added

    def save_settings(self):
        self._collect_config_from_ui()
        if self.config_mgr.save():
            self.update_status("Settings saved.", "ok")
        else:
            self.update_status("Failed to save settings — check log.", "error")

    # ══════════════════════════════════════════════════════════════════════════
    #  FILE LOADING & RAW PREVIEW
    # ══════════════════════════════════════════════════════════════════════════

    def browse_file(self):
        path = filedialog.askopenfilename(
            title="Select JobTime Export File",
            filetypes=[
                ("Excel / CSV", "*.xlsx *.xls *.xlsm *.csv"),
                ("All files",   "*.*"),
            ]
        )
        if path:
            self.load_file(path)

    def _show_dist_info(self):
        popup = tk.Toplevel(self)
        popup.overrideredirect(True)
        popup.attributes("-topmost", True)
        popup.configure(bg=BG_CARD)

        # Position near the ⓘ button (roughly top-left area of left panel)
        popup.update_idletasks()
        x = self.winfo_rootx() + 30
        y = self.winfo_rooty() + 200
        popup.geometry(f"370x310+{x}+{y}")

        # Card border effect
        border = tk.Frame(popup, bg=ACCENT2, bd=0)
        border.place(relx=0, rely=0, relwidth=1, relheight=1)
        card = tk.Frame(border, bg=BG_CARD)
        card.place(x=1, y=1, relwidth=1, relheight=1, width=-2, height=-2)

        # Header
        hdr = tk.Frame(card, bg=BG_MID)
        hdr.pack(fill="x")
        tk.Label(hdr, text="🗂️  Distribution Mapping File",
                 bg=BG_MID, fg=TEXT_WH, font=("Segoe UI", 11, "bold"),
                 padx=12, pady=8).pack(side="left")
        tk.Button(hdr, text="✕", bg=BG_MID, fg=TEXT_DIM,
                  activebackground="#C53030", activeforeground=TEXT_WH,
                  font=("Segoe UI", 10, "bold"), bd=0, padx=8, pady=6,
                  cursor="hand2", command=popup.destroy).pack(side="right")

        # Body text
        body = tk.Frame(card, bg=BG_CARD)
        body.pack(fill="both", expand=True, padx=14, pady=10)

        info_text = (
            "This file maps each employee to their G/L distribution codes "
            "used when exporting to Sage 300.\n\n"
            "Keep this file up to date by:\n"
            "  •  Adding a row for every new hire before their first pay period is processed.\n"
            "  •  Updating the G/L codes if an employee changes departments or cost centres.\n"
            "  •  Removing or deactivating rows for terminated employees.\n\n"
            "If an employee appears in the timecard but is missing from this file, "
            "Vectis will block the export and flag those employees as errors."
        )
        tk.Label(body, text=info_text, bg=BG_CARD, fg=TEXT_DIM,
                 font=("Segoe UI", 10), wraplength=336, justify="left",
                 anchor="nw").pack(fill="both", expand=True)

        # Close when clicking anywhere outside the popup
        def _on_click_outside(e):
            wx, wy = popup.winfo_rootx(), popup.winfo_rooty()
            ww, wh = popup.winfo_width(), popup.winfo_height()
            if not (wx <= e.x_root <= wx + ww and wy <= e.y_root <= wy + wh):
                popup.destroy()

        self.bind("<Button-1>", _on_click_outside, add="+")
        popup.bind("<Destroy>", lambda _: self.unbind("<Button-1>"))

        popup.focus_set()

    def browse_dist_file(self):
        path = filedialog.askopenfilename(
            title="Select Distribution Mapping File",
            filetypes=[
                ("Excel files", "*.xlsx *.xls *.xlsm"),
                ("All files",   "*.*"),
            ]
        )
        if path:
            self.config_mgr.set("dist_file_path", path)
            self.config_mgr.save()
            self.dist_label.configure(text=os.path.basename(path), text_color=TEXT_WH)
            self._load_dist_map(path)

    def _load_dist_map(self, path: str):
        try:
            self.dist_map = load_distribution_map(path)
            # If a JobTime file is already loaded, recheck immediately
            if self.raw_df is not None:
                self._dist_anomaly_map = flag_dist_anomalies(
                    self.raw_df, self.col_map, self.dist_map)
                self._anomaly_map = {
                    **self._data_anomaly_map,
                    **self._dist_anomaly_map,
                    **self._unmapped_anomaly_map,
                }
                self._refresh_raw_preview(self.raw_df)

            ndist = len(self._get_dist_problem_employees())
            if ndist:
                self.update_status(
                    f"Distribution map loaded: {len(self.dist_map)} employees.  "
                    f"⛔ {ndist} employee{'s' if ndist != 1 else ''} not in file — "
                    f"export blocked. Rows highlighted in red.",
                    "error")
            else:
                self.update_status(
                    f"Distribution map loaded: {len(self.dist_map)} employees.", "ok")
        except Exception as e:
            self.dist_map = {}
            self.update_status(f"Distribution file error: {e}", "error")
            messagebox.showerror("Distribution File Error", str(e))

    def clear_input_file(self):
        self.raw_df                 = None
        self.col_map                = {}
        self._raw_display_df        = None
        self._data_anomaly_map      = {}
        self._dist_anomaly_map      = {}
        self._unmapped_anomaly_map  = {}
        self._anomaly_map           = {}
        self._anomaly_items         = {}
        self._preview_header_df     = None
        self._preview_detail_df     = None
        self._preview_excluded_df   = None
        self.file_label.configure(text="No file selected", text_color=TEXT_WH)
        self.perend_var.set("")
        self.tc_code_var.set("")
        self.tc_desc_var.set("")
        self.raw_search_var.set("")
        self.raw_match_label.configure(text="")
        self.preview_search_var.set("")
        self.preview_match_label.configure(text="")
        self.summary_label.configure(text="No file loaded")
        self.tree.delete(*self.tree.get_children())
        self.preview_tree.delete(*self.preview_tree.get_children())
        self.preview_summary_label.configure(
            text="Click  ▶ Generate Preview  to see what will be exported.",
            text_color=TEXT_DIM)
        self.update_status("Input file cleared.", "ok")

    def clear_dist_file(self):
        self.dist_map          = {}
        self._dist_anomaly_map = {}
        self._anomaly_map      = {**self._data_anomaly_map, **self._unmapped_anomaly_map}
        self.config_mgr.set("dist_file_path", "")
        self.config_mgr.save()
        self.dist_label.configure(text="No file selected", text_color=TEXT_DIM)
        if self.raw_df is not None:
            self._refresh_raw_preview(self.raw_df)
        self.update_status("Distribution file removed.", "ok")

    def load_file(self, path: str):
        self.update_status(f"Loading {os.path.basename(path)}…", "warn")
        self.file_label.configure(text=os.path.basename(path))
        try:
            df = read_input_file(path)
            self.raw_df            = df
            self.col_map           = detect_columns(df)
            self._data_anomaly_map     = flag_anomalies(df, self.col_map)
            self._dist_anomaly_map     = (
                flag_dist_anomalies(df, self.col_map, self.dist_map)
                if self.dist_map else {}
            )
            self._unmapped_anomaly_map = flag_unmapped_paycodes_rows(df, self.col_map, self.config_mgr)
            self._anomaly_map = {
                **self._data_anomaly_map,
                **self._dist_anomaly_map,
                **self._unmapped_anomaly_map,
            }
            warnings         = validate_input(df, self.col_map)
            self._refresh_raw_preview(df)
            self._auto_populate_description(df)
            self._sync_paycodes_from_file(df)

            msg = f"Loaded {len(df):,} rows."
            nd  = len(self._data_anomaly_map)
            ndist = len(self._get_dist_problem_employees())
            if ndist:
                self.update_status(
                    f"{msg}  ⛔ {ndist} employee{'s' if ndist != 1 else ''} not in "
                    f"distribution file — export blocked. Rows highlighted in red.",
                    "error")
            elif nd:
                self.update_status(
                    f"{msg}  ⚠ {nd} anomalous row{'s' if nd != 1 else ''} highlighted in red.",
                    "warn")
            elif warnings:
                self.update_status(msg + "  ⚠ " + warnings[0], "warn")
            else:
                self.update_status(msg, "ok")
        except Exception as e:
            logging.exception("Load failed")
            messagebox.showerror("Load Error", str(e))
            self.update_status(f"Load failed: {e}", "error")

    def _auto_populate_description(self, df: pd.DataFrame):
        """Auto-fill Description, Period End Date, and Timecard Run # from the file's date range."""
        date_col = self.col_map.get('work_date')
        if not date_col or date_col not in df.columns:
            return
        dates = pd.to_datetime(df[date_col], errors='coerce').dropna()
        if dates.empty:
            return
        min_d = dates.min()
        max_d = dates.max()

        # Description: date range of work dates in the file
        if min_d == max_d:
            desc = min_d.strftime("%b %d").replace(" 0", " ")
        else:
            desc = f"{min_d.strftime('%b %d').replace(' 0', ' ')} - {max_d.strftime('%b %d').replace(' 0', ' ')}"
        self.tc_desc_var.set(desc)

        # Timecard Run # and Period End Date:
        # Find the next pay date ON OR AFTER the latest work date in the file.
        # e.g. work dates ending Apr 3 → next pay date Apr 10 → PP8
        anchor_str = self.config_mgr.get('payroll_anchor', '2026-01-02')
        try:
            anchor = datetime.strptime(anchor_str, '%Y-%m-%d').date()
            work_end = max_d.date()
            delta = (work_end - anchor).days
            if delta < 0:
                run_num = 1
                pay_date = anchor
            else:
                periods_floor = delta // 14
                pay_date = anchor + timedelta(days=periods_floor * 14)
                if pay_date < work_end:
                    periods_floor += 1
                    pay_date = anchor + timedelta(days=periods_floor * 14)
                run_num = periods_floor + 1

            self.tc_code_var.set(f"PP{run_num:02d}")
            self.perend_var.set(pay_date.strftime('%Y-%m-%d'))
        except Exception:
            pass

    def _refresh_raw_preview(self, df: pd.DataFrame, max_rows: int = 200):
        # Float anomalous rows to the top so they're always visible
        if self._anomaly_map:
            bad_idx  = [i for i in df.index if i in self._anomaly_map]
            good_idx = [i for i in df.index if i not in self._anomaly_map]
            display_df = pd.concat([df.loc[bad_idx], df.loc[good_idx]]).head(max_rows)
        else:
            display_df = df.head(max_rows)
        self._raw_display_df = display_df
        self.summary_label.configure(
            text=f"Showing {min(max_rows, len(df)):,} of {len(df):,} rows")
        # Reset search; the trace triggers _on_raw_search_change → _populate_tree
        if self.raw_search_var.get() != "":
            self.raw_search_var.set("")
        else:
            self._on_raw_search_change()
        # Clear stale export preview
        self._preview_header_df = None
        self._preview_detail_df = None
        self.preview_tree.delete(*self.preview_tree.get_children())
        self.preview_summary_label.configure(
            text="Click  ▶ Generate Preview  to see what will be exported.",
            text_color=TEXT_DIM)


    def _get_dist_problem_employees(self) -> dict:
        """Return {emp_id: full_display_string} for employees missing from the distribution file."""
        emp_col = self.col_map.get('employee')
        if not emp_col or self.raw_df is None:
            return {}
        emp_display = {}
        for idx in self._dist_anomaly_map:
            try:
                raw    = str(self.raw_df.at[idx, emp_col]).strip()
                emp_id = raw.split(' - ')[0].strip() if ' - ' in raw else raw
                if emp_id not in emp_display:
                    emp_display[emp_id] = raw   # e.g. "20250130 - Antenor, Mike Arnold"
            except Exception:
                pass
        return emp_display

    def _get_data_problem_employees(self) -> dict:
        """Return {emp_id: full_display_string} for employees with data anomalies."""
        emp_col = self.col_map.get('employee')
        if not emp_col or self.raw_df is None:
            return {}
        emp_display = {}
        for idx in self._data_anomaly_map:
            try:
                raw = str(self.raw_df.at[idx, emp_col]).strip()
                if raw and raw != 'nan':
                    emp_id = raw.split(' - ')[0].strip() if ' - ' in raw else raw
                    if emp_id not in emp_display:
                        emp_display[emp_id] = raw
            except Exception:
                pass
        return emp_display

    def _show_issues_window(self):
        """Open a window listing every unique highlighted employee, deduplicated."""
        if self.raw_df is None:
            messagebox.showinfo("Issues", "Load a JobTime file first.")
            return

        dist_emps = self._get_dist_problem_employees()
        data_emps = self._get_data_problem_employees()

        if not dist_emps and not data_emps:
            messagebox.showinfo("Issues", "No issues detected — all rows look good.")
            return

        win = ctk.CTkToplevel(self)
        win.title("Highlighted Employees")
        win.geometry("660x600")
        win.configure(fg_color=BG_DARK)
        win.resizable(True, True)
        win.minsize(480, 300)
        win.lift()
        win.focus_force()
        win.grab_set()

        hdr = ctk.CTkFrame(win, fg_color=BG_MID, height=52, corner_radius=0)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        ctk.CTkLabel(hdr, text="Highlighted Employees",
                     font=("Segoe UI", 15, "bold"), text_color=TEXT_WH
                     ).pack(side="left", padx=16, pady=14)
        ctk.CTkLabel(hdr, text="Drag or Shift-click to select  •  Ctrl+C to copy  •  Right-click for menu",
                     font=("Segoe UI", 9), text_color=TEXT_DIM
                     ).pack(side="left", padx=8)

        # Plain tk.Frame so mouse drag events reach the treeview uninterrupted
        body = tk.Frame(win, bg=BG_DARK)
        body.pack(fill="both", expand=True)

        _style = ttk.Style()
        _style.configure("Issues.Treeview",
                         background="#FFFFFF", foreground="#000000",
                         fieldbackground="#FFFFFF", rowheight=26,
                         font=("Segoe UI", 11))
        _style.configure("Issues.Treeview.Heading",
                         background=BG_CARD, foreground="#FFFFFF",
                         font=("Segoe UI", 11, "bold"), relief="flat")
        _style.map("Issues.Treeview",
                   background=[("selected", ACCENT)],
                   foreground=[("selected", "#FFFFFF")])

        # Build emp_id → name lookup from raw_df name columns
        emp_col = self.col_map.get('employee')
        _name_cache: dict[str, str] = {}
        if emp_col and self.raw_df is not None:
            name_cols = [c for c in self.raw_df.columns
                         if isinstance(c, str) and 'name' in c.lower()]
            if name_cols:
                name_col = name_cols[0]
                for _, row in self.raw_df[[emp_col, name_col]].drop_duplicates().iterrows():
                    eid = str(row[emp_col]).strip()
                    val = str(row[name_col]).strip()
                    if eid and eid != 'nan' and val and val != 'nan' and eid not in _name_cache:
                        _name_cache[eid] = val

        def _get_name(raw_str):
            # Try embedded "ID - Name" format first
            if ' - ' in raw_str:
                return raw_str.split(' - ', 1)[1].strip() or "N/A"
            # Fall back to lookup from a name column in raw_df
            emp_id = raw_str.strip()
            return _name_cache.get(emp_id, "N/A")

        def _copy_rows(tree):
            sel = tree.selection()
            if not sel:
                sel = tree.get_children()
            lines = ["\t".join(str(v) for v in tree.item(i)["values"]) for i in sel]
            win.clipboard_clear()
            win.clipboard_append("\n".join(lines))

        def _show_context_menu(event, tree):
            menu = tk.Menu(win, tearoff=0)
            menu.add_command(label="Copy selected",  command=lambda: _copy_rows(tree))
            menu.add_command(label="Select all",     command=lambda: tree.selection_set(tree.get_children()))
            menu.tk_popup(event.x_root, event.y_root)

        def _make_table(parent, rows, title_text, subtitle_text, title_color):
            outer = tk.Frame(parent, bg=BG_DARK)
            outer.pack(fill="x", padx=12, pady=(12, 0))

            tk.Label(outer, text=title_text, bg=BG_DARK, fg=title_color,
                     font=("Segoe UI", 12, "bold"), wraplength=600,
                     justify="left", anchor="w"
                     ).pack(fill="x", pady=(6, 1))
            tk.Label(outer, text=subtitle_text, bg=BG_DARK, fg=TEXT_DIM,
                     font=("Segoe UI", 10), wraplength=600,
                     justify="left", anchor="w"
                     ).pack(fill="x", pady=(0, 4))

            tv_wrap = tk.Frame(outer, bg=BG_DARK)
            tv_wrap.pack(fill="x")

            tree = ttk.Treeview(tv_wrap, style="Issues.Treeview",
                                columns=("emp_no", "name", "dept"),
                                show="headings",
                                height=min(len(rows), 12),
                                selectmode="extended")
            tree.heading("emp_no", text="Emp No")
            tree.heading("name",   text="Name")
            tree.heading("dept",   text="Department")
            tree.column("emp_no", width=110, minwidth=80,  stretch=False)
            tree.column("name",   width=300, minwidth=120, stretch=True)
            tree.column("dept",   width=140, minwidth=80,  stretch=False)

            vsb = ttk.Scrollbar(tv_wrap, orient="vertical", command=tree.yview)
            tree.configure(yscrollcommand=vsb.set)
            tree.pack(side="left", fill="both", expand=True)
            vsb.pack(side="right", fill="y")

            for emp_id, name, dept in rows:
                tree.insert("", "end", values=(emp_id, name, dept))

            tree.bind("<Control-c>", lambda _: _copy_rows(tree))
            tree.bind("<Control-a>", lambda _: tree.selection_set(tree.get_children()))
            tree.bind("<Button-3>",  lambda e: _show_context_menu(e, tree))

        if dist_emps:
            rows = sorted(
                [(emp_id, _get_name(raw), "Not in dist file")
                 for emp_id, raw in dist_emps.items()]
            )
            _make_table(body, rows,
                        f"Missing distribution mapping  ({len(dist_emps)} employee{'s' if len(dist_emps) != 1 else ''})",
                        "Not in the distribution file — export is blocked until resolved.",
                        ERR_RED)

        if data_emps:
            rows = sorted(
                [(emp_id, _get_name(raw), self.dist_map.get(emp_id, {}).get('distcode', 'N/A'))
                 for emp_id, raw in data_emps.items()]
            )
            _make_table(body, rows,
                        f"Data quality issues  ({len(data_emps)} employee{'s' if len(data_emps) != 1 else ''})",
                        "Rows with bad data (non-numeric hours, missing pay code) — will be skipped.",
                        WARN_YLW)

        ctk.CTkButton(win, text="Close", command=win.destroy,
                      fg_color=BG_CARD, hover_color=BG_MID,
                      font=("Segoe UI", 12), height=36, corner_radius=8
                      ).pack(pady=10, padx=12, fill="x")

    def _block_if_dist_incomplete(self) -> bool:
        """Show a blocking error dialog if distribution anomalies exist. Returns True if blocked."""
        emp_display = self._get_dist_problem_employees()
        if not emp_display:
            return False

        self.tabview.set("📄  Raw Input")

        dlg = ctk.CTkToplevel(self)
        dlg.title("Export Blocked — Distribution Incomplete")
        dlg.geometry("520x520")
        dlg.configure(fg_color=BG_DARK)
        dlg.resizable(False, False)
        dlg.lift()
        dlg.focus_force()
        dlg.grab_set()

        # Header
        hdr = ctk.CTkFrame(dlg, fg_color="#7B1A1A", height=48, corner_radius=0)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        ctk.CTkLabel(hdr, text="⛔  Export Blocked — Distribution Incomplete",
                     font=("Segoe UI", 13, "bold"), text_color="#FFFFFF"
                     ).pack(side="left", padx=14, pady=12)

        body = tk.Frame(dlg, bg=BG_DARK)
        body.pack(fill="both", expand=True, padx=18, pady=12)

        emp_list = "\n".join(f"  •  {v}" for v in sorted(emp_display.values()))
        detail = (
            f"{len(emp_display)} employee(s) are not in the distribution file:\n\n"
            f"{emp_list}\n\n"
            f"Without a distribution code and GL account, Sage 300 will reject the import.\n\n"
            f"To fix:\n"
            f"  1. Add the employee(s) to the distribution file\n"
            f"     (CLASS1 and GL Distribution columns required).\n"
            f"  2. Reload the distribution file."
        )
        tk.Label(body, text=detail, bg=BG_DARK, fg=TEXT_WH,
                 font=("Segoe UI", 10), justify="left", anchor="nw",
                 wraplength=470).pack(fill="x", pady=(0, 14))

        # Override section
        sep = tk.Frame(body, bg="#444466", height=1)
        sep.pack(fill="x", pady=(0, 10))
        tk.Label(body, text="Override (testing only)",
                 bg=BG_DARK, fg=TEXT_DIM, font=("Segoe UI", 10, "bold")
                 ).pack(anchor="w")
        tk.Label(body, text="Enter the override password to force-export anyway:",
                 bg=BG_DARK, fg=TEXT_DIM, font=("Segoe UI", 9)
                 ).pack(anchor="w", pady=(2, 6))

        pwd_var = tk.StringVar()
        pwd_entry = tk.Entry(body, textvariable=pwd_var, show="•",
                             bg="#2A2A4A", fg=TEXT_WH, insertbackground=TEXT_WH,
                             relief="flat", bd=4, font=("Segoe UI", 11), width=24)
        pwd_entry.pack(anchor="w")

        err_label = tk.Label(body, text="", bg=BG_DARK, fg=ERR_RED,
                             font=("Segoe UI", 9))
        err_label.pack(anchor="w", pady=(3, 0))

        result = [True]

        def _try_override():
            if pwd_var.get() == _OVERRIDE_PWD:
                result[0] = False
                dlg.destroy()
            else:
                err_label.configure(text="Incorrect password.")
                pwd_entry.delete(0, "end")
                pwd_entry.focus_set()

        def _cancel():
            dlg.destroy()

        pwd_entry.bind("<Return>", lambda _: _try_override())

        btn_row = tk.Frame(body, bg=BG_DARK)
        btn_row.pack(fill="x", pady=(12, 0))
        ctk.CTkButton(btn_row, text="Force Export", command=_try_override,
                      fg_color="#7B1A1A", hover_color="#9B2A2A",
                      text_color=TEXT_WH, font=("Segoe UI", 11, "bold"),
                      height=36, corner_radius=8
                      ).pack(side="left", padx=(0, 8))
        ctk.CTkButton(btn_row, text="Cancel", command=_cancel,
                      fg_color=BG_CARD, hover_color=BG_MID,
                      text_color=TEXT_WH, font=("Segoe UI", 11),
                      height=36, corner_radius=8
                      ).pack(side="left")

        self.wait_window(dlg)
        return result[0]

    def _block_if_hours_mismatch(self, date_from, date_to) -> bool:
        """Block export if any pay codes in the file have no enabled mapping. Returns True if blocked."""
        report = find_unmapped_paycodes(
            self.raw_df, self.col_map, self.config_mgr, date_from, date_to
        )
        if not report['unmapped']:
            return False

        missing_hrs = round(report['total_hours'] - report['mapped_hours'], 2)
        pc_lines = "\n".join(
            f"  •  \"{pc}\"  —  {hrs:.2f} hrs"
            for pc, hrs in sorted(report['unmapped'].items())
        )

        dlg = ctk.CTkToplevel(self)
        dlg.title("Export Blocked — Hours Mismatch")
        dlg.geometry("520x500")
        dlg.configure(fg_color=BG_DARK)
        dlg.resizable(False, False)
        dlg.lift()
        dlg.focus_force()
        dlg.grab_set()

        hdr = ctk.CTkFrame(dlg, fg_color="#7B4A00", height=48, corner_radius=0)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        ctk.CTkLabel(hdr, text="⚠  Export Blocked — Hours Mismatch",
                     font=("Segoe UI", 13, "bold"), text_color="#FFFFFF"
                     ).pack(side="left", padx=14, pady=12)

        body = tk.Frame(dlg, bg=BG_DARK)
        body.pack(fill="both", expand=True, padx=18, pady=12)

        detail = (
            f"The JobTime file contains pay codes that are not in the app's\n"
            f"Pay Code Mapping, so those hours will NOT appear in the export.\n\n"
            f"  JobTime total hours:   {report['total_hours']:.2f}\n"
            f"  Hours that will export: {report['mapped_hours']:.2f}\n"
            f"  Missing from export:    {missing_hrs:.2f}\n\n"
            f"Unmapped pay codes:\n{pc_lines}\n\n"
            f"To fix: add each pay code above to the Pay Code Mapping\n"
            f"panel and assign it a Sage EARNDED code, then re-run."
        )
        tk.Label(body, text=detail, bg=BG_DARK, fg=TEXT_WH,
                 font=("Segoe UI", 10), justify="left", anchor="nw",
                 wraplength=470).pack(fill="x", pady=(0, 14))

        sep = tk.Frame(body, bg="#444466", height=1)
        sep.pack(fill="x", pady=(0, 10))
        tk.Label(body, text="Override (testing only)",
                 bg=BG_DARK, fg=TEXT_DIM, font=("Segoe UI", 10, "bold")
                 ).pack(anchor="w")
        tk.Label(body, text="Enter the override password to force-export anyway:",
                 bg=BG_DARK, fg=TEXT_DIM, font=("Segoe UI", 9)
                 ).pack(anchor="w", pady=(2, 6))

        pwd_var = tk.StringVar()
        pwd_entry = tk.Entry(body, textvariable=pwd_var, show="•",
                             bg="#2A2A4A", fg=TEXT_WH, insertbackground=TEXT_WH,
                             relief="flat", bd=4, font=("Segoe UI", 11), width=24)
        pwd_entry.pack(anchor="w")

        err_label = tk.Label(body, text="", bg=BG_DARK, fg=ERR_RED, font=("Segoe UI", 9))
        err_label.pack(anchor="w", pady=(3, 0))

        result = [True]

        def _try_override():
            if pwd_var.get() == _OVERRIDE_PWD:
                result[0] = False
                dlg.destroy()
            else:
                err_label.configure(text="Incorrect password.")
                pwd_entry.delete(0, "end")
                pwd_entry.focus_set()

        pwd_entry.bind("<Return>", lambda _: _try_override())

        btn_row = tk.Frame(body, bg=BG_DARK)
        btn_row.pack(fill="x", pady=(12, 0))
        ctk.CTkButton(btn_row, text="Force Export", command=_try_override,
                      fg_color="#7B4A00", hover_color="#9B6000",
                      text_color=TEXT_WH, font=("Segoe UI", 11, "bold"),
                      height=36, corner_radius=8
                      ).pack(side="left", padx=(0, 8))
        ctk.CTkButton(btn_row, text="Cancel", command=dlg.destroy,
                      fg_color=BG_CARD, hover_color=BG_MID,
                      text_color=TEXT_WH, font=("Segoe UI", 11),
                      height=36, corner_radius=8
                      ).pack(side="left")

        self.wait_window(dlg)
        return result[0]

    # ══════════════════════════════════════════════════════════════════════════
    #  EXPORT PREVIEW
    # ══════════════════════════════════════════════════════════════════════════

    def _start_export_preview(self):
        if self.raw_df is None:
            messagebox.showwarning("No Data", "Please select a JobTime file first.")
            return
        if not all(self.col_map.get(f) for f in ("employee", "paycode", "hours")):
            messagebox.showerror("Column Error",
                                 "Could not detect Employee No, Pay Code, and Hours columns.")
            return
        self._collect_config_from_ui()
        self.gen_preview_btn.configure(state="disabled", text="Generating…")
        self.update_status("Generating export preview…", "warn")
        date_from = self._parse_date(self.date_from_var.get().strip())
        date_to   = self._parse_date(self.date_to_var.get().strip())
        threading.Thread(
            target=self._run_export_preview,
            args=(self.raw_df, self.col_map, self.config_mgr, date_from, date_to,
                  self.dist_map),
            daemon=True,
        ).start()

    def _run_export_preview(self, df, col_map, config, date_from, date_to, dist_map):
        try:
            unmapped  = find_unmapped_paycodes(df, col_map, config, date_from, date_to)
            excluded  = get_excluded_rows(df, col_map, config, date_from, date_to)
            h, d, summary = process_timesheet(df, col_map, config, date_from, date_to, dist_map)
            summary['unmapped_report'] = unmapped
            summary['excluded_df']     = excluded
            self.after(0, lambda: self._on_preview_ready(h, d, summary))
        except Exception as e:
            logging.exception("Preview failed")
            self.after(0, lambda: self._on_preview_error(str(e)))

    def _on_preview_ready(self, header_df, detail_df, summary):
        self._preview_header_df   = header_df
        self._preview_detail_df   = detail_df
        self._preview_excluded_df = summary.get('excluded_df')
        self.gen_preview_btn.configure(state="normal", text="▶  Generate Preview")

        unmapped = summary.get('unmapped_report', {})
        if unmapped.get('unmapped'):
            missing = round(unmapped['total_hours'] - unmapped['mapped_hours'], 2)
            pc_list = ", ".join(f'"{p}"' for p in sorted(unmapped['unmapped']))
            warn = (
                f"⚠  HOURS MISMATCH: JobTime total {unmapped['total_hours']:.2f} hrs  "
                f"vs export {unmapped['mapped_hours']:.2f} hrs  "
                f"({missing:+.2f} hrs missing) — unmapped pay codes: {pc_list}"
            )
            self.preview_summary_label.configure(text=warn, text_color=WARN_YLW)
            self.update_status("Preview ready — hours mismatch detected, export will be blocked.", "warn")
        else:
            self.preview_summary_label.configure(
                text=(
                    f"  👥 {summary['employee_count']} employees    "
                    f"⏱ {summary['total_hours']:.2f} total hours    "
                    f"📋 {len(header_df)} header rows  ({len(header_df.columns)} cols)    "
                    f"📄 {summary['detail_lines']} detail rows  ({len(detail_df.columns)} cols)    "
                    f"📅 Period end: {summary['perend']}"
                ),
                text_color=OK_GRN,
            )
            self.update_status("Export preview generated.", "ok")
        self._switch_preview_mode(self.preview_mode_var.get())

    def _on_preview_error(self, msg: str):
        self.gen_preview_btn.configure(state="normal", text="▶  Generate Preview")
        self.preview_summary_label.configure(text=f"⚠  {msg}", text_color=ERR_RED)
        self.update_status(f"Preview error: {msg}", "error")

    def _switch_preview_mode(self, *_):
        self._on_preview_search_change()

    # ══════════════════════════════════════════════════════════════════════════
    #  PROCESS & EXPORT
    # ══════════════════════════════════════════════════════════════════════════

    def _start_process(self):
        if self.raw_df is None:
            messagebox.showwarning("No Data", "Please select a JobTime file first.")
            return
        if not all(self.col_map.get(f) for f in ("employee", "paycode", "hours")):
            messagebox.showerror("Column Error",
                                 "Could not detect Employee No, Pay Code, and Hours columns.")
            return
        if self._block_if_dist_incomplete():
            return
        self._collect_config_from_ui()
        date_from = self._parse_date(self.date_from_var.get().strip())
        date_to   = self._parse_date(self.date_to_var.get().strip())
        if self._block_if_hours_mismatch(date_from, date_to):
            return
        out_path = filedialog.asksaveasfilename(
            title="Save Sage 300 Import File",
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook", "*.xlsx"), ("All files", "*.*")],
            initialfile=f"Sage300_Timecard_{date.today().strftime('%Y%m%d')}.xlsx",
        )
        if not out_path:
            return
        self.process_btn.configure(state="disabled", text="Processing…")
        self.update_status("Processing…", "warn")
        threading.Thread(
            target=self._run_process,
            args=(self.raw_df, self.col_map, self.config_mgr,
                  date_from, date_to, out_path, self.dist_map),
            daemon=True,
        ).start()

    def _run_process(self, df, col_map, config, date_from, date_to, out_path, dist_map):
        try:
            h, d, summary = process_timesheet(df, col_map, config, date_from, date_to, dist_map)
            export_to_excel(h, d, out_path)
            msg = (
                f"✅ Export complete!\n\n"
                f"  Employees:    {summary['employee_count']}\n"
                f"  Detail lines: {summary['detail_lines']}\n"
                f"  Total hours:  {summary['total_hours']:.2f}\n"
                f"  Period end:   {summary['perend']}\n"
                f"  Sheets:       6 (matching Sage template)\n\n"
                f"Saved to:\n{out_path}"
            )
            self.after(0, lambda: self._on_success(msg, out_path))
        except Exception as e:
            logging.exception("Processing failed")
            self.after(0, lambda: self._on_error(str(e)))

    def _on_success(self, msg: str, out_path: str):
        self.process_btn.configure(state="normal", text="⚙  Process & Export →")
        self.update_status(f"Export complete → {os.path.basename(out_path)}", "ok")
        if messagebox.askyesno("Export Complete", msg + "\n\nOpen the file now?"):
            os.startfile(out_path)

    def _on_error(self, msg: str):
        self.process_btn.configure(state="normal", text="⚙  Process & Export →")
        self.update_status(f"Error: {msg}", "error")
        messagebox.showerror("Processing Error", msg)

    # ══════════════════════════════════════════════════════════════════════════
    #  SEARCH
    # ══════════════════════════════════════════════════════════════════════════

    def _apply_search(self, df: pd.DataFrame, query: str) -> pd.DataFrame:
        if not query.strip():
            return df
        q = query.strip().lower()
        mask = df.apply(
            lambda col: col.astype(str).str.lower().str.contains(q, regex=False, na=False)
        ).any(axis=1)
        return df[mask]

    def _on_raw_search_change(self, *_):
        if self._raw_display_df is None:
            return
        query    = self.raw_search_var.get()
        filtered = self._apply_search(self._raw_display_df, query)
        self._populate_tree(self.tree, filtered, anomaly_map=self._anomaly_map)
        if query.strip():
            matched, total = len(filtered), len(self._raw_display_df)
            self.raw_match_label.configure(
                text=f"{matched:,} / {total:,}",
                text_color=OK_GRN if matched else ERR_RED,
            )
        else:
            self.raw_match_label.configure(text="")

    def _on_preview_search_change(self, *_):
        mode = self.preview_mode_var.get()
        if mode == "Excluded Rows":
            df = self._preview_excluded_df
        elif mode == "Timecard_Header":
            df = self._preview_header_df
        else:
            df = self._preview_detail_df
        if df is None:
            self.preview_tree.delete(*self.preview_tree.get_children())
            self.preview_summary_label.configure(
                text="Generate a preview first, then select Excluded Rows.",
                text_color=TEXT_DIM)
            return
        query    = self.preview_search_var.get()
        filtered = self._apply_search(df, query)
        if mode == "Excluded Rows":
            # Apply orange highlight tag to every row — all are excluded
            self.preview_tree.delete(*self.preview_tree.get_children())
            all_cols = list(filtered.columns)
            self.preview_tree["columns"] = all_cols
            for c in all_cols:
                self.preview_tree.heading(c, text=c)
                w = max(len(str(c)) * 9 + 24, 72)
                self.preview_tree.column(c, width=w, minwidth=w, anchor="w", stretch=False)
            self.preview_tree.tag_configure("excluded", background="#FF8C00", foreground="#000000")
            for _, row in filtered.iterrows():
                vals = [str(v)[:10] if hasattr(v, 'date') else ("" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)) for v in row]
                self.preview_tree.insert("", "end", values=vals, tags=("excluded",))
            if query.strip():
                self.preview_match_label.configure(
                    text=f"{len(filtered):,} / {len(df):,}",
                    text_color=OK_GRN if filtered else ERR_RED)
            else:
                self.preview_match_label.configure(
                    text=f"{len(df):,} excluded row{'s' if len(df) != 1 else ''}",
                    text_color=WARN_YLW)
        else:
            self._populate_tree(self.preview_tree, filtered)
            if query.strip():
                matched, total = len(filtered), len(df)
                self.preview_match_label.configure(
                    text=f"{matched:,} / {total:,}",
                    text_color=OK_GRN if matched else ERR_RED,
                )
            else:
                self.preview_match_label.configure(text="")

    # ══════════════════════════════════════════════════════════════════════════
    #  HELPERS
    # ══════════════════════════════════════════════════════════════════════════

    def _populate_tree(self, tree: ttk.Treeview, df: pd.DataFrame,
                       hide_cols: set | None = None,
                       anomaly_map: dict | None = None):
        """Fill a Treeview from a DataFrame. Anomalous rows are highlighted red."""
        tree.delete(*tree.get_children())
        is_raw = (tree is self.tree)
        if is_raw:
            self._anomaly_items = {}

        tree.tag_configure("anomaly",       background="#FFF176", foreground="#000000")
        tree.tag_configure("dist_anomaly",  background="#FFD600", foreground="#000000")
        tree.tag_configure("unmapped_anomaly", background="#FF8C00", foreground="#000000")

        all_cols = list(df.columns)
        cols     = [c for c in all_cols if c not in (hide_cols or set())]
        tree["columns"] = cols
        for c in cols:
            tree.heading(c, text=c)
            header_w = len(str(c)) * 9 + 24
            if not df.empty:
                max_len = df[c].astype(str).str.len().max()
                data_w  = int(max_len) * 8 + 16 if pd.notna(max_len) else 0
            else:
                data_w = 0
            w = max(header_w, data_w, 72)
            tree.column(c, width=w, minwidth=w, anchor="w", stretch=False)

        for orig_idx, row in df.iterrows():
            vals = []
            for c in cols:
                v = row[c]
                if isinstance(v, (datetime, pd.Timestamp)):
                    vals.append(str(v)[:10])
                elif isinstance(v, float) and pd.isna(v):
                    vals.append("")
                else:
                    vals.append("" if v is None else str(v))

            is_dist     = is_raw and orig_idx in self._dist_anomaly_map
            is_unmapped = is_raw and orig_idx in self._unmapped_anomaly_map and not is_dist
            is_data     = anomaly_map and orig_idx in anomaly_map and not is_dist and not is_unmapped
            if is_dist:
                tag = ("dist_anomaly",)
            elif is_unmapped:
                tag = ("unmapped_anomaly",)
            elif is_data:
                tag = ("anomaly",)
            else:
                tag = ()
            iid = tree.insert("", "end", values=vals, tags=tag)

            if is_raw and (is_dist or is_unmapped or is_data):
                if is_dist:
                    self._anomaly_items[iid] = self._dist_anomaly_map[orig_idx]
                elif is_unmapped:
                    self._anomaly_items[iid] = self._unmapped_anomaly_map[orig_idx]
                else:
                    self._anomaly_items[iid] = anomaly_map[orig_idx]

        # Bind click handler only on the raw input tree
        if is_raw:
            tree.bind("<ButtonRelease-1>", self._on_raw_tree_click)

    # ── Anomaly popup ─────────────────────────────────────────────────────────

    def _on_raw_tree_click(self, event):
        iid = self.tree.identify_row(event.y)
        if iid and iid in self._anomaly_items:
            self._show_anomaly_popup(event.x_root, event.y_root,
                                     self._anomaly_items[iid])
        else:
            self._dismiss_anomaly_popup()

    def _create_popup(self, x: int, y: int, border_color: str):
        """Create a borderless popup at (x, y). Returns (popup, inner_frame)."""
        self._dismiss_anomaly_popup()
        popup = tk.Toplevel(self)
        popup.overrideredirect(True)
        popup.geometry(f"+{x}+{y}")
        popup.configure(bg="#1A1A2E")
        outer = tk.Frame(popup, bg=border_color, padx=1, pady=1)
        outer.pack(fill="both", expand=True)
        inner = tk.Frame(outer, bg="#2D3748", padx=14, pady=10)
        inner.pack(fill="both", expand=True)
        self._anomaly_popup = popup
        popup.focus_set()
        popup.bind("<FocusOut>", lambda _: self._dismiss_anomaly_popup())
        self.after(100, lambda: self.bind("<Button-1>", lambda _: self._dismiss_anomaly_popup(), add="+"))
        return popup, inner

    def _show_anomaly_popup(self, x: int, y: int, message: str):
        _, inner = self._create_popup(x + 12, y + 12, "#ECC94B")
        tk.Label(inner, text="⚠  Data Issue — Row will be skipped",
                 bg="#2D3748", fg="#ECC94B",
                 font=("Segoe UI", 11, "bold"), justify="left"
                 ).pack(anchor="w", pady=(0, 6))
        tk.Label(inner, text=message,
                 bg="#2D3748", fg="#E2E8F0",
                 font=("Segoe UI", 10), justify="left", wraplength=380
                 ).pack(anchor="w")
        tk.Label(inner, text="Click anywhere to dismiss",
                 bg="#2D3748", fg="#718096",
                 font=("Segoe UI", 9, "italic"), justify="left"
                 ).pack(anchor="w", pady=(8, 0))

    def _show_log_info(self):
        popup = tk.Toplevel(self)
        popup.overrideredirect(True)
        popup.attributes("-topmost", True)
        popup.configure(bg=BG_CARD)

        popup_w, popup_h = 370, 270
        btn_x = self.log_info_btn.winfo_rootx()
        btn_y = self.log_info_btn.winfo_rooty()
        btn_h = self.log_info_btn.winfo_height()
        x = btn_x - popup_w + self.log_info_btn.winfo_width()
        y = btn_y + btn_h + 4
        if y + popup_h > self.winfo_screenheight() - 20:
            y = btn_y - popup_h - 4
        popup.geometry(f"{popup_w}x{popup_h}+{x}+{y}")

        border = tk.Frame(popup, bg=ACCENT2, bd=0)
        border.place(relx=0, rely=0, relwidth=1, relheight=1)
        card = tk.Frame(border, bg=BG_CARD)
        card.place(x=1, y=1, relwidth=1, relheight=1, width=-2, height=-2)

        hdr = tk.Frame(card, bg=BG_MID)
        hdr.pack(fill="x")
        tk.Label(hdr, text="📄  Activity Log",
                 bg=BG_MID, fg=TEXT_WH, font=("Segoe UI", 11, "bold"),
                 padx=12, pady=8).pack(side="left")
        tk.Button(hdr, text="✕", bg=BG_MID, fg=TEXT_DIM,
                  activebackground="#C53030", activeforeground=TEXT_WH,
                  font=("Segoe UI", 10, "bold"), bd=0, padx=8, pady=6,
                  cursor="hand2", command=popup.destroy).pack(side="right")

        body = tk.Frame(card, bg=BG_CARD)
        body.pack(fill="both", expand=True, padx=14, pady=10)
        tk.Label(body,
                 text=(
                     "Every action the app takes is recorded in vectis.log:\n\n"
                     "  •  File loads and row counts\n"
                     "  •  Processing steps and any data warnings\n"
                     "  •  Export success or failure details\n"
                     "  •  Full error traces if something goes wrong\n\n"
                     "Click  📄 Log  to open the file in your text editor."
                 ),
                 bg=BG_CARD, fg=TEXT_DIM,
                 font=("Segoe UI", 10), justify="left", wraplength=336, anchor="nw"
                 ).pack(fill="both", expand=True)

        def _on_click_outside(e):
            wx, wy = popup.winfo_rootx(), popup.winfo_rooty()
            ww, wh = popup.winfo_width(), popup.winfo_height()
            if not (wx <= e.x_root <= wx + ww and wy <= e.y_root <= wy + wh):
                popup.destroy()

        # Delay so the click that opened the popup doesn't immediately close it
        self.after(150, lambda: self.bind("<Button-1>", _on_click_outside, add="+"))
        popup.bind("<Destroy>", lambda _: self.unbind("<Button-1>"))
        popup.focus_set()

    def _dismiss_anomaly_popup(self):
        if self._anomaly_popup:
            try:
                self._anomaly_popup.destroy()
            except Exception:
                pass
            self._anomaly_popup = None
            self.unbind("<Button-1>")

    def update_status(self, text: str, level: str = "ok"):
        colors = {"ok": OK_GRN, "warn": WARN_YLW, "error": ERR_RED}
        self.status_dot.configure(text_color=colors.get(level, OK_GRN))
        self.status_label.configure(text=text[:130])
        logging.info(f"[{level}] {text}")

    def _parse_date(self, s: str) -> date | None:
        if not s:
            return None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        messagebox.showwarning("Date Error", f"Can't parse '{s}' — use YYYY-MM-DD.")
        return None

    def _open_log(self):
        if os.path.exists(log_path):
            os.startfile(log_path)
        else:
            messagebox.showinfo("Log", "No log file yet.")


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = CSVDaddyApp()
    app.mainloop()
