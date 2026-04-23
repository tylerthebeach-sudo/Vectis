"""
config.py — Configuration for CSV Daddy
"""
import json, os, logging

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app_config.json")

DEFAULT_CONFIG = {
    "timecard_code":    "",
    "timecard_desc":    "",
    "perend_date":      "",
    "payroll_anchor":   "2026-01-02",   # Pay Period 1 of the payroll year
    "dist_file_path":   "",
    "pay_code_mappings": [
        {"jobtime_code": "1 - Regular",     "earnded": "100",   "linenum": 1000, "enabled": True},
        {"jobtime_code": "2 - Overtime",    "earnded": "200",   "linenum": 2000, "enabled": True},
        {"jobtime_code": "3 - Drive Time",  "earnded": "DRIVE", "linenum": 3000, "enabled": True},
        {"jobtime_code": "4 - BC Overtime", "earnded": "250",   "linenum": 4000, "enabled": True},
    ]
}


class AppConfig:
    def __init__(self):
        self.data = {}
        self.load()

    def load(self):
        if os.path.exists(CONFIG_PATH):
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    saved = json.load(f)
                self.data = {**DEFAULT_CONFIG, **saved}
                if not isinstance(self.data.get("pay_code_mappings"), list):
                    self.data["pay_code_mappings"] = DEFAULT_CONFIG["pay_code_mappings"]
                logging.info(f"Config loaded from {CONFIG_PATH}")
            except Exception as e:
                logging.warning(f"Config load error, using defaults: {e}")
                self.data = dict(DEFAULT_CONFIG)
        else:
            self.data = dict(DEFAULT_CONFIG)

    def save(self):
        try:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=2, default=str)
            logging.info(f"Config saved to {CONFIG_PATH}")
            return True
        except Exception as e:
            logging.error(f"Config save error: {e}")
            return False

    def get(self, key, default=None):
        return self.data.get(key, default)

    def set(self, key, value):
        self.data[key] = value

    @property
    def pay_code_mappings(self):
        return self.data.get("pay_code_mappings", [])

    @pay_code_mappings.setter
    def pay_code_mappings(self, value):
        self.data["pay_code_mappings"] = value

    def resolve_paycode(self, jobtime_code: str) -> dict | None:
        """Return mapping dict for a JobTime pay code, or None if not matched/enabled."""
        code = jobtime_code.strip()
        for m in self.pay_code_mappings:
            if m.get("enabled", True) and m.get("jobtime_code", "").strip() == code:
                return m
        return None
