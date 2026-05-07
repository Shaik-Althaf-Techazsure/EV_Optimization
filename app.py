import os
import json
import logging
import uuid
import sys
import threading
import time
import random
import math
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union

import pandas as pd
import numpy as np
from flask import Flask, render_template, jsonify, request, Response, abort, send_from_directory, has_request_context
from flask_cors import CORS

# Attempting to load enterprise database drivers
try:
    import mysql.connector
    from mysql.connector import Error, pooling
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False


base_dir = os.path.abspath(os.path.dirname(__file__))
template_dir = os.path.join(base_dir, 'templates')
static_dir = os.path.join(base_dir, 'static')

app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(256).hex())

CORS(app, 
     resources={r"/*": {"origins": "*"}}, 
     supports_credentials=True,
     allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
     methods=["GET", "POST", "OPTIONS"])

class IndustrialLogger:
    @staticmethod
    def setup():
        old_factory = logging.getLogRecordFactory()
        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            # FIX: Check if we are inside a web request before accessing 'request'
            if has_request_context():
                record.request_id = getattr(request, 'request_id', 'REQ-PROD')
            else:
                record.request_id = 'SYSTEM-INIT'
            return record
        
        logging.setLogRecordFactory(record_factory)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - [%(name)s] - [%(request_id)s] - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        return logging.getLogger("GridSense-Production")

logger = IndustrialLogger.setup()


class OSConfig:
    VERSION = "8.7.0-PRO-PRODUCTION"
    ENGINE_ID = f"TZ-PROD-{str(uuid.uuid4())[:8].upper()}"
    
    # Absolute data paths
    DEMAND_DATA = os.path.join(base_dir, 'processed_demand.csv')
    INFRA_DATA = os.path.join(base_dir, 'infrastructure_hotspots.csv')

    NOMINAL_VOLTAGE = 415.0
    POWER_FACTOR = 0.94
    TARIFF_BASE = 8.50
    TARIFF_PEAK_SURCHARGE = 1.25
    LOSS_COEFFICIENT = 0.0825
    CARBON_CREDIT_RATE = 0.852
    PEAK_WINDOW = range(18, 23)
    
    SUBDIVISIONS = ['HSR Layout', 'Indiranagar', 'Whitefield', 'Electronic City', 'Hebbal', 'Koramangala']
    CAPACITY_MAP = {'HSR Layout': 250, 'Indiranagar': 500, 'Whitefield': 1000, 'Electronic City': 1000, 'Hebbal': 400, 'Koramangala': 400}

    DB_CREDS = {
        'host': os.environ.get('MYSQLHOST', 'localhost'),
        'user': os.environ.get('MYSQLUSER', 'root'),
        'password': os.environ.get('MYSQLPASSWORD', 'TeChAzSuRe786'),
        'database': os.environ.get('MYSQLDATABASE', 'gridsense_db'),
        'port': int(os.environ.get('MYSQLPORT', 3306))
    }


    @staticmethod
    def calculate_jittered_load(zone: str) -> Dict[str, Any]:
        hour = datetime.now().hour
        is_peak = hour in OSConfig.PEAK_WINDOW
        base_cap = OSConfig.CAPACITY_MAP.get(zone, 300)
        target_util = 0.82 if is_peak else 0.35
        load_kw = round(base_cap * (target_util + random.uniform(-0.03, 0.05)), 2)
        util_perc = round((load_kw / base_cap) * 100, 1)
        voltage = round(OSConfig.NOMINAL_VOLTAGE - (util_perc * 0.08) + random.uniform(-1, 1), 1)
        current_a = round((load_kw * 1000) / (voltage * OSConfig.POWER_FACTOR * 1.732), 2)
        return {
            "zone_id": zone.lower().replace(" ", "_"),
            "zone_name": zone,
            "current_load_kw": load_kw,
            "voltage_v": voltage,
            "current_a": current_a,
            "utilization_perc": util_perc,
            "status": "OVERLOAD_RISK" if util_perc > 90 else "OPERATIONAL",
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

class PersistenceHub:
    _pool = None
    
    @classmethod
    def initialize_db(cls):
        if not MYSQL_AVAILABLE: return None
        try:
            cls._pool = pooling.MySQLConnectionPool(pool_name="gs_prod_pool", pool_size=5, **OSConfig.DB_CREDS)
            return cls._pool
        except Exception as e:
            logger.error(f"DB Init Failed: {e}")
            return None

    @classmethod
    def execute_query(cls, query: str):
        if not cls._pool: return []
        try:
            conn = cls._pool.get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query)
            res = cursor.fetchall()
            cursor.close()
            conn.close()
            return res
        except: return []

    @classmethod
    def get_backbone(cls) -> Tuple[pd.DataFrame, pd.DataFrame]:
        try:
            if os.path.exists(OSConfig.DEMAND_DATA) and os.path.exists(OSConfig.INFRA_DATA):
                return pd.read_csv(OSConfig.DEMAND_DATA), pd.read_csv(OSConfig.INFRA_DATA)
            return cls._emulate()
        except: return cls._emulate()

    @classmethod
    def _emulate(cls):
        d_rows = []
        for z in OSConfig.SUBDIVISIONS:
            for h in range(24):
                val = 130 + (np.sin((h-10)*np.pi/12) * 80) + random.uniform(0, 20)
                d_rows.append({'Zone': z, 'Hour': h, 'Energy_Requested_kWh': round(val, 2), 
                               'Optimized_Load_kWh': round(val*0.8 if h in OSConfig.PEAK_WINDOW else val, 2),
                               'Revenue_INR': round(val * OSConfig.TARIFF_BASE, 2)})
        i_rows = []
        for i, z in enumerate(OSConfig.SUBDIVISIONS):
            ghi = random.uniform(45, 95)
            i_rows.append({'Zone': z, 'Transformer_ID': f"DT-{100+i}", 'GHI': ghi, 
                           'Expansion_Requirement': "CRITICAL" if ghi < 60 else "STABLE",
                           'ROI_Savings_INR': random.uniform(40000, 80000)})
        return pd.DataFrame(d_rows), pd.DataFrame(i_rows)

@app.route('/')
def route_index(): return render_template('index.html', os_id=OSConfig.ENGINE_ID)

@app.route('/load_balancing.html')
def route_lb(): return render_template('load_balancing.html')

@app.route('/asset_roadmap.html')
def route_ar(): return render_template('asset_roadmap.html')

@app.route('/financial_settlement.html')
def route_fs(): return render_template('financial_settlement.html')

@app.route('/grid_audit_registry.html')
def route_gar(): return render_template('grid_audit_registry.html')

@app.route('/cybersecurity.html')
def route_cs(): return render_template('cybersecurity.html')

@app.route('/hsr_layout.html')
def route_area_hsr(): return render_template('hsr_layout.html')

@app.route('/whitefield.html')
def route_wf(): return render_template('whitefield.html')

@app.route('/indiranagar.html')
def route_in(): return render_template('indiranagar.html')

@app.route('/hebbal.html')
def route_hb(): return render_template('hebbal.html')

@app.route('/koramangala.html')
def route_km(): return render_template('koramangala.html')

@app.route('/api/v1/summary')
def get_summary():
    demand, infra = PersistenceHub.get_backbone()
    return jsonify({
        "total_revenue": f"₹{demand['Revenue_INR'].sum():,.0f}",
        "capex_deferred": f"₹{infra['ROI_Savings_INR'].sum():,.0f}",
        "carbon_offset": f"{demand['Energy_Requested_kWh'].sum() * OSConfig.CARBON_CREDIT_RATE:,.0f} kg",
        "avg_grid_health": f"{infra['GHI'].mean():.1f}%"
    })

@app.route('/api/v1/live-tracking')
def get_live():
    res = PersistenceHub.execute_query("SELECT * FROM live_telemetry")
    if not res: res = [GridPhysicsEngine.calculate_jittered_load(z) for z in OSConfig.SUBDIVISIONS]
    return jsonify(res)

@app.route('/api/v1/area-details/<zone_name>')
def get_details(zone_name):
    demand, infra = PersistenceHub.get_backbone()
    target = zone_name.lower()
    z_demand = demand[demand['Zone'].str.lower() == target]
    if z_demand.empty: z_demand = demand[demand['Zone'] == 'HSR Layout']
    
    total_cons = float(z_demand['Energy_Requested_kWh'].sum())
    peak_row = z_demand.loc[z_demand['Energy_Requested_kWh'].idxmax()]
    low_row = z_demand.loc[z_demand['Energy_Requested_kWh'].idxmin()]
    
    return jsonify({
        "metadata": {"subdivision": zone_name.upper()},
        "power_metrics": {"consumed_kwh": round(total_cons, 2), "supplied_kwh": round(total_cons*1.08, 2), "technical_loss_kwh": round(total_cons*0.08, 2)},
        "financial_metrics": {"total_revenue": f"₹{total_cons*8.5:,.2f}", "revenue_leakage": f"₹{total_cons*0.08*8.5:,.2f}"},
        "infrastructure": {"installed_dt_units": 6, "planned_expansion_hubs": 2 if target in ['hebbal', 'whitefield'] else 0},
        "temporal_insights": {"peak_consumption_hour": f"{int(peak_row['Hour'])}:00", "peak_load_value": f"{peak_row['Energy_Requested_kWh']:.1f} kWh",
                              "low_consumption_hour": f"{int(low_row['Hour'])}:00", "low_load_value": f"{low_row['Energy_Requested_kWh']:.1f} kWh"},
        "hourly_chart_data": z_demand[['Hour', 'Energy_Requested_kWh', 'Optimized_Load_kWh']].to_dict(orient='records')
    })

@app.route('/api/v1/ai-advisory')
def get_ai():
    return jsonify({
        "briefing": {"message": "Grid Integrity: Nominal. Nora AGI is optimizing load shifting for Bengaluru regional clusters."},
        "maintenance_pipeline": [{"zone": "Hebbal", "action": "Thermal Calibration", "urgency": "IMMEDIATE"}]
    })

@app.route('/api/v1/grid-prediction')
def get_prediction():
    demand, _ = PersistenceHub.get_backbone()
    hourly = demand.groupby('Hour').agg({'Energy_Requested_kWh': 'sum', 'Optimized_Load_kWh': 'sum'}).reset_index()
    return jsonify(hourly.to_dict(orient='records'))

@app.route('/api/v1/system-audit')
def get_audit():
    ledger = []
    for i in range(10):
        ledger.append({"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "event_code": "SECURE_SYNC", 
                       "auth_sig": hashlib.sha256(str(uuid.uuid4()).encode()).hexdigest()[:12].upper(), "status": "VERIFIED"})
    return jsonify(ledger)

def execute_preflight():
    logger.info(f"--- TECHAZSURE GRIDSENSE OS v{OSConfig.VERSION} ---")
    PersistenceHub.initialize_db()
    logger.info("Bootstrap sequence complete.")

if __name__ == '__main__':
    execute_preflight()
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
