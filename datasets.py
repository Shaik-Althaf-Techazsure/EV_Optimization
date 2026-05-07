import pandas as pd
import numpy as np
import os
import json
import logging
import time
import uuid
import sys
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    handlers=[logging.FileHandler("techazsure_core.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("GridSense-Core")

class TechAZsureAnalyticsEngine:
    """
    A robust analytical engine designed as a decision-support layer for BESCOM.
    Operates as a non-invasive software twin of the electrical distribution grid.
    """

    def __init__(self, dataset_path):
        self.dataset_path = dataset_path
        self.df = None
        self.engine_id = f"TZ-{str(uuid.uuid4())[:8].upper()}"
        self.start_timestamp = time.time()

        self.GRID_CONSTANTS = {
            'TARIFF_INR_KWH': 8.5,           # Commercial/EV Tariff rate
            'MAINTENANCE_SAVE_PER_ALERT': 5500.0, # Estimated CAPEX saving per prevented overload
            'PEAK_START_HOUR': 18,           # 6 PM
            'PEAK_END_HOUR': 22,             # 10 PM
            'OPTIMIZATION_SHIFT_RATIO': 0.225, # 22.5% Load shifting goal
            'CO2_COEFFICIENT': 0.85          # kg CO2 saved per kWh optimized
        }

        self.transformer_specs = {
            'HSR Layout': 250, 'Indiranagar': 500, 'Whitefield': 1000,
            'Electronic City': 1000, 'Hebbal': 400, 'Koramangala': 400
        }
        
        logger.info(f"Engine v4.0 Initialized. Core ID: {self.engine_id}")

    def validate_and_load_data(self):
        """Ingests raw telemetry and executes strict integrity checks."""
        logger.info(f"Accessing data source: {self.dataset_path}...")
        if not os.path.exists(self.dataset_path):
            logger.critical("FATAL: Primary dataset missing. System entering standby.")
            return False
            
        try:
            self.df = pd.read_csv(self.dataset_path)

            self.df['Plug_In_Time'] = pd.to_datetime(self.df['Plug_In_Time'])
            self.df['Hour'] = self.df['Plug_In_Time'].dt.hour
            self.df['Day_of_Week'] = self.df['Plug_In_Time'].dt.dayofweek
            self.df['Is_Weekend'] = (self.df['Day_of_Week'] >= 5).astype(int)

            if self.df.isnull().sum().any():
                logger.warning("Null values detected. Initiating Mean Imputation...")
                self.df = self.df.fillna(self.df.mean(numeric_only=True))
                
            logger.info(f"Ingestion successful. Processed {len(self.df)} telemetry sessions.")
            return True
        except Exception as e:
            logger.error(f"Pipeline Initialization Error: {str(e)}")
            return False

    def train_demand_intelligence(self):
        """Trains a Gradient Boosting Regressor for precision load forecasting."""
        logger.info("Initializing ML Intelligence Pipeline (Part A)...")

        le = LabelEncoder()
        ml_df = self.df.copy()
        ml_df['Zone_Encoded'] = le.fit_transform(ml_df['Zone'])

        X = ml_df[['Hour', 'Day_of_Week', 'Zone_Encoded', 'Is_Weekend']]
        y = ml_df['Energy_Requested_kWh']
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.15, random_state=42)

        model = GradientBoostingRegressor(n_estimators=150, learning_rate=0.1, max_depth=5, random_state=42)
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_test)
        logger.info(f"Forecasting Engine Validated. MAE: {mean_absolute_error(y_test, y_pred):.4f} kWh")
        return model

    def execute_grid_optimization(self):
        """Applies algorithmic shifts to demand to reduce peak transformer stress."""
        logger.info("Orchestrating Demand Shifting (Part A Implementation)...")

        demand_metrics = self.df.groupby(['Zone', 'Hour']).agg({
            'Energy_Requested_kWh': 'sum',
            'Session_ID': 'count',
            'Grid_Limit_Alert': 'sum'
        }).reset_index()

        def calculate_optimized_load(row):
            if self.GRID_CONSTANTS['PEAK_START_HOUR'] <= row['Hour'] <= self.GRID_CONSTANTS['PEAK_END_HOUR']:
                return row['Energy_Requested_kWh'] * (1 - self.GRID_CONSTANTS['OPTIMIZATION_SHIFT_RATIO'])
            return row['Energy_Requested_kWh']

        demand_metrics['Optimized_Load_kWh'] = demand_metrics.apply(calculate_optimized_load, axis=1)

        demand_metrics['Revenue_INR'] = demand_metrics['Energy_Requested_kWh'] * self.GRID_CONSTANTS['TARIFF_INR_KWH']
        demand_metrics['CO2_Saved_kg'] = (demand_metrics['Energy_Requested_kWh'] - demand_metrics['Optimized_Load_kWh']) * self.GRID_CONSTANTS['CO2_COEFFICIENT']

        demand_metrics['Allotted_Capacity_kW'] = demand_metrics['Zone'].map(self.transformer_specs)
        demand_metrics['Utilization_Rate'] = (demand_metrics['Energy_Requested_kWh'] / demand_metrics['Allotted_Capacity_kW']) * 100
        
        demand_metrics.to_csv('processed_demand.csv', index=False)
        self.processed_demand = demand_metrics
        logger.info("Demand Orchestration complete. Exported 'processed_demand.csv'")

    def compute_ghi_score(self, alerts, volume, utilization):
        """Proprietary Health Indexing logic for grid assets."""
        stress_penalty = (alerts / volume) * 60
        load_penalty = (utilization / 100) * 20
        
        health_score = 100 - stress_penalty - load_penalty
        return max(10, min(100, health_score))

    def perform_infrastructure_audit(self):
        """Identifies critical expansion zones and evaluates ROI (Part B Implementation)."""
        logger.info("Executing Geospatial Infrastructure Audit (Part B)...")
        
        hotspots = self.df.groupby(['Zone', 'Transformer_ID']).agg({
            'Grid_Limit_Alert': 'sum',
            'Energy_Requested_kWh': 'sum'
        }).reset_index()
        
        hotspots['Allotted_kW'] = hotspots['Zone'].map(self.transformer_specs)
        
        def apply_ghi(row):
            total_sessions = len(self.df[self.df['Transformer_ID'] == row['Transformer_ID']])
            peak_util = (row['Energy_Requested_kWh'] / row['Allotted_kW']) * 100
            return self.compute_ghi_score(row['Grid_Limit_Alert'], total_sessions, peak_util)
            
        hotspots['GHI'] = hotspots.apply(apply_ghi, axis=1)

        hotspots['Deferred_CAPEX_INR'] = hotspots['Grid_Limit_Alert'] * self.GRID_CONSTANTS['MAINTENANCE_SAVE_PER_ALERT']
        hotspots['Expansion_Requirement'] = hotspots['GHI'].apply(lambda x: "CRITICAL" if x < 60 else "STABLE")

        self.infrastructure_hotspots = hotspots.sort_values(by='GHI', ascending=True)
        self.infrastructure_hotspots.to_csv('infrastructure_hotspots.csv', index=False)
        logger.info("Infrastructure Audit complete. Exported 'infrastructure_hotspots.csv'")

    def compile_ai_intelligence_report(self):
        """Generates a structured JSON intelligence report for the AI Agent."""
        logger.info("Compiling daily intelligence report for AI Agent...")
        
        full_report = []
        for zone in self.infrastructure_hotspots['Zone'].unique():
            zone_data = self.infrastructure_hotspots[self.infrastructure_hotspots['Zone'] == zone]
            
            avg_ghi = zone_data['GHI'].mean()
            total_savings = zone_data['Deferred_CAPEX_INR'].sum()
            alert_status = "ACTION REQUIRED" if (zone_data['GHI'] < 60).any() else "OPERATIONAL"
            
            insight = f"Zone {zone} shows an average health index of {avg_ghi:.1f}%. "
            if alert_status == "ACTION REQUIRED":
                insight += "Critical congestion detected. AI recommends immediate deployment of additional 50kW Fast Charging station to relieve DT load."
            else:
                insight += "Grid stability within safe bounds. Proceed with standard maintenance."

            full_report.append({
                "zone": zone,
                "ghi_score": f"{avg_ghi:.1f}%",
                "status": alert_status,
                "roi_savings": f"₹{total_savings:,.0f}",
                "ai_agent_recommendation": insight
            })
            
        with open('ai_daily_report.json', 'w') as f:
            json.dump(full_report, f, indent=4)
            
        logger.info("AI Intelligence Package ready.")

    def run_full_analytical_cycle(self):
        """Executes the complete TechAZsure Intelligence Pipeline."""
        logger.info("--- STARTING GRID INTELLIGENCE CYCLE ---")
        
        try:
            if not self.validate_and_load_data():
                return
                
            self.train_demand_intelligence()
            self.execute_grid_optimization()
            
            self.perform_infrastructure_audit()

            self.compile_ai_intelligence_report()
            
            execution_time = time.time() - self.start_timestamp
            logger.info(f"--- CYCLE COMPLETE | EXECUTION TIME: {execution_time:.2f}s | STATUS: SUCCESS ---")
            
        except Exception as e:
            logger.error(f"--- PIPELINE FAILURE: {str(e)} ---")

if __name__ == "__main__":
    DATA_PATH = 'dataset.csv'

    engine = TechAZsureAnalyticsEngine(DATA_PATH)

    engine.run_full_analytical_cycle()