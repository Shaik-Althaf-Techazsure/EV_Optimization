import time
import random
import logging
import os
import sys
from datetime import datetime
import mysql.connector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [TZ-SIMULATOR] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Simulator-MySQL")

class GridTelemetrySimulator:
    """
    Simulates electrical telemetry from various Bengaluru subdivisions.
    Provides live kW, Voltage, and Current (A) metrics to the MySQL grid.
    """

    def __init__(self):
        self.db_config = {
            'host': os.environ.get('MYSQLHOST', 'localhost'),
            'user': os.environ.get('MYSQLUSER', 'root'),
            'password': os.environ.get('MYSQLPASSWORD', 'TeChAzSuRe786'), 
            'database': os.environ.get('MYSQLDATABASE', 'gridsense_db'),
            'port': os.environ.get('MYSQLPORT', 3306)
        }
        
        self.zones = ['HSR Layout', 'Indiranagar', 'Whitefield', 'Electronic City', 'Hebbal', 'Koramangala']

        self.capacities = {
            'HSR Layout': 250, 'Indiranagar': 500, 'Whitefield': 1000,
            'Electronic City': 1000, 'Hebbal': 400, 'Koramangala': 400
        }
        
        self.init_db()

    def get_connection(self):
        """Establishes a robust connection to the MySQL server."""
        try:
            return mysql.connector.connect(**self.db_config)
        except mysql.connector.Error as err:
            logger.error(f"Database connection failed: {err}")
            return None

    def init_db(self):
        """Initializes the live_telemetry table with MySQL-specific constraints."""
        logger.info("Initializing MySQL Grid Schema...")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS live_telemetry (
            zone_id VARCHAR(50) PRIMARY KEY,
            zone_name VARCHAR(100),
            current_load_kw FLOAT,
            voltage_v FLOAT,
            current_a FLOAT,
            utilization_perc FLOAT,
            status VARCHAR(20),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );
        """
        try:
            conn = self.get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute(create_table_query)
                cursor.close()
                conn.close()
                logger.info("Schema synchronization complete.")
        except Exception as e:
            logger.error(f"Schema Error: {e}")

    def generate_live_metrics(self, zone):
        """Calculates simulated electrical metrics using grid physics logic."""
        base_cap = self.capacities[zone]
        hour = datetime.now().hour

        is_peak = 18 <= hour <= 22
        load_factor = 0.82 if is_peak else 0.28

        utilization = (load_factor + random.uniform(-0.15, 0.15)) * base_cap
        utilization = max(10.0, min(utilization, base_cap * 1.05))
        
        voltage = 415 + random.uniform(-6, 6) 
        current = (utilization * 1000) / (voltage * 1.732)
        
        return (
            zone.lower().replace(" ", "_"),
            zone,
            round(utilization, 2),
            round(voltage, 1),
            round(current, 2),
            round((utilization / base_cap) * 100, 1),
            "OPERATIONAL" if utilization < (base_cap * 0.90) else "OVERLOAD_RISK"
        )

    def run_stream(self):
        """Infinite loop to stream telemetry to MySQL every 3 seconds."""
        logger.info("Starting High-Frequency Telemetry Stream...")

        upsert_query = """
        INSERT INTO live_telemetry (zone_id, zone_name, current_load_kw, voltage_v, current_a, utilization_perc, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            current_load_kw = VALUES(current_load_kw),
            voltage_v = VALUES(voltage_v),
            current_a = VALUES(current_a),
            utilization_perc = VALUES(utilization_perc),
            status = VALUES(status),
            last_updated = CURRENT_TIMESTAMP;
        """

        while True:
            conn = self.get_connection()
            if conn:
                try:
                    cursor = conn.cursor()
                    for zone in self.zones:
                        metrics = self.generate_live_metrics(zone)
                        cursor.execute(upsert_query, metrics)
                    conn.commit()
                    cursor.close()
                    conn.close()
                    logger.info(f"Broadcast successful: {datetime.now().strftime('%H:%M:%S')}")
                except Exception as e:
                    logger.error(f"Broadcast Error: {e}")
                    if conn.is_connected(): conn.close()
            
            time.sleep(3)

if __name__ == "__main__":
    simulator = GridTelemetrySimulator()
    simulator.run_stream()