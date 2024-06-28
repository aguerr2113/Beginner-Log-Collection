import sqlite3
import pandas as pd
import numpy as np

# Step 1: Data Collection - Create a SQLite database and a table for logs
conn = sqlite3.connect('logs.db')
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS logs (
    timestamp TEXT,
    value REAL
)
''')
conn.commit()

# Step 2: Generate simulated log data and insert into the database
log_data = pd.DataFrame({
    'timestamp': pd.date_range(start='1/1/2022', periods=100, freq='min').astype(str),
    'value': np.random.normal(0, 1, 100)
})

log_data.to_sql('logs', conn, if_exists='append', index=False)
conn.close()

# Step 3: Preprocessing - Retrieve and preprocess data
conn = sqlite3.connect('logs.db')
log_data = pd.read_sql('SELECT * FROM logs', conn)

print("Data retrieved from database:")
print(log_data.head())

# Normalize the data
log_data['value'] = (log_data['value'] - log_data['value'].mean()) / log_data['value'].std()

print("Normalized data:")
print(log_data.head())

conn.close()

# Step 4: Simple Anomaly Detection - Detect anomalies using z-score
anomaly_threshold = 3
log_data['z_score'] = (log_data['value'] - log_data['value'].mean()) / log_data['value'].std()
anomalies = log_data[log_data['z_score'].abs() > anomaly_threshold]

# Check if anomalies were detected
print("Anomalies detected:")
print(anomalies.head())

# Step 5: Reporting - Save anomalies to a CSV file in a different location
anomalies.to_csv('anomalies_report.csv', index=False)

print("Anomaly detection complete. Report saved as 'anomalies_report.csv'.")
