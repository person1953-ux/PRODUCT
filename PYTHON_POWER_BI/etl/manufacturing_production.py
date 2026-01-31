import pandas as pd
import random
from datetime import datetime, timedelta

# Configuration
num_records = 1000
departments = ["thin", "photo", "diff","cvd", "cmp", "endfab"]
statuses = ["run", "wait", "hold", "shipped"]
products = ["P100", "P200", "P300", "P400","P500"]
processes = ["PRC02NN", "PRC04NN", "PRC16NN", "PRC18NN", "PRC27NN"]

start_date = datetime(2025, 1, 1)

data = []

for i in range(num_records):
    record = {
        "tnxtimestamp": start_date + timedelta(minutes=random.randint(0, 60*24*180)),
        "processid": random.choice(processes),
        "productid": random.choice(products),
        "department": random.choice(departments),
        "lotid": f"LOT{random.randint(1000, 9999)}",
        "quantity": random.randint(10, 300),
        "status": random.choice(statuses)
    }
    data.append(record)

df = pd.DataFrame(data)

# Sort by timestamp (realistic manufacturing flow)
df = df.sort_values("tnxtimestamp")

# Save to CSV
df.to_csv("manufacturing_production.csv", index=False)

print("manufacturing_production.csv created with 1000 records")
