import pandas as pd

# Load data
df = pd.read_csv("C:/Users/User/PycharmProjects/PythonProject/PROJECT/PRODUCT/python-powerbi-project/data/sales_data.csv")
# Convert Date column to datetime
df["Date"] = pd.to_datetime(df["Date"])

# Create a new column: Revenue per Unit
df["Revenue_per_Unit"] = df["Sales_Amount"] / df["Units_Sold"]

# Save cleaned data
df.to_csv("cleaned_sales_data.csv", index=False)

print("Data cleaned and saved successfully!")

###
# data = {
#    "Date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
#    "Product": ["Widget A", "Widget B", "Widget A", "Widget C", "Widget B"],
#    "Region": ["North", "South", "East", "West", "North"],
#    "Sales_Amount": [1200, 850, 950, 1500, 700],
#    "Units_Sold": [10, 7, 8, 12, 6]
#}

#df = pd.DataFrame(data)
#df.to_csv("sales_data.csv", index=False)
