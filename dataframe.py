import pandas as pd
### It seems dataframe change row to be column
data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 32, 18],
    'City': ['New York', 'Los Angeles', 'Chicago']
}
df = pd.DataFrame(data)
print(df)

#output
#       Name       Age    City
# 0    Alice       25     New York
# 1      Bob       32     Los Angeles
# 2  Charlie       18     Chicago

# Create DataFrames
data = {'A': [1, 2], 'B': [3, 4]}
df = pd.DataFrame(data)
print(df)
#      A       B
#0     1       3
#1     2       4

df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
df2 = pd.DataFrame({'A': [5], 'B': [6]})
print(df1)
print(df2)
# Use concat
result = pd.concat([df1, df2], ignore_index=True)
print(result)
