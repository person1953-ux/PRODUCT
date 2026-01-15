import torch
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd


# Create a NumPy array
numpy_array = np.random.rand(5, 3)
# Convert to a PyTorch tensor
tensor = torch.from_numpy(numpy_array)

# Perform a PyTorch operation
output = tensor * 10
print(output)

# Visualizing PyTorch Data with Matplotlib
# Generate random data
data = torch.randn(1000)
# Convert to NumPy array for plotting
numpy_data = data.numpy()

# Create histogram
plt.hist(numpy_data, bins=30, alpha=0.5, color='g')
plt.title('Histogram of Data Points')
plt.show()

# The conversion between Pandas DataFrames and PyTorch tensors:
# Create a sample DataFrame
df = pd.DataFrame({
    'A': range(1, 5),
    'B': range(5, 9)
})

# Convert DataFrame to a PyTorch tensor
tensor = torch.tensor(df.values)

# Perform a PyTorch operation
output = tensor * 2
print(output)

# Convert tensor back to DataFrame
new_df = pd.DataFrame(output.numpy(), columns=['A', 'B'])
print(new_df)


