import torch.nn as nn
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import torch
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd



# Generate synthetic data
X, y = np.random.rand(100, 10), np.random.randint(0, 2, 100)

# Split data
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Scale features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Convert to PyTorch tensors
X_train_tensor = torch.tensor(X_train_scaled, dtype=torch.float32)
X_test_tensor = torch.tensor(X_test_scaled, dtype=torch.float32)
y_train_tensor = torch.tensor(y_train, dtype=torch.float32).unsqueeze(1)
y_test_tensor = torch.tensor(y_test, dtype=torch.float32).unsqueeze(1)

# Define a simple neural network
class SimpleNN(nn.Module):
    def __init__(self):
        super(SimpleNN, self).__init__()
        self.layer1 = nn.Linear(10, 5)
        self.layer2 = nn.Linear(5, 1)

    def forward(self, x):
        x = torch.relu(self.layer1(x))
        x = torch.sigmoid(self.layer2(x))
        return x

# Initialize model
model = SimpleNN()

# Loss and optimizer
criterion = nn.BCELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

# Training loop
epochs = 50
for epoch in range(epochs):
    model.train()
    optimizer.zero_grad()

    outputs = model(X_train_tensor)
    loss = criterion(outputs, y_train_tensor)

    loss.backward()
    optimizer.step()

    if (epoch + 1) % 10 == 0:
        print(f"Epoch {epoch+1}/{epochs}, Loss: {loss.item():.4f}")

# Evaluation
model.eval()
with torch.no_grad():
    preds = model(X_test_tensor)
    preds = (preds > 0.5).float()
    accuracy = (preds.eq(y_test_tensor).sum() / len(y_test_tensor)).item()

print(f"\nModel accuracy: {accuracy:.2f}")


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


