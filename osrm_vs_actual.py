import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import pearsonr, spearmanr

# ===== 1. Load CSV =====
# CSV file with two columns: OSRM distance and actual distance
df = pd.read_csv("data/osrm_vs_real.csv")

# Rename columns for easier reference
df.rename(columns={"osrm_distances": "OSRM", "actual_distances": "Actual"}, inplace=True)

# Ensure columns are in float format
df["OSRM"] = df["OSRM"].astype(float)
df["Actual"] = df["Actual"].astype(float)

# ===== 2. Error Analysis =====
df["Error"] = df["OSRM"] - df["Actual"]
mae = np.mean(np.abs(df["Error"]))      # Mean Absolute Error
rmse = np.sqrt(np.mean(df["Error"]**2)) # Root Mean Square Error
mape = np.mean(np.abs(df["Error"] / df["Actual"])) * 100  # Mean Absolute Percentage Error

print(f"MAE  = {mae:.2f}")
print(f"RMSE = {rmse:.2f}")
print(f"MAPE = {mape:.2f}%")

# ===== 3. Correlation Analysis =====
pearson_r, pearson_p = pearsonr(df["OSRM"], df["Actual"])
spearman_r, spearman_p = spearmanr(df["OSRM"], df["Actual"])

print(f"Pearson r = {pearson_r:.3f} (p={pearson_p:.3f})")
print(f"Spearman ρ = {spearman_r:.3f} (p={spearman_p:.3f})")

# ===== 4. Scatter Plot =====
plt.figure(figsize=(8,8))
plt.scatter(df["Actual"], df["OSRM"], 
           alpha=0.4,                    # More transparent
           s=20,                        # Smaller points
           color='blue',                # Better color for visibility
           label=f"Data Points (n={len(df)})")
plt.plot([df["Actual"].min(), df["Actual"].max()],
         [df["Actual"].min(), df["Actual"].max()],
         'r--', label="y=x Ideal Line", linewidth=1.5)
plt.xlabel("Actual Distance")
plt.ylabel("OSRM Distance")
plt.title(f"OSRM vs Actual Distance (Pearson's r = {pearson_r:.3f})")
plt.grid(True, alpha=0.3)              # Add grid
plt.legend()
plt.show()

# ===== 5. Error Distribution =====
plt.figure(figsize=(8,5))
# Calculate standard deviation
error_std = np.std(df["Error"])
error_mean = np.mean(df["Error"])

# Plot histogram with reasonable range (±3 sigma)
plt.hist(df["Error"], bins=30, alpha=0.7, range=(error_mean - 3*error_std, error_mean + 3*error_std))

# Add vertical lines for mean and standard deviations
plt.axvline(error_mean, color="red", linestyle="--", label=f"Mean Error = {error_mean:.2f}")
# 1 sigma (68.27%)
plt.axvline(error_mean + error_std, color="orange", linestyle="--", label=f"±1σ = {error_std:.2f}")
plt.axvline(error_mean - error_std, color="orange", linestyle="--")
# 2 sigma (95.45%)
plt.axvline(error_mean + 2*error_std, color="green", linestyle="--", label=f"±2σ = {2*error_std:.2f}")
plt.axvline(error_mean - 2*error_std, color="green", linestyle="--")
# Add MAE lines
plt.axvline(mae, color="purple", linestyle=":", label=f"±MAE = {mae:.2f}")
plt.axvline(-mae, color="purple", linestyle=":")

plt.xlabel("Error (OSRM - Actual)")
plt.ylabel("Frequency")
plt.title("Error Distribution with Standard Deviations")
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()
