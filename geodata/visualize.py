import json
import duckdb
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as mcolors
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection

con = duckdb.connect("data/wiesbaden.duckdb", read_only=True)
con.execute("LOAD spatial")

df = con.execute("""
    SELECT
        o.ortsbezirk_name AS name,
        o.bevoelkerungsbestand,
        o.anteil_auslaender,
        o.anteil_migrationshintergrund,
        m.angebotsmieten,
        m.sozialwohnungen,
        m.anzahl_der_angebotenen_mietwohnungen
    FROM ortsbezirke o
    JOIN mieten m ON o.ortsbezirk_id = m.ortsbezirk_id
    WHERE m.jahr = (SELECT MAX(jahr) FROM mieten)
      AND m.ortsbezirk_id != '00'
""").fetchall()

cols = ["name", "bevoelkerungsbestand", "anteil_auslaender",
        "anteil_migrationshintergrund", "angebotsmieten",
        "sozialwohnungen", "anzahl_mietwohnungen"]
data = {col: [row[i] for row in df] for i, col in enumerate(cols)}

con.close()

with open("data/ortsbezirke.geojson") as f:
    geo = json.load(f)


# --- Figure 1: Side-by-side maps ---

def plot_map(ax, values, title, label, fmt=".1f", cmap_name="YlOrRd"):
    vmin = min(values.values())
    vmax = max(values.values())
    norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
    cmap = plt.get_cmap(cmap_name)

    patches = []
    colors = []
    for feature in geo["features"]:
        name = feature["properties"]["name"]
        if name not in values:
            continue
        coords = feature["geometry"]["coordinates"][0]
        poly = Polygon(coords, closed=True)
        patches.append(poly)
        colors.append(cmap(norm(values[name])))

        xs = [c[0] for c in coords]
        ys = [c[1] for c in coords]
        cx, cy = sum(xs) / len(xs), sum(ys) / len(ys)
        ax.text(cx, cy, f"{name}\n{values[name]:{fmt}}",
                ha="center", va="center", fontsize=5.5, fontweight="bold",
                color="black",
                bbox=dict(boxstyle="round,pad=0.15", fc="white", alpha=0.75, lw=0))

    pc = PatchCollection(patches, facecolors=colors, edgecolors="white", linewidths=1.0)
    ax.add_collection(pc)
    ax.autoscale_view()
    ax.set_aspect("equal")
    ax.set_title(title, fontsize=13, fontweight="bold", pad=12)
    ax.axis("off")

    sm = cm.ScalarMappable(cmap=cmap, norm=norm)
    plt.colorbar(sm, ax=ax, shrink=0.75, label=label, pad=0.02)


migr_values = dict(zip(data["name"], data["anteil_migrationshintergrund"]))
miete_values = dict(zip(data["name"], data["angebotsmieten"]))

fig1, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))
plot_map(ax1, migr_values, "Anteil Migrationshintergrund (Jan 2026)", "%",
         cmap_name="YlOrRd")
plot_map(ax2, miete_values, "Angebotsmieten (2023)", "€/m²",
         cmap_name="YlGnBu")
fig1.suptitle("Wiesbaden – Migrationshintergrund & Mietpreise",
              fontsize=16, fontweight="bold", y=0.97)
plt.tight_layout()
fig1.savefig("data/wiesbaden_maps.png", dpi=200, bbox_inches="tight")


# --- Figure 2: Correlation analysis ---

numeric_cols = ["anteil_auslaender", "anteil_migrationshintergrund",
                "angebotsmieten", "sozialwohnungen", "anzahl_mietwohnungen",
                "bevoelkerungsbestand"]
nice_labels = ["Ausländeranteil %", "Migrationshintergrund %",
               "Angebotsmieten €/m²", "Sozialwohnungen", "Angebotene Mietwhg.",
               "Bevölkerung"]

matrix = np.array([[float(v) for v in data[c]] for c in numeric_cols])
corr = np.corrcoef(matrix)
n_vars = len(numeric_cols)

fig2, axes = plt.subplots(2, 2, figsize=(16, 14))

# top-left: correlation heatmap
ax = axes[0, 0]
im = ax.imshow(corr, cmap="RdBu_r", vmin=-1, vmax=1, aspect="equal")
ax.set_xticks(range(n_vars))
ax.set_yticks(range(n_vars))
ax.set_xticklabels(nice_labels, rotation=45, ha="right", fontsize=8)
ax.set_yticklabels(nice_labels, fontsize=8)
for i in range(n_vars):
    for j in range(n_vars):
        ax.text(j, i, f"{corr[i, j]:.2f}", ha="center", va="center",
                fontsize=8, color="white" if abs(corr[i, j]) > 0.6 else "black")
plt.colorbar(im, ax=ax, shrink=0.8, label="Pearson r")
ax.set_title("Korrelationsmatrix", fontsize=12, fontweight="bold")

# top-right: Migrationshintergrund vs Angebotsmieten
ax = axes[0, 1]
ax.scatter(data["anteil_migrationshintergrund"], data["angebotsmieten"],
           s=np.array(data["bevoelkerungsbestand"]) / 80, alpha=0.7,
           edgecolors="black", linewidths=0.5, c=data["anteil_migrationshintergrund"],
           cmap="YlOrRd")
for name, x, y in zip(data["name"], data["anteil_migrationshintergrund"], data["angebotsmieten"]):
    ax.annotate(name, (x, y), fontsize=5.5, ha="center", va="bottom",
                xytext=(0, 5), textcoords="offset points")
z = np.polyfit(data["anteil_migrationshintergrund"], data["angebotsmieten"], 1)
xline = np.linspace(min(data["anteil_migrationshintergrund"]),
                     max(data["anteil_migrationshintergrund"]), 100)
ax.plot(xline, np.polyval(z, xline), "r--", alpha=0.7, lw=1.5)
r = corr[numeric_cols.index("anteil_migrationshintergrund")][numeric_cols.index("angebotsmieten")]
ax.set_xlabel("Migrationshintergrund (%)", fontsize=10)
ax.set_ylabel("Angebotsmieten (€/m²)", fontsize=10)
ax.set_title(f"Migration vs. Miete (r = {r:.2f})", fontsize=12, fontweight="bold")

# bottom-left: Ausländeranteil vs Angebotsmieten
ax = axes[1, 0]
ax.scatter(data["anteil_auslaender"], data["angebotsmieten"],
           s=np.array(data["bevoelkerungsbestand"]) / 80, alpha=0.7,
           edgecolors="black", linewidths=0.5, c=data["anteil_auslaender"],
           cmap="YlOrRd")
for name, x, y in zip(data["name"], data["anteil_auslaender"], data["angebotsmieten"]):
    ax.annotate(name, (x, y), fontsize=5.5, ha="center", va="bottom",
                xytext=(0, 5), textcoords="offset points")
z = np.polyfit(data["anteil_auslaender"], data["angebotsmieten"], 1)
xline = np.linspace(min(data["anteil_auslaender"]), max(data["anteil_auslaender"]), 100)
ax.plot(xline, np.polyval(z, xline), "r--", alpha=0.7, lw=1.5)
r = corr[numeric_cols.index("anteil_auslaender")][numeric_cols.index("angebotsmieten")]
ax.set_xlabel("Ausländeranteil (%)", fontsize=10)
ax.set_ylabel("Angebotsmieten (€/m²)", fontsize=10)
ax.set_title(f"Ausländeranteil vs. Miete (r = {r:.2f})", fontsize=12, fontweight="bold")

# bottom-right: Sozialwohnungen vs Migrationshintergrund
ax = axes[1, 1]
ax.scatter(data["sozialwohnungen"], data["anteil_migrationshintergrund"],
           s=np.array(data["bevoelkerungsbestand"]) / 80, alpha=0.7,
           edgecolors="black", linewidths=0.5, c=data["sozialwohnungen"],
           cmap="PuBuGn")
for name, x, y in zip(data["name"], data["sozialwohnungen"], data["anteil_migrationshintergrund"]):
    ax.annotate(name, (x, y), fontsize=5.5, ha="center", va="bottom",
                xytext=(0, 5), textcoords="offset points")
z = np.polyfit(data["sozialwohnungen"], data["anteil_migrationshintergrund"], 1)
xline = np.linspace(min(data["sozialwohnungen"]), max(data["sozialwohnungen"]), 100)
ax.plot(xline, np.polyval(z, xline), "r--", alpha=0.7, lw=1.5)
r = corr[numeric_cols.index("sozialwohnungen")][numeric_cols.index("anteil_migrationshintergrund")]
ax.set_xlabel("Sozialwohnungen (Anzahl)", fontsize=10)
ax.set_ylabel("Migrationshintergrund (%)", fontsize=10)
ax.set_title(f"Sozialwohnungen vs. Migration (r = {r:.2f})", fontsize=12, fontweight="bold")

fig2.suptitle("Wiesbaden – Korrelationsanalyse", fontsize=16, fontweight="bold", y=0.98)
plt.tight_layout()
fig2.savefig("data/wiesbaden_correlations.png", dpi=200, bbox_inches="tight")

import subprocess
subprocess.Popen(["open", "data/wiesbaden_maps.png"])
subprocess.Popen(["open", "data/wiesbaden_correlations.png"])
