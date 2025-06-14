import os
import pandas as pd
from map_orders import map_orders
# ─── 1. Ensure output folder exists ───────────────────────────────────────────
os.makedirs("output", exist_ok=True)

# ─── 2. Load Excel sheets ────────────────────────────────────────────────────
inventory = pd.read_excel("data/WMS-04-02.xlsx", sheet_name="Current_Inventory")
combos    = pd.read_excel("data/WMS-04-02.xlsx", sheet_name="Combos skus")
mapping   = pd.read_excel("data/WMS-04-02.xlsx", sheet_name="Msku With Skus")

# ─── 3. Clean & normalize column names ────────────────────────────────────────
inventory.columns = [
    str(col).strip().lower().replace(" ", "_")
    for col in inventory.columns
]
combos.columns = [str(col).strip() for col in combos.columns]
mapping.columns = [
    str(col).strip().lower().replace(" ", "_")
    for col in mapping.columns
]

# ─── 4. Clean key fields ─────────────────────────────────────────────────────
inventory["msku"] = inventory["msku"].astype(str).str.strip()
mapping["sku"]   = mapping["sku"].astype(str).str.strip()
mapping["msku"]  = mapping["msku"].astype(str).str.strip()

# ─── 5. Compute available_stock in inventory ─────────────────────────────────
inventory["available_stock"] = (
    pd.to_numeric(inventory["opening_stock"], errors="coerce")
      .fillna(0)
      .astype(int)
)

# ─── 6. Merge inventory ↔ SKU Mapping ────────────────────────────────────────
merged = pd.merge(
    inventory,
    mapping,
    on="msku",
    how="left"
)

# ─── 7. Build combo → component SKU list ─────────────────────────────────────
combo_mappings = []
for _, row in combos.iterrows():
    combo_msku = row["Combo"]
    for i in range(1, 15):
        sku_col = f"SKU{i}"
        if sku_col in row and pd.notna(row[sku_col]):
            combo_mappings.append({
                "combo_msku":      combo_msku,
                "component_sku":   str(row[sku_col]).strip()
            })

combo_df = pd.DataFrame(combo_mappings)

# ─── 8. Compute total stock per SKU ──────────────────────────────────────────
sku_stock = merged.groupby("sku")["available_stock"].sum().to_dict()

# ─── 9. Compute combo availability ───────────────────────────────────────────
combo_stock_list = []
for combo_msku in combo_df["combo_msku"].unique():
    parts = combo_df.loc[
        combo_df["combo_msku"] == combo_msku, "component_sku"
    ]
    stocks = [sku_stock.get(sku, 0) for sku in parts]

    combo_stock_list.append({
        "combo_msku":             combo_msku,
        "available_combo_stock":  min(stocks) if stocks else 0,
        "components_count":       len(stocks),
        "components":             list(parts),
        "stocks_per_component":   stocks
    })

combo_stock_df = pd.DataFrame(combo_stock_list)

# ─── 10. Output ───────────────────────────────────────────────────────────────
print("\n📦 Combo Stock Availability (first 10 rows):\n")
print(combo_stock_df.head(10))

# # ─── 11. Export to Excel ─────────────────────────────────────────────────────
# out_path = "output/combo_stock_availability.xlsx"
# combo_stock_df.to_excel(out_path, index=False)
# print(f"\n✅ Exported combo availability to: {out_path}")


# ─── 11. Export merged inventory & combo CSVs for map_orders ────────────────
merged.to_csv("output/merged_inventory.csv", index=False)
combo_stock_df.to_csv("output/combo_stock.csv", index=False)
print("\n✅ Exported merged inventory to: output/merged_inventory.csv")
print("✅ Exported combo stock to:    output/combo_stock.csv")

# ─── 12. Now map your orders CSV (will read the above two) ───────────────────
map_orders("data/Orders_2025-01-26_2025-02-01_2025-02-04_12_12-12_17_528114.csv")


