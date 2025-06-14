import pandas as pd

def map_orders(order_file,
               inventory_file="output/merged_inventory.csv",
               combo_file="output/combo_stock.csv"):
    # ─── 1. Load input data ─────────────────────────────────────────────
    orders    = pd.read_csv(order_file)
    inventory = pd.read_csv(inventory_file)
    combos    = pd.read_csv(combo_file)

    # ─── 2. Normalize all column names to lowercase ─────────────────────
    for df in (orders, inventory, combos):
        df.columns = df.columns.str.strip().str.lower()

    # ─── 3. Standardize inventory stock column ──────────────────────────
    if "available_stock" in inventory.columns:
        inventory["stock"] = inventory["available_stock"]
    elif "opening_stock" in inventory.columns:
        inventory["stock"] = inventory["opening_stock"]
    else:
        raise KeyError("No stock column found in inventory!")

    # ─── 4. Standardize combo stock column ──────────────────────────────
    if "available_combo_stock" in combos.columns:
        combos["stock"] = combos["available_combo_stock"]
    elif "stock" not in combos.columns:
        raise KeyError("No stock column found in combo data!")

    # ─── 5. Auto-detect order_id column ─────────────────────────────────
    possible_id_cols = ["order id", "sub order no", "order_no", "orderid"]
    order_id_col = next((col for col in orders.columns if col in possible_id_cols), None)
    if not order_id_col:
        raise KeyError("Couldn't find order ID column in orders sheet!")

    results = []

    # ─── 6. Loop over each order ────────────────────────────────────────
    for _, order in orders.iterrows():
        sku = str(order.get("sku", "")).strip()
        qty = int(order.get("quantity", 0))
        oid = order.get(order_id_col, "N/A")

        # ── Check Simple SKU ────────────────────────────────────────────
        inv_rows = inventory[inventory["sku"] == sku]
        if not inv_rows.empty:
            available = int(inv_rows.iloc[0]["stock"])
            status = (
                "Available" if available >= qty
                else "Partial" if available > 0
                else "Out of Stock"
            )
            results.append({
                "order_id":        oid,
                "type":            "simple",
                "sku":             sku,
                "msku":            inv_rows.iloc[0].get("msku", ""),
                "ordered_qty":     qty,
                "available_stock": available,
                "status":          status
            })
            continue

        # ── Check Combo SKU ─────────────────────────────────────────────
        key = "combo_msku" if "combo_msku" in combos.columns else "msku"
        combo_rows = combos[combos[key] == sku]
        if not combo_rows.empty:
            available = int(combo_rows.iloc[0]["stock"])
            status = (
                "Available" if available >= qty
                else "Partial" if available > 0
                else "Out of Stock"
            )
            results.append({
                "order_id":        oid,
                "type":            "combo",
                "sku":             sku,
                "msku":            sku,
                "ordered_qty":     qty,
                "available_stock": available,
                "status":          status
            })
            continue

        # ── Fallback: Unmapped SKU ─────────────────────────────────────
        results.append({
            "order_id":        oid,
            "type":            "unknown",
            "sku":             sku,
            "msku":            "",
            "ordered_qty":     qty,
            "available_stock": 0,
            "status":          "Unmapped"
        })

    # ─── 7. Output Results ──────────────────────────────────────────────
    out_df = pd.DataFrame(results)
    out_df.to_csv("output/mapped_orders.csv", index=False)
    print("✅ Mapped orders saved to output/mapped_orders.csv")
