import mysql.connector
from datetime import datetime

# --- Connections ---
conn_src = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Kaikoth@7",
    database="fleximart"
)

conn_dw = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Kaikoth@7",
    database="fleximart_dw"
)

src = conn_src.cursor(dictionary=True)
dw = conn_dw.cursor(buffered=True)


# --- Load customers into dim_customer ---
src.execute("SELECT * FROM customers")
for c in src.fetchall():
    full_name = f"{c['first_name']} {c['last_name']}"
    dw.execute(
        """
        INSERT INTO dim_customer (customer_id, customer_name, email, city)
        VALUES (%s,%s,%s,%s)
        """,
        (c["customer_id"], full_name, c["email"], c["city"])
    )

# --- Load products into dim_product ---
src.execute("SELECT * FROM products")
for p in src.fetchall():
    dw.execute(
        """
        INSERT INTO dim_product (product_id, product_name, category, price)
        VALUES (%s,%s,%s,%s)
        """,
        (p["product_id"], p["product_name"], p["category"], p["price"])
    )

# --- Load dates + sales facts ---
src.execute("""
    SELECT 
        o.order_date,
        oi.quantity,
        oi.subtotal,
        o.customer_id,
        oi.product_id
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
""")

for r in src.fetchall():
    date = r["order_date"]

    # Date dimension
    dw.execute(
        """
        INSERT IGNORE INTO dim_date (full_date, year, month, month_name)
        VALUES (%s,%s,%s,%s)
        """,
        (date, date.year, date.month, date.strftime("%B"))
    )

    dw.execute("SELECT date_key FROM dim_date WHERE full_date = %s", (date,))
    date_key = dw.fetchone()[0]

    # Customer key
    dw.execute(
        "SELECT customer_key FROM dim_customer WHERE customer_id = %s",
        (r["customer_id"],)
    )
    customer_key = dw.fetchone()[0]

    # Product key
    dw.execute(
        "SELECT product_key FROM dim_product WHERE product_id = %s",
        (r["product_id"],)
    )
    product_key = dw.fetchone()[0]

    # Fact table insert
    dw.execute(
        """
        INSERT INTO fact_sales
        (customer_key, product_key, date_key, quantity, total_amount)
        VALUES (%s,%s,%s,%s,%s)
        """,
        (customer_key, product_key, date_key, r["quantity"], r["subtotal"])
    )

conn_dw.commit()
print("Data warehouse populated successfully.")
