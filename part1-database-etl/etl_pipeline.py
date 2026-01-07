

import os
import argparse
import pandas as pd
import mysql.connector
from datetime import datetime
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# ---------- Helpers ----------
def standardize_phone(p):
    if pd.isna(p): return None
    s = str(p).strip()
    # remove spaces and dashes
    s = re.sub(r'[ \-()]+', '', s)
    # if starts with 0 and 11 digits -> remove leading 0
    if s.startswith('0') and len(s) >= 11:
        s = s.lstrip('0')
    # if starts with country code without +, add +91
    if s.startswith('91') and not s.startswith('+'):
        s = '+' + s
    if not s.startswith('+'):
        # assume India and prepend +91 if 10 digits
        if len(s) == 10:
            s = '+91' + s
    return s

def parse_date(d):
    if pd.isna(d): return None
    s = str(d).strip()
    for fmt in ('%Y-%m-%d','%d/%m/%Y','%m-%d-%Y','%d-%m-%Y','%Y/%m/%d'):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    # last resort: try pandas
    try:
        return pd.to_datetime(s, dayfirst=False).date()
    except:
        return None

# ---------- Main ETL ----------
def main(csv_dir, host, user, password, port=3306):
    # file paths
    f_customers = os.path.join(csv_dir, 'customers_raw.csv')
    f_products = os.path.join(csv_dir, 'products_raw.csv')
    f_sales = os.path.join(csv_dir, 'sales_raw.csv')

    report = {'customers':{}, 'products':{}, 'sales':{}}

    # ---------- READ ----------
    df_c = pd.read_csv(f_customers)
    df_p = pd.read_csv(f_products)
    df_s = pd.read_csv(f_sales)

    report['customers']['raw_records'] = len(df_c)
    report['products']['raw_records'] = len(df_p)
    report['sales']['raw_records'] = len(df_s)

    # ---------- TRANSFORM - CUSTOMERS ----------
    # Trim whitespace
    df_c = df_c.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Deduplicate on customer_id + email (keep first)
    before = len(df_c)
    df_c = df_c.drop_duplicates(subset=['customer_id','email'], keep='first')
    report['customers']['duplicates_removed'] = before - len(df_c)

    # Standardize phone
    df_c['phone'] = df_c['phone'].apply(standardize_phone)

    # Normalize city case (Title Case)
    df_c['city'] = df_c['city'].apply(lambda x: str(x).title() if pd.notna(x) else x)

    # Parse registration_date
    df_c['registration_date'] = df_c['registration_date'].apply(parse_date)

    # Handle missing emails: generate placeholder using customer_id
    missing_emails = df_c['email'].isna().sum()
    report['customers']['missing_emails'] = int(missing_emails)
    df_c['email'] = df_c.apply(lambda row: row['email'] if pd.notna(row['email'])
                               else f"{row['first_name'].lower()}.{row['last_name'].lower()}@{row['customer_id'].lower()}.local", axis=1)

    # FINAL customers count
    report['customers']['cleaned_records'] = len(df_c)

    # ---------- TRANSFORM - PRODUCTS ----------
    df_p = df_p.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Standardize category names -> Title Case
    df_p['category'] = df_p['category'].str.title()

    # Fill missing stock_quantity with 0
    df_p['stock_quantity'] = df_p['stock_quantity'].fillna(0).astype(int)

    # Handle missing prices: fill with median price of category or overall median
    df_p['price'] = pd.to_numeric(df_p['price'], errors='coerce')
    missing_prices = df_p['price'].isna().sum()
    report['products']['missing_prices'] = int(missing_prices)
    for cat in df_p['category'].unique():
        median = df_p[df_p['category']==cat]['price'].median()
        df_p.loc[(df_p['category']==cat) & (df_p['price'].isna()), 'price'] = median
    # Any remaining NaN -> fill with overall median
    df_p['price'] = df_p['price'].fillna(df_p['price'].median())

    report['products']['cleaned_records'] = len(df_p)

    # ---------- TRANSFORM - SALES ----------
    df_s = df_s.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Date normalization
    df_s['transaction_date'] = df_s['transaction_date'].apply(parse_date)

    # Remove duplicate transactions by transaction_id (keep first)
    before = len(df_s)
    df_s = df_s.drop_duplicates(subset=['transaction_id'], keep='first')
    report['sales']['duplicates_removed'] = before - len(df_s)

    # Identify and drop rows missing customer_id or product_id (cannot load)
    missing_customer = df_s['customer_id'].isna().sum()
    missing_product = df_s['product_id'].isna().sum()
    report['sales']['missing_customer_ids'] = int(missing_customer)
    report['sales']['missing_product_ids'] = int(missing_product)

    df_s = df_s.dropna(subset=['customer_id','product_id']).copy()

    # Convert numeric columns
    df_s['quantity'] = pd.to_numeric(df_s['quantity'], errors='coerce').fillna(0).astype(int)
    df_s['unit_price'] = pd.to_numeric(df_s['unit_price'], errors='coerce').fillna(0.0)
    # compute subtotal
    df_s['subtotal'] = df_s['quantity'] * df_s['unit_price']

    report['sales']['cleaned_records'] = len(df_s)

    # ---------- LOAD (MySQL) ----------
    conn = mysql.connector.connect(host=host, user=user, password=password, port=port)
    cursor = conn.cursor()

    # Create DB and tables (use provided schema exactly)
    cursor.execute("CREATE DATABASE IF NOT EXISTS fleximart;")
    cursor.execute("USE fleximart;")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INT PRIMARY KEY AUTO_INCREMENT,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        phone VARCHAR(20),
        city VARCHAR(50),
        registration_date DATE
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INT PRIMARY KEY AUTO_INCREMENT,
        product_code VARCHAR(20), -- store original code for mapping
        product_name VARCHAR(100) NOT NULL,
        category VARCHAR(50) NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        stock_quantity INT DEFAULT 0
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id INT PRIMARY KEY AUTO_INCREMENT,
        customer_id INT NOT NULL,
        order_date DATE NOT NULL,
        total_amount DECIMAL(10,2) NOT NULL,
        status VARCHAR(20) DEFAULT 'Pending',
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS order_items (
        order_item_id INT PRIMARY KEY AUTO_INCREMENT,
        order_id INT NOT NULL,
        product_id INT NOT NULL,
        quantity INT NOT NULL,
        unit_price DECIMAL(10,2) NOT NULL,
        subtotal DECIMAL(10,2) NOT NULL,
        FOREIGN KEY (order_id) REFERENCES orders(order_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );
    """)
    conn.commit()

    # Insert customers
    inserted_customers = 0
    for _, r in df_c.iterrows():
        try:
            cursor.execute("""
                INSERT INTO customers (first_name,last_name,email,phone,city,registration_date)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (r['first_name'], r['last_name'], r['email'], r['phone'], r['city'], r['registration_date']))
            inserted_customers += 1
        except mysql.connector.IntegrityError as e:
            logging.warning(f"Skipping customer due to DB error: {e}")
    conn.commit()
    report['customers']['loaded'] = inserted_customers

    # Insert products and keep mapping original product_id -> db product_id
    product_code_to_dbid = {}
    inserted_products = 0
    for _, r in df_p.iterrows():
        cursor.execute("""
            INSERT INTO products (product_code,product_name,category,price,stock_quantity)
            VALUES (%s,%s,%s,%s,%s)
        """, (r['product_id'], r['product_name'], r['category'], float(r['price']), int(r['stock_quantity'])))
        dbid = cursor.lastrowid
        product_code_to_dbid[r['product_id']] = dbid
        inserted_products += 1
    conn.commit()
    report['products']['loaded'] = inserted_products

    # Insert orders + order_items.
    # We'll aggregate sales by transaction_id -> make one order per transaction (simple approach)
    inserted_orders = 0
    inserted_order_items = 0
    for tid, group in df_s.groupby('transaction_id'):
        # map customer external code -> customers table: find customer_id using email or first/last? We inserted customers with auto-IDs, but didn't store external customer_id.
        # We stored only auto-increment DB ids; need mapping from customer code to DB id using first_name/last_name? Simpler: customer_id external like 'C001' not in DB. So store a temporary lookup by matching email generated earlier.
        # To simplify, we rely on the fact customers were inserted in same order as df_c and correspond to external customer_id order. Create mapping by reading customers table with an additional helper mapping table.
        pass

    # Build mapping of external customers codes -> inserted DB ids by reading customers table in insertion order
    cursor.execute("SELECT customer_id, email FROM customers;")
    db_customers = cursor.fetchall()
    # Build a dict: email -> customer_id
    email_to_dbid = {email: cid for (cid, email) in db_customers}

    # For safety: create mapping from external customer code to email using original df_c
    external_customer_to_email = {}
    for _, r in df_c.iterrows():
        external_customer_to_email[r['customer_id']] = r['email']

    # Now insert orders
    for tid, group in df_s.groupby('transaction_id'):
        # get external customer code from one row
        ext_c = group['customer_id'].iloc[0]
        email = external_customer_to_email.get(ext_c)
        db_cid = email_to_dbid.get(email)
        if db_cid is None:
            logging.warning(f"Skipping transaction {tid} because customer mapping not found.")
            continue
        order_date = group['transaction_date'].iloc[0] or datetime.today().date()
        total_amount = float(group['subtotal'].sum())
        status = group['status'].iloc[0] if 'status' in group.columns else 'Completed'
        cursor.execute("""
            INSERT INTO orders (customer_id,order_date,total_amount,status)
            VALUES (%s,%s,%s,%s)
        """, (db_cid, order_date, total_amount, status))
        order_dbid = cursor.lastrowid
        inserted_orders += 1

        # insert order_items rows (but product_id must be mapped)
        for _, row in group.iterrows():
            prod_code = row['product_id']
            prod_dbid = product_code_to_dbid.get(prod_code)
            if prod_dbid is None:
                logging.warning(f"Skipping order_item for {tid} product {prod_code} (no product mapping)")
                continue
            cursor.execute("""
                INSERT INTO order_items (order_id,product_id,quantity,unit_price,subtotal)
                VALUES (%s,%s,%s,%s,%s)
            """, (order_dbid, prod_dbid, int(row['quantity']), float(row['unit_price']), float(row['subtotal'])))
            inserted_order_items += 1
    conn.commit()
    report['sales']['orders_loaded'] = inserted_orders
    report['sales']['order_items_loaded'] = inserted_order_items

    # CLOSE
    cursor.close()
    conn.close()

    # Generate a simple data_quality_report.txt
    with open('part1-database-etl/data_quality_report.txt', 'w') as fh:
        fh.write("DATA QUALITY REPORT\n")
        fh.write("===================\n\n")
        for k in report:
            fh.write(f"{k.upper()}:\n")
            for kk, vv in report[k].items():
                fh.write(f"  {kk}: {vv}\n")
            fh.write("\n")
    logging.info("ETL finished. See part1-database-etl/data_quality_report.txt")
    print(report)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv_dir', default='data', help='Directory with customers_raw.csv products_raw.csv sales_raw.csv')
    parser.add_argument('--db_host', default='localhost')
    parser.add_argument('--db_user', default='root')
    parser.add_argument('--db_pass', default='')
    parser.add_argument('--db_port', default=3306, type=int)
    args = parser.parse_args()
    main(args.csv_dir, args.db_host, args.db_user, args.db_pass, args.db_port)
