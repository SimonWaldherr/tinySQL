#!/usr/bin/env python3
import pyodbc

# Create a test database with tables
conn = pyodbc.connect('DSN=tinysql_file;Database=file:/tmp/powerbi_test.gob')
cursor = conn.cursor()

# Create sample tables
cursor.execute('CREATE TABLE customers (id INT, name TEXT, email TEXT)')
cursor.execute("INSERT INTO customers VALUES (1, 'John Doe', 'john@example.com')")
cursor.execute("INSERT INTO customers VALUES (2, 'Jane Smith', 'jane@example.com')")

cursor.execute('CREATE TABLE orders (order_id INT, customer_id INT, amount DECIMAL)')
cursor.execute("INSERT INTO orders VALUES (101, 1, 99.99)")
cursor.execute("INSERT INTO orders VALUES (102, 2, 149.50)")

conn.commit()

# Test metadata functions
print("=== Testing Metadata Functions ===\n")

# List tables
print("Tables:")
for table in cursor.tables():
    print(f"  - {table.table_name} ({table.table_type})")

print("\nColumns in 'customers' table:")
for col in cursor.columns(table='customers'):
    print(f"  - {col.column_name}: {col.type_name} (size: {col.column_size})")

print("\nColumns in 'orders' table:")
for col in cursor.columns(table='orders'):
    print(f"  - {col.column_name}: {col.type_name} (size: {col.column_size})")

# Query data
print("\n=== Query Results ===\n")
cursor.execute('SELECT * FROM customers')
print("Customers:")
for row in cursor:
    print(f"  {row}")

cursor.execute('SELECT * FROM orders')
print("\nOrders:")
for row in cursor:
    print(f"  {row}")

cursor.close()
conn.close()

print("\n✓ Metadata functions working! Ready for Power BI.")
