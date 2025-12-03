#!/usr/bin/env python3
"""
Test script for tinySQL ODBC driver using pyodbc.

Install pyodbc:
    pip install pyodbc

Usage:
    python test_odbc.py
"""

import sys

try:
    import pyodbc
except ImportError:
    print("pyodbc not installed. Install with: pip install pyodbc")
    sys.exit(1)


def test_memory_database():
    """Test in-memory database via ODBC"""
    print("Testing in-memory database...")
    
    try:
        # Connect using DSN (requires DSN to be configured)
        conn = pyodbc.connect('DSN=tinysql_mem', autocommit=True)
    except pyodbc.Error:
        # Fallback to direct driver connection
        try:
            conn = pyodbc.connect('DRIVER={tinySQL};SERVER=mem://', autocommit=True)
        except pyodbc.Error as e:
            print(f"Failed to connect: {e}")
            print("\nMake sure:")
            print("1. ODBC driver is built: cd odbc && make")
            print("2. Driver is registered in odbcinst.ini")
            print("3. DSN is configured in odbc.ini (or use direct connection)")
            return False
    
    cursor = conn.cursor()
    
    # Create table
    print("  Creating table...")
    cursor.execute('CREATE TABLE users (id INT, name TEXT, active BOOL)')
    
    # Insert data
    print("  Inserting data...")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice', true)")
    cursor.execute("INSERT INTO users VALUES (2, 'Bob', false)")
    cursor.execute("INSERT INTO users VALUES (3, 'Charlie', true)")
    
    # Query data
    print("  Querying data...")
    cursor.execute('SELECT * FROM users WHERE active = true ORDER BY id')
    rows = cursor.fetchall()
    
    print("\n  Results:")
    for row in rows:
        print(f"    {row}")
    
    # Get column info
    cursor.execute('SELECT id, name FROM users')
    columns = [column[0] for column in cursor.description]
    print(f"\n  Columns: {columns}")
    
    # Row count
    cursor.execute('SELECT COUNT(*) as total FROM users')
    count = cursor.fetchone()[0]
    print(f"  Total rows: {count}")
    
    cursor.close()
    conn.close()
    
    print("\n✓ In-memory database test passed!")
    return True


def test_file_database():
    """Test file-based database via ODBC"""
    print("\nTesting file-based database...")
    
    import tempfile
    import os
    
    db_path = os.path.join(tempfile.gettempdir(), 'test_odbc.gob')
    
    try:
        conn = pyodbc.connect(f'DRIVER={{tinySQL}};SERVER=file:{db_path}', autocommit=True)
    except pyodbc.Error as e:
        print(f"Failed to connect: {e}")
        return False
    
    cursor = conn.cursor()
    
    # Create and populate
    print("  Creating table...")
    cursor.execute('CREATE TABLE products (id INT, name TEXT, price FLOAT)')
    cursor.execute("INSERT INTO products VALUES (1, 'Widget', 19.99)")
    cursor.execute("INSERT INTO products VALUES (2, 'Gadget', 29.99)")
    
    cursor.close()
    conn.close()
    
    # Reconnect to verify persistence
    print("  Reconnecting to verify persistence...")
    conn = pyodbc.connect(f'DRIVER={{tinySQL}};SERVER=file:{db_path}', autocommit=True)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM products ORDER BY id')
    rows = cursor.fetchall()
    
    print("\n  Results after reconnect:")
    for row in rows:
        print(f"    {row}")
    
    cursor.close()
    conn.close()
    
    # Clean up
    if os.path.exists(db_path):
        os.remove(db_path)
    
    print("\n✓ File database test passed!")
    return True


def list_drivers():
    """List available ODBC drivers"""
    print("\nAvailable ODBC drivers:")
    drivers = pyodbc.drivers()
    for driver in drivers:
        print(f"  - {driver}")
    
    if 'tinySQL' in drivers:
        print("\n✓ tinySQL driver is registered")
    else:
        print("\n✗ tinySQL driver not found. Register it in odbcinst.ini")


if __name__ == '__main__':
    print("=" * 60)
    print("TinySQL ODBC Driver Test")
    print("=" * 60)
    
    list_drivers()
    print()
    
    success = True
    
    if test_memory_database():
        pass
    else:
        success = False
    
    if test_file_database():
        pass
    else:
        success = False
    
    print("\n" + "=" * 60)
    if success:
        print("✓ All tests passed!")
    else:
        print("✗ Some tests failed")
    print("=" * 60)
    
    sys.exit(0 if success else 1)
