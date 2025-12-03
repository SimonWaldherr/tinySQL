# Build ODBC driver for different platforms

.PHONY: all linux macos windows windows-cross clean test install powerbi-demo powerbi-package

all: linux

linux:
	CGO_ENABLED=1 go build -buildmode=c-shared -o libtinysqlodbc.so .
	@echo "Built libtinysqlodbc.so"

macos:
	CGO_ENABLED=1 go build -buildmode=c-shared -o libtinysqlodbc.dylib .
	@echo "Built libtinysqlodbc.dylib"

# Build for Windows (requires mingw-w64 cross-compiler)
windows:
	CGO_ENABLED=1 GOOS=windows GOARCH=amd64 CC=x86_64-w64-mingw32-gcc go build -buildmode=c-shared -o tinysqlodbc.dll .
	@echo "Built tinysqlodbc.dll"
	@echo ""
	@echo "To install on Windows:"
	@echo "  1. Copy tinysqlodbc.dll to a permanent location"
	@echo "  2. Run powerbi_setup.ps1 as Administrator"

# Alternative: Build natively on Windows
windows-native:
	@echo "Run this command on Windows with MinGW-w64 installed:"
	@echo "  CGO_ENABLED=1 go build -buildmode=c-shared -o tinysqlodbc.dll ."

clean:
	rm -f libtinysqlodbc.so libtinysqlodbc.dylib libtinysqlodbc.h tinysqlodbc.dll tinysqlodbc.h
	rm -f powerbi_demo.gob sample_data.sql
	rm -rf powerbi_package powerbi_package.zip

test:
	@echo "Testing ODBC driver installation..."
	@if command -v odbcinst >/dev/null 2>&1; then \
		odbcinst -q -d | grep -q tinySQL && echo "✓ Driver registered" || echo "✗ Driver not registered"; \
	else \
		echo "unixODBC not installed. Install with: apt-get install unixodbc unixodbc-dev"; \
	fi

install-linux: linux
	@echo "Installing to /usr/local/lib..."
	sudo cp libtinysqlodbc.so /usr/local/lib/
	sudo chmod 755 /usr/local/lib/libtinysqlodbc.so
	@echo ""
	@echo "Add to /etc/odbcinst.ini or ~/.odbcinst.ini:"
	@echo ""
	@echo "[tinySQL]"
	@echo "Description = TinySQL ODBC Driver"
	@echo "Driver = /usr/local/lib/libtinysqlodbc.so"
	@echo "Setup = /usr/local/lib/libtinysqlodbc.so"
	@echo "FileUsage = 1"

install-macos: macos
	@echo "Installing to /usr/local/lib..."
	sudo cp libtinysqlodbc.dylib /usr/local/lib/
	sudo chmod 755 /usr/local/lib/libtinysqlodbc.dylib
	@echo ""
	@echo "Add to ~/.odbcinst.ini:"
	@echo ""
	@echo "[tinySQL]"
	@echo "Description = TinySQL ODBC Driver"
	@echo "Driver = /usr/local/lib/libtinysqlodbc.dylib"
	@echo "Setup = /usr/local/lib/libtinysqlodbc.dylib"
	@echo "FileUsage = 1"

# Quick test using isql (if available)
test-isql:
	@if command -v isql >/dev/null 2>&1; then \
		echo "Testing connection with isql..."; \
		echo "SELECT 'Hello from tinySQL!' as message;" | isql -b tinysql_mem; \
	else \
		echo "isql not found. Install unixODBC tools."; \
	fi

# Create sample database and show Power BI settings
powerbi-demo:
	@echo ""
	@echo "=========================================="
	@echo "Power BI Demo Setup"
	@echo "=========================================="
	@echo ""
	@echo "Creating sample database..."
	@rm -f /tmp/powerbi_demo.gob powerbi_demo.gob
	@chmod +x populate_sample_db.py
	@python3 populate_sample_db.py
	@echo ""
	@echo "=========================================="
	@echo "Power BI Connection Instructions"
	@echo "=========================================="
	@echo ""
	@echo "For Windows:"
	@echo "  1. Build driver: make windows"
	@echo "  2. Run PowerShell as Admin: ./powerbi_setup.ps1"
	@echo "  3. Connection string:"
	@echo "     DRIVER={tinySQL};Database=file:C:\\path\\to\\powerbi_demo.gob"
	@echo ""
	@echo "For macOS/Linux:"
	@echo "  Current database: $$(pwd)/powerbi_demo.gob"
	@if [ "$$(uname)" = "Darwin" ]; then \
		echo "  Connection string:"; \
		echo "     DSN=tinysql_file;Database=file:$$(pwd)/powerbi_demo.gob"; \
	else \
		echo "  Connection string:"; \
		echo "     DSN=tinysql_file;Database=file:$$(pwd)/powerbi_demo.gob"; \
	fi
	@echo ""
	@echo "In Power BI Desktop:"
	@echo "  1. Get Data → ODBC"
	@echo "  2. Advanced options"
	@echo "  3. Enter connection string above"
	@echo "  4. Select tables: sales, customers, products"
	@echo "  5. Load or Transform Data"
	@echo ""
	@echo "Sample Visualizations:"
	@echo "  • Sales by Region (Bar Chart)"
	@echo "  • Sales by Category (Pie Chart)"
	@echo "  • Sales Trend by Date (Line Chart)"
	@echo "  • Product Stock Levels (Table)"
	@echo ""

# Create a complete package for Windows users
powerbi-package: windows
	@echo "Creating Power BI package..."
	@mkdir -p powerbi_package
	@cp tinysqlodbc.dll powerbi_package/ 2>/dev/null || echo "Windows DLL not found - run 'make windows' first"
	@cp tinysqlodbc.h powerbi_package/ 2>/dev/null || true
	@cp powerbi_setup.ps1 powerbi_package/
	@cp create_sample_db.ps1 powerbi_package/
	@cp populate_sample_db.py powerbi_package/
	@cp README.md powerbi_package/
	@echo "# TinySQL ODBC Driver for Power BI" > powerbi_package/INSTALL.txt
	@echo "" >> powerbi_package/INSTALL.txt
	@echo "Quick Start:" >> powerbi_package/INSTALL.txt
	@echo "1. Right-click PowerShell, Run as Administrator" >> powerbi_package/INSTALL.txt
	@echo "2. cd to this directory" >> powerbi_package/INSTALL.txt
	@echo "3. Run: .\\powerbi_setup.ps1" >> powerbi_package/INSTALL.txt
	@echo "4. Run: python populate_sample_db.py (optional, for demo data)" >> powerbi_package/INSTALL.txt
	@echo "5. Open Power BI Desktop" >> powerbi_package/INSTALL.txt
	@echo "6. Get Data → ODBC" >> powerbi_package/INSTALL.txt
	@echo "7. Connection string: DRIVER={tinySQL};Database=file:C:\\path\\to\\your.gob" >> powerbi_package/INSTALL.txt
	@echo "" >> powerbi_package/INSTALL.txt
	@echo "See README.md for complete documentation" >> powerbi_package/INSTALL.txt
	@if command -v zip >/dev/null 2>&1; then \
		cd powerbi_package && zip -r ../powerbi_package.zip * && cd ..; \
		echo "✓ Package created: powerbi_package.zip"; \
	else \
		echo "✓ Package created: powerbi_package/"; \
		echo "  (zip not available, folder created instead)"; \
	fi
	@echo ""
	@echo "Transfer powerbi_package.zip to Windows and extract it"
	@echo ""
