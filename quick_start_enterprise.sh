#!/bin/bash

# Enterprise Data Lineage Platform - Quick Start Script
# =====================================================

set -e

echo "🔄 Enterprise Data Lineage Platform - Quick Start"
echo "================================================="

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is required but not installed"
    exit 1
fi

print_status "Python 3 found: $(python3 --version)"

# Check if pip is installed
if ! command -v pip3 &> /dev/null; then
    print_error "pip3 is required but not installed"
    exit 1
fi

# Install required dependencies
print_status "Installing enterprise dependencies..."

pip3 install -q pandas pyyaml click streamlit networkx plotly > /dev/null 2>&1

if [ $? -eq 0 ]; then
    print_success "Dependencies installed successfully"
else
    print_error "Failed to install dependencies"
    exit 1
fi

# Verify directory structure
print_status "Verifying directory structure..."

required_dirs=("data/raw" "data/processed" "lineage/events" "tools" "app" "pipelines")

for dir in "${required_dirs[@]}"; do
    if [ ! -d "$dir" ]; then
        print_warning "Creating missing directory: $dir"
        mkdir -p "$dir"
    fi
done

# Check if sample data exists
if [ ! -f "data/raw/customers.csv" ] || [ ! -f "data/raw/orders.csv" ]; then
    print_warning "Sample data not found. You may need to add sample data files."
fi

# Run pipelines to generate lineage data
print_status "Running ETL pipelines to generate lineage data..."

# Check if pipelines exist
if [ ! -f "pipelines/etl_orders.py" ]; then
    print_error "ETL pipeline files not found"
    exit 1
fi

# Run orders pipeline
print_status "Executing orders pipeline..."
python3 run_pipeline.py --name etl_orders > /dev/null 2>&1

if [ $? -eq 0 ]; then
    print_success "Orders pipeline completed"
else
    print_warning "Orders pipeline may have issues - check logs"
fi

# Run customers pipeline  
print_status "Executing customers pipeline..."
python3 run_pipeline.py --name etl_customers_daily > /dev/null 2>&1

if [ $? -eq 0 ]; then
    print_success "Customers pipeline completed"
else
    print_warning "Customers pipeline may have issues - check logs"
fi

# Validate lineage data
print_status "Validating lineage data..."

if [ -f "tools/impact_analysis.py" ]; then
    python3 tools/impact_analysis.py validate > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        print_success "Lineage validation passed"
    else
        print_warning "Lineage validation found issues"
    fi
else
    print_warning "Impact analysis tool not found"
fi

# Check if enhanced Streamlit app exists
if [ -f "app/enhanced_streamlit_app.py" ]; then
    APP_FILE="app/enhanced_streamlit_app.py"
    print_success "Enhanced Streamlit app found"
elif [ -f "app/streamlit_app.py" ]; then
    APP_FILE="app/streamlit_app.py" 
    print_success "Standard Streamlit app found"
else
    print_error "No Streamlit app found"
    exit 1
fi

# Display summary
echo ""
echo "🎉 Enterprise Data Lineage Platform Setup Complete!"
echo "=================================================="
echo ""
echo "📊 Available Features:"
echo "  • Interactive lineage visualization"
echo "  • Field-level impact analysis"  
echo "  • Multi-dataset support"
echo "  • Enterprise validation tools"
echo "  • Export capabilities"
echo ""
echo "🚀 Quick Commands:"
echo ""
echo "  # Launch interactive web interface:"
echo "  streamlit run ${APP_FILE}"
echo ""
echo "  # Run impact analysis (if available):"
echo "  python3 tools/impact_analysis.py trace net_amount"
echo "  python3 tools/impact_analysis.py validate"
echo ""  
echo "  # Re-run pipelines:"
echo "  python3 run_pipeline.py --name etl_orders"
echo "  python3 run_pipeline.py --name etl_customers_daily"
echo ""
echo "📚 Documentation:"
echo "  • README.md - Basic usage"
echo "  • README_ENTERPRISE.md - Advanced features"
echo "  • docs/usage.md - Detailed user guide"
echo ""

# Optional: Auto-launch Streamlit app
read -p "🌐 Would you like to launch the web interface now? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_status "Launching Streamlit application..."
    echo "  Opening browser at: http://localhost:8501"
    echo "  Press Ctrl+C to stop the server"
    echo ""
    streamlit run "${APP_FILE}"
else
    echo ""
    print_status "Setup complete! Run 'streamlit run ${APP_FILE}' when ready to explore."
fi