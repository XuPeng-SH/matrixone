#!/bin/bash

# AI Dataset Demo Script for MatrixOne
# Demonstrates Git for Data capabilities with AI data pipeline

set -e

echo "🚀 MatrixOne AI Dataset Demo"
echo "=============================="

# Check if MatrixOne is running
echo "🔍 Checking MatrixOne connection..."
if ! nc -z 127.0.0.1 6001; then
    echo "❌ MatrixOne is not running on port 6001"
    echo "Please start MatrixOne first:"
    echo "  ./mo-service -cfg etc/launch/launch.toml"
    exit 1
fi

echo "✅ MatrixOne is running"

# Set database connection string
export MO_DSN="root:111@tcp(127.0.0.1:6001)/test"

# Create test database if it doesn't exist
echo "📊 Setting up test database..."
mysql -h 127.0.0.1 -P 6001 -u root -p111 -e "CREATE DATABASE IF NOT EXISTS test;" 2>/dev/null || {
    echo "⚠️  Could not create database, continuing with existing database..."
}

# Build and run the demo
echo "🔨 Building demo application..."
go mod tidy
go build -o ai_dataset_demo .

echo "🎬 Running AI Dataset Demo..."
echo "This demo will show:"
echo "  • Git for Data: Time Travel queries"
echo "  • AI Data Pipeline: Automated and human annotations"
echo "  • Vector Search: Similarity-based retrieval"
echo "  • Version Control: Metadata tracking"
echo ""

./ai_dataset_demo

echo ""
echo "🎉 Demo completed!"
echo "💡 You can now explore the data using SQL queries:"
echo "   mysql -h 127.0.0.1 -P 6001 -u root -p111 test"
echo ""
echo "📝 Example queries:"
echo "   SELECT * FROM ai_dataset LIMIT 10;"
echo "   SELECT label, COUNT(*) FROM ai_dataset GROUP BY label;"
echo "   SELECT * FROM ai_dataset WHERE JSON_EXTRACT(metadata, '$.annotator') = 'AI_model_v1';"
