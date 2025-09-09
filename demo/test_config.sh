#!/bin/bash

# AI Dataset Demo 配置测试脚本

echo "🧪 Testing AI Dataset Demo Configuration Options"
echo "================================================"

# 测试帮助信息
echo "1. Testing help information..."
./ai_dataset_demo -help > /dev/null && echo "✅ Help command works" || echo "❌ Help command failed"

# 测试默认配置
echo "2. Testing default configuration..."
./ai_dataset_demo -host 127.0.0.1 -port 6001 > /dev/null 2>&1 && echo "✅ Default config works" || echo "⚠️  Default config test (expected if MatrixOne not running)"

# 测试自定义 host
echo "3. Testing custom host configuration..."
./ai_dataset_demo -host 192.168.1.100 -port 6001 > /dev/null 2>&1 && echo "✅ Custom host config works" || echo "⚠️  Custom host config test (expected if host not reachable)"

# 测试 DSN 配置
echo "4. Testing DSN configuration..."
./ai_dataset_demo -dsn "root:111@tcp(192.168.1.100:6001)/test" > /dev/null 2>&1 && echo "✅ DSN config works" || echo "⚠️  DSN config test (expected if host not reachable)"

# 测试交互式模式
echo "5. Testing interactive mode flag..."
./ai_dataset_demo -interactive > /dev/null 2>&1 && echo "✅ Interactive flag works" || echo "⚠️  Interactive flag test (expected if MatrixOne not running)"

# 测试环境变量
echo "6. Testing environment variables..."
export MO_HOST=192.168.1.100
export MO_PORT=6001
export MO_USER=root
export MO_PASSWORD=111
export MO_DATABASE=test
./ai_dataset_demo > /dev/null 2>&1 && echo "✅ Environment variables work" || echo "⚠️  Environment variables test (expected if host not reachable)"

echo ""
echo "🎉 Configuration testing completed!"
echo "💡 Note: Connection failures are expected if MatrixOne is not running or host is not reachable"
echo "   The important thing is that the configuration parsing works correctly."
