# AI Dataset Demo Makefile

.PHONY: help build run interactive clean test sql

# 默认目标
help:
	@echo "AI Dataset Demo - Available Commands:"
	@echo "======================================"
	@echo "make build        - 构建演示程序"
	@echo "make run          - 运行完整演示"
	@echo "make run-host     - 运行演示（指定 host）"
	@echo "make interactive  - 运行交互式演示"
	@echo "make interactive-host - 运行交互式演示（指定 host）"
	@echo "make sql          - 执行 SQL 演示脚本"
	@echo "make test         - 运行测试"
	@echo "make clean        - 清理构建文件"
	@echo "make help         - 显示此帮助信息"
	@echo ""
	@echo "配置选项:"
	@echo "  HOST=192.168.1.100 PORT=6001 USER=root PASSWORD=111 DATABASE=test"
	@echo ""
	@echo "环境变量:"
	@echo "  MO_HOST, MO_PORT, MO_USER, MO_PASSWORD, MO_DATABASE"
	@echo ""
	@echo "命令行参数:"
	@echo "  -host, -port, -user, -password, -database, -dsn, -interactive"

# 构建演示程序
build:
	@echo "🔨 Building AI Dataset Demo..."
	go mod tidy
	go build -o ai_dataset_demo .
	# Interactive demo is now built into the main binary
	@echo "✅ Build completed!"

# 运行完整演示
run: build
	@echo "🎬 Running AI Dataset Demo..."
	@if ! nc -z 127.0.0.1 6001; then \
		echo "❌ MatrixOne is not running on port 6001"; \
		echo "Please start MatrixOne first:"; \
		echo "  ./mo-service -cfg etc/launch/launch.toml"; \
		exit 1; \
	fi
	@echo "✅ MatrixOne is running"
	@./ai_dataset_demo

# 运行演示（指定 host）
run-host: build
	@echo "🎬 Running AI Dataset Demo with custom host..."
	@./ai_dataset_demo -host $(HOST) -port $(PORT) -user $(USER) -password $(PASSWORD) -database $(DATABASE)

# 运行交互式演示
interactive: build
	@echo "🎮 Starting Interactive Demo..."
	@if ! nc -z 127.0.0.1 6001; then \
		echo "❌ MatrixOne is not running on port 6001"; \
		echo "Please start MatrixOne first:"; \
		echo "  ./mo-service -cfg etc/launch/launch.toml"; \
		exit 1; \
	fi
	@echo "✅ MatrixOne is running"
	@./ai_dataset_demo -interactive

# 运行交互式演示（指定 host）
interactive-host: build
	@echo "🎮 Starting Interactive Demo with custom host..."
	@./ai_dataset_demo -interactive -host $(HOST) -port $(PORT) -user $(USER) -password $(PASSWORD) -database $(DATABASE)

# 执行 SQL 演示脚本
sql:
	@echo "📊 Executing SQL Demo Script..."
	@if ! nc -z 127.0.0.1 6001; then \
		echo "❌ MatrixOne is not running on port 6001"; \
		echo "Please start MatrixOne first:"; \
		echo "  ./mo-service -cfg etc/launch/launch.toml"; \
		exit 1; \
	fi
	@echo "✅ MatrixOne is running"
	@mysql -h 127.0.0.1 -P 6001 -u root -p111 test < demo_queries.sql

# 运行测试
test:
	@echo "🧪 Running tests..."
	go test -v ./...

# 清理构建文件
clean:
	@echo "🧹 Cleaning build files..."
	rm -f ai_dataset_demo interactive_demo
	go clean
	@echo "✅ Clean completed!"

# 检查依赖
deps:
	@echo "📦 Checking dependencies..."
	go mod verify
	go mod tidy

# 格式化代码
fmt:
	@echo "🎨 Formatting code..."
	go fmt ./...

# 代码检查
lint:
	@echo "🔍 Running linter..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "⚠️  golangci-lint not installed, skipping linting"; \
	fi

# 安装依赖
install-deps:
	@echo "📥 Installing dependencies..."
	go mod download
	@echo "✅ Dependencies installed!"

# 快速启动（检查环境并运行演示）
quickstart: build
	@echo "🚀 Quick Start - AI Dataset Demo"
	@echo "================================="
	@if ! nc -z 127.0.0.1 6001; then \
		echo "❌ MatrixOne is not running on port 6001"; \
		echo "Please start MatrixOne first:"; \
		echo "  ./mo-service -cfg etc/launch/launch.toml"; \
		exit 1; \
	fi
	@echo "✅ MatrixOne is running"
	@echo "🎬 Starting demo in 3 seconds..."
	@sleep 3
	@export MO_DSN="root:111@tcp(127.0.0.1:6001)/test" && ./ai_dataset_demo

# 开发模式（自动重新构建和运行）
dev:
	@echo "🔄 Development mode - watching for changes..."
	@if command -v air >/dev/null 2>&1; then \
		air; \
	else \
		echo "⚠️  air not installed, falling back to manual build"; \
		make build && make run; \
	fi
