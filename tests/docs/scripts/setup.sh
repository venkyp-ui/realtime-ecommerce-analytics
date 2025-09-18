#!/bin/bash

# Setup script for Real-Time E-commerce Analytics Pipeline

echo "🚀 Setting up Real-Time E-commerce Analytics Pipeline..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  Please edit .env file with your configuration"
fi

# Start infrastructure services
echo "🐳 Starting infrastructure services..."
docker-compose up -d

echo "✅ Setup complete!"
echo "🔗 Access points:"
echo "   - Kafka: localhost:9092"
echo "   - PostgreSQL: localhost:5432"  
echo "   - Redis: localhost:6379"
