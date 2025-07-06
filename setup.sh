#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show help
show_help() {
    echo "Twine Solana Geyser Plugin - Setup"
    echo ""
    echo "Usage: ./setup.sh [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start      Build plugin and start all services (DB, Monitoring)"
    echo "  stop       Stop all services"
    echo "  clean      Remove all containers and data (WARNING: destructive)"
    echo "  logs       Show logs for a specific service (db, prometheus, grafana, pgadmin)"
    echo "  all-logs   Show logs for all services"
    echo "  build      Build the Rust plugin"
    echo "  status     Show service status"
    echo "  init-db    Initialize or reinitialize database tables"
    echo "  init-grafana Restart Grafana to reload dashboard configuration"
    echo "  help       Show this help"
    echo ""
}

# Check if docker and docker-compose are installed
check_requirements() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed"
        exit 1
    fi
    
    if ! docker compose version &> /dev/null; then
        print_error "Docker Compose is not installed or not available as 'docker compose'"
        exit 1
    fi
    
    if ! command -v cargo &> /dev/null; then
        print_warn "Cargo is not installed - needed for 'build' command"
    fi
}

# Check for config.json
check_config() {
    if [ ! -f "config.json" ]; then
        print_warn "config.json not found. Copying from config.json.example."
        print_warn "Please edit config.json with your settings."
        cp config.json.example config.json
    fi
}

# Create network if it doesn't exist
create_network() {
    if ! docker network ls | grep -q "twine-network"; then
        print_info "Creating docker network 'twine-network'..."
        docker network create twine-network
    fi
}

# Start all services
start_all() {
    check_config
    create_network
    
    print_info "Building Twine Geyser Plugin..."
    build_plugin
    if [ $? -ne 0 ]; then
        print_error "Build failed. Aborting start."
        exit 1
    fi
    
    print_info "Starting all services..."
    docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d --remove-orphans
    
    if [ $? -ne 0 ]; then
        print_error "Failed to start services"
        exit 1
    fi
    
    print_info "Waiting for services to be ready..."
    sleep 15
    
    # Wait for database to be fully ready
    print_info "Waiting for database to be ready..."
    for i in {1..30}; do
        if docker exec twine-timescaledb pg_isready -U geyser_writer -d twine_solana_db &>/dev/null; then
            print_info "Database is ready!"
            break
        fi
        if [ $i -eq 30 ]; then
            print_error "Database failed to start properly"
            exit 1
        fi
        sleep 1
    done
    
    # Verify database tables were created
    print_info "Verifying database tables..."
    TABLES=$(docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';")
    TABLES=$(echo $TABLES | tr -d ' ')
    
    if [ "$TABLES" -lt "5" ]; then
        print_warn "Database tables not found. Running initialization script..."
        docker exec -i twine-timescaledb psql -U geyser_writer -d twine_solana_db < schema/init.sql
        if [ $? -eq 0 ]; then
            print_info "Database initialized successfully!"
        else
            print_error "Failed to initialize database"
            exit 1
        fi
    else
        print_info "Database tables already exist (found $TABLES tables)"
    fi
    
    # Check if services are healthy
    print_info "Checking service health..."
    docker compose -f docker-compose.yml -f docker-compose.monitoring.yml ps
    
    echo ""
    print_info "Services are running! Access them at:"
    print_info "  Grafana Dashboard: http://localhost:3000 (username: admin, password: admin)"
    print_info "  Prometheus: http://localhost:9090"
    print_info "  pgAdmin: http://localhost:5050 (username: dev@twinelabs.xyz, password: Twine202%202%)"
    print_info "  Plugin Metrics: http://localhost:9091/metrics"
    echo ""
    print_info "The Grafana dashboard will automatically show all metrics once the plugin is running."
    print_info "Make sure to start your Solana validator with the plugin configuration."
}

# Stop all services
stop_all() {
    print_info "Stopping all services..."
    docker compose -f docker-compose.yml -f docker-compose.monitoring.yml down
    print_info "All services stopped"
}

# Clean everything
clean_all() {
    print_warn "This will DELETE all data and containers!"
    echo -n "Are you sure? (y/N): "
    read -r response
    
    if [[ "$response" =~ ^[Yy]$ ]]; then
        print_info "Cleaning up..."
        docker compose -f docker-compose.yml -f docker-compose.monitoring.yml down -v
        docker volume rm twine-solana-geyser_timescale_data 2>/dev/null || true
        docker volume rm twine-solana-geyser-timescale-data 2>/dev/null || true
        docker network rm twine-network 2>/dev/null || true
        print_info "Cleanup complete"
    else
        print_info "Cleanup cancelled"
    fi
}

# Show logs
show_logs() {
    case "$1" in
        db)
            docker compose logs -f timescaledb
            ;;
        prometheus)
            docker compose -f docker-compose.monitoring.yml logs -f prometheus
            ;;
        grafana)
            docker compose -f docker-compose.monitoring.yml logs -f grafana
            ;;
        pgadmin)
            docker compose -f docker-compose.monitoring.yml logs -f pgadmin
            ;;
        *)
            print_error "Unknown service: $1. Use 'db', 'prometheus', 'grafana', or 'pgadmin'."
            ;;
    esac
}

# Show all logs
show_all_logs() {
    print_info "Showing logs for all services..."
    docker compose -f docker-compose.yml -f docker-compose.monitoring.yml logs -f
}

# Build the plugin
build_plugin() {
    print_info "Building Twine Geyser Plugin..."
    cargo build --release
    
    if [ $? -eq 0 ]; then
        print_info "Build successful!"
        echo "Plugin location: target/release/libtwine_solana_geyser.so"
        return 0
    else
        print_error "Build failed"
        return 1
    fi
}

# Show status
show_status() {
    print_info "Service Status:"
    echo ""
    docker compose -f docker-compose.yml -f docker-compose.monitoring.yml ps
    
    # Check if plugin is built
    if [ -f "target/release/libtwine_solana_geyser.so" ]; then
        echo -e "  Plugin: ${GREEN}Built${NC}"
    else
        echo -e "  Plugin: ${YELLOW}Not built${NC}"
    fi
}

# Initialize database
init_database() {
    print_info "Checking if database container is running..."
    
    if ! docker ps | grep -q twine-timescaledb; then
        print_error "Database container is not running. Please run './setup.sh start' first."
        exit 1
    fi
    
    print_info "Waiting for database to be ready..."
    for i in {1..30}; do
        if docker exec twine-timescaledb pg_isready -U geyser_writer -d twine_solana_db &>/dev/null; then
            print_info "Database is ready!"
            break
        fi
        if [ $i -eq 30 ]; then
            print_error "Database is not responding"
            exit 1
        fi
        sleep 1
    done
    
    print_info "Running database initialization script..."
    docker exec -i twine-timescaledb psql -U geyser_writer -d twine_solana_db < schema/init.sql
    
    if [ $? -eq 0 ]; then
        print_info "Database initialized successfully!"
        
        # List created tables
        print_info "Created tables:"
        docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "\dt"
    else
        print_error "Failed to initialize database"
        exit 1
    fi
}

# Initialize/restart Grafana to reload dashboards
init_grafana() {
    print_info "Checking if Grafana container is running..."
    
    if ! docker ps | grep -q twine-grafana; then
        print_error "Grafana container is not running. Please run './setup.sh start' first."
        exit 1
    fi
    
    print_info "Restarting Grafana to reload dashboard configuration..."
    docker compose -f docker-compose.yml -f docker-compose.monitoring.yml restart grafana
    
    if [ $? -eq 0 ]; then
        print_info "Grafana restarted successfully!"
        
        # Wait for Grafana to be ready
        print_info "Waiting for Grafana to be ready..."
        for i in {1..30}; do
            if curl -s -o /dev/null -w "%{http_code}" http://localhost:3000/api/health | grep -q "200"; then
                print_info "Grafana is ready!"
                break
            fi
            if [ $i -eq 30 ]; then
                print_warn "Grafana may still be starting up. Please check manually."
            fi
            sleep 2
        done
        
        echo ""
        print_info "Grafana has been restarted with the latest dashboard configuration."
        print_info "Access Grafana at: http://localhost:3000 (username: admin, password: admin)"
        print_info "The dashboard should now include the Vote Participation & Stake Coverage section."
        
        # Refresh materialized view if database is running
        if docker ps | grep -q twine-timescaledb; then
            print_info "Refreshing vote participation stats..."
            docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "SELECT refresh_vote_participation_stats();" 2>/dev/null || true
        fi
    else
        print_error "Failed to restart Grafana"
        exit 1
    fi
}

# Main script
check_requirements

case "$1" in
    start)
        start_all
        ;;
    stop)
        stop_all
        ;;
    clean)
        clean_all
        ;;
    logs)
        if [ -z "$2" ]; then
            print_error "Please specify a service: db, prometheus, grafana, or pgadmin"
            exit 1
        fi
        show_logs "$2"
        ;;
    all-logs)
        show_all_logs
        ;;
    build)
        build_plugin
        ;;
    status)
        show_status
        ;;
    init-db)
        init_database
        ;;
    init-grafana)
        init_grafana
        ;;
    help|--help|-h|"")
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac