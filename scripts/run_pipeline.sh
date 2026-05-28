#!/usr/bin/env bash
# run_pipeline.sh — Start all pipeline components in tmux panes (or background)

set -e

GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}Starting Real-Time Sentiment Pipeline...${NC}"

# 1. Start Docker services
echo -e "${GREEN}[1] Starting Kafka + Zookeeper + Airflow...${NC}"
docker-compose up -d
sleep 10   # wait for Kafka to be ready

# 2. Start producer in background
echo -e "${GREEN}[2] Starting HN Producer...${NC}"
source .venv-producer/bin/activate
nohup python producer/hn_producer.py > logs/producer.log 2>&1 &
PRODUCER_PID=$!
deactivate
echo "  Producer PID: $PRODUCER_PID"

# 3. Start Spark consumer in background
echo -e "${GREEN}[3] Starting Spark Consumer...${NC}"
source .venv-consumer/bin/activate
nohup python consumer/spark_consumer.py > logs/consumer.log 2>&1 &
CONSUMER_PID=$!
deactivate
echo "  Consumer PID: $CONSUMER_PID"

# 4. Wait for Silver data then run dbt
echo -e "${GREEN}[4] Waiting 60s for first batch then running dbt...${NC}"
sleep 60
source .venv-dbt/bin/activate
cd dbt && dbt run --profiles-dir . && cd ..
deactivate

# 5. Launch dashboard
echo -e "${GREEN}[5] Launching Streamlit Dashboard...${NC}"
mkdir -p logs
source .venv-dashboard/bin/activate
streamlit run dashboard/app.py --server.port 8501 &
deactivate

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  Pipeline running!${NC}"
echo -e "  📊 Dashboard  → ${CYAN}http://localhost:8501${NC}"
echo -e "  🌀 Airflow    → ${CYAN}http://localhost:8080${NC}"
echo -e "  📨 Kafka UI   → ${CYAN}http://localhost:8090${NC}"
echo -e "  📄 Logs       → ./logs/"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
