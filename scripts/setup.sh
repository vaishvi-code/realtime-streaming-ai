#!/usr/bin/env bash
# =============================================================================
# setup.sh — One-command environment setup for the Sentiment Pipeline
# =============================================================================

set -e

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}"
echo "  ██████╗ ██╗██████╗ ███████╗██╗     ██╗███╗   ██╗███████╗"
echo "  ██╔══██╗██║██╔══██╗██╔════╝██║     ██║████╗  ██║██╔════╝"
echo "  ██████╔╝██║██████╔╝█████╗  ██║     ██║██╔██╗ ██║█████╗  "
echo "  ██╔═══╝ ██║██╔═══╝ ██╔══╝  ██║     ██║██║╚██╗██║██╔══╝  "
echo "  ██║     ██║██║     ███████╗███████╗██║██║ ╚████║███████╗"
echo "  ╚═╝     ╚═╝╚═╝     ╚══════╝╚══════╝╚═╝╚═╝  ╚═══╝╚══════╝"
echo -e "${NC}"
echo -e "${CYAN}  Real-Time Sentiment Streaming Pipeline — Setup${NC}"
echo ""

# Check prerequisites
echo -e "${YELLOW}[1/5] Checking prerequisites...${NC}"

command -v docker >/dev/null 2>&1 || { echo -e "${RED}✗ Docker not found. Install from https://docs.docker.com/get-docker/${NC}"; exit 1; }
command -v docker-compose >/dev/null 2>&1 || command -v "docker compose" >/dev/null 2>&1 || { echo -e "${RED}✗ Docker Compose not found.${NC}"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo -e "${RED}✗ Python 3 not found.${NC}"; exit 1; }
command -v java >/dev/null 2>&1 || { echo -e "${YELLOW}⚠ Java not found — needed for Spark. Install Java 11+${NC}"; }

echo -e "${GREEN}✓ Prerequisites OK${NC}"

# Create data directories
echo -e "${YELLOW}[2/5] Creating data lake directories...${NC}"
mkdir -p data/bronze/posts data/silver/posts data/gold data/checkpoints
echo -e "${GREEN}✓ Data directories created${NC}"

# Create Python virtual environment
echo -e "${YELLOW}[3/5] Setting up Python virtual environments...${NC}"

python3 -m venv .venv-producer
source .venv-producer/bin/activate
pip install -q -r producer/requirements.txt
deactivate

python3 -m venv .venv-consumer
source .venv-consumer/bin/activate
pip install -q -r consumer/requirements.txt
python -c "import nltk; nltk.download('punkt', quiet=True); nltk.download('averaged_perceptron_tagger', quiet=True)"
deactivate

python3 -m venv .venv-dashboard
source .venv-dashboard/bin/activate
pip install -q -r dashboard/requirements.txt
deactivate

python3 -m venv .venv-dbt
source .venv-dbt/bin/activate
pip install -q -r dbt/requirements.txt
deactivate

echo -e "${GREEN}✓ Virtual environments ready${NC}"

# Pull Docker images
echo -e "${YELLOW}[4/5] Pulling Docker images (this may take a few minutes)...${NC}"
docker-compose pull
echo -e "${GREEN}✓ Docker images pulled${NC}"

# Final instructions
echo -e "${YELLOW}[5/5] Setup complete!${NC}"
echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  ✓ Setup complete! Run the pipeline:${NC}"
echo ""
echo -e "  ${CYAN}./scripts/run_pipeline.sh${NC}     # Start everything"
echo ""
echo -e "  Or manually:"
echo -e "  ${CYAN}docker-compose up -d${NC}          # Start Kafka + Airflow"
echo -e "  ${CYAN}source .venv-producer/bin/activate && python producer/hn_producer.py${NC}"
echo -e "  ${CYAN}source .venv-consumer/bin/activate && python consumer/spark_consumer.py${NC}"
echo -e "  ${CYAN}source .venv-dbt/bin/activate && cd dbt && dbt run${NC}"
echo -e "  ${CYAN}source .venv-dashboard/bin/activate && streamlit run dashboard/app.py${NC}"
echo ""
echo -e "  Services:"
echo -e "  📊 Dashboard  → ${CYAN}http://localhost:8501${NC}"
echo -e "  🌀 Airflow    → ${CYAN}http://localhost:8080${NC}  (admin/admin)"
echo -e "  📨 Kafka UI   → ${CYAN}http://localhost:8090${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
