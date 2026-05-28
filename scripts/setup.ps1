# setup.ps1 - Windows PowerShell setup for Real-Time Sentiment Pipeline
# Usage:
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#   .\scripts\setup.ps1

Write-Host ""
Write-Host "Real-Time Sentiment Streaming Pipeline - Windows Setup" -ForegroundColor Cyan
Write-Host ""

# 1. Create data directories
Write-Host "[1/4] Creating data lake directories..." -ForegroundColor Yellow
$dirs = @("data\bronze\posts", "data\silver\posts", "data\gold", "data\checkpoints", "logs")
foreach ($d in $dirs) {
    New-Item -ItemType Directory -Force -Path $d | Out-Null
}
Write-Host "OK" -ForegroundColor Green

# 2. Producer venv
Write-Host "[2/4] Setting up Python virtual environments..." -ForegroundColor Yellow

Write-Host "  Creating producer venv..."
python -m venv .venv-producer
& .\.venv-producer\Scripts\pip.exe install -q -r producer\requirements.txt

Write-Host "  Creating consumer venv..."
python -m venv .venv-consumer
& .\.venv-consumer\Scripts\pip.exe install -q -r consumer\requirements.txt

$nltkScript = "import nltk; nltk.download('punkt'); nltk.download('averaged_perceptron_tagger')"
& .\.venv-consumer\Scripts\python.exe -c $nltkScript

Write-Host "  Creating dbt venv..."
python -m venv .venv-dbt
& .\.venv-dbt\Scripts\pip.exe install -q -r dbt\requirements.txt

Write-Host "  Creating dashboard venv..."
python -m venv .venv-dashboard
& .\.venv-dashboard\Scripts\pip.exe install -q -r dashboard\requirements.txt

Write-Host "OK" -ForegroundColor Green

# 3. Docker
Write-Host "[3/4] Starting Docker services (Kafka + Airflow)..." -ForegroundColor Yellow
docker compose up -d
Write-Host "OK" -ForegroundColor Green

# 4. Done
Write-Host ""
Write-Host "[4/4] Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Now open 4 separate PowerShell terminals and run:" -ForegroundColor Yellow
Write-Host ""
Write-Host "Terminal 1 - Producer:" -ForegroundColor Cyan
Write-Host "  .\.venv-producer\Scripts\Activate.ps1"
Write-Host "  python producer\hn_producer.py"
Write-Host ""
Write-Host "Terminal 2 - Spark Consumer:" -ForegroundColor Cyan
Write-Host "  .\.venv-consumer\Scripts\Activate.ps1"
Write-Host "  python consumer\spark_consumer.py"
Write-Host ""
Write-Host "Terminal 3 - dbt (run after ~1 min of data flowing):" -ForegroundColor Cyan
Write-Host "  .\.venv-dbt\Scripts\Activate.ps1"
Write-Host "  cd dbt"
Write-Host "  dbt run --profiles-dir ."
Write-Host ""
Write-Host "Terminal 4 - Dashboard:" -ForegroundColor Cyan
Write-Host "  .\.venv-dashboard\Scripts\Activate.ps1"
Write-Host "  streamlit run dashboard\app.py"
Write-Host ""
Write-Host "Dashboard  -> http://localhost:8501" -ForegroundColor Green
Write-Host "Airflow    -> http://localhost:8080  (admin / admin)" -ForegroundColor Green
Write-Host "Kafka UI   -> http://localhost:8090" -ForegroundColor Green
