# run_pipeline.ps1 - Start all pipeline components in separate windows

Write-Host "Starting pipeline components..." -ForegroundColor Cyan

$root = $PWD.Path

Start-Process powershell -ArgumentList @(
    "-NoExit", "-Command",
    "cd '$root'; .\.venv-producer\Scripts\Activate.ps1; python producer\hn_producer.py"
)

Start-Process powershell -ArgumentList @(
    "-NoExit", "-Command",
    "cd '$root'; .\.venv-consumer\Scripts\Activate.ps1; python consumer\spark_consumer.py"
)

Write-Host "Waiting 60s for first batch before running dbt..." -ForegroundColor Yellow
Start-Sleep -Seconds 60

& .\.venv-dbt\Scripts\python.exe -m dbt run --profiles-dir dbt --project-dir dbt

Start-Process powershell -ArgumentList @(
    "-NoExit", "-Command",
    "cd '$root'; .\.venv-dashboard\Scripts\Activate.ps1; streamlit run dashboard\app.py"
)

Write-Host ""
Write-Host "All components started!" -ForegroundColor Green
Write-Host "Dashboard -> http://localhost:8501" -ForegroundColor Cyan
Write-Host "Airflow   -> http://localhost:8080" -ForegroundColor Cyan
Write-Host "Kafka UI  -> http://localhost:8090" -ForegroundColor Cyan
