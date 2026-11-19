
#!/usr/bin/env bash

set -u
cd "$(dirname "$0")" || exit 1

echo
echo "  ============================================================== "
echo "   TESSERA - Geo-Agentic Real-Time Spatial Stream Analytics"
echo "   Data Source > Kafka > Stream Processing > Storage > Dashboard"
echo "  ============================================================== "
echo

# Check Docker installation
if ! command -v docker >/dev/null 2>&1; then
    echo "[X] Docker is not installed."
    echo "    Install Docker Desktop: https://www.docker.com/products/docker-desktop/"
    exit 1
fi

# Check Docker engine
if ! docker info >/dev/null 2>&1; then
    echo "[X] Docker is installed but not running."
    echo "    Start Docker Desktop or your Docker service, then run start.sh again."
    exit 1
fi

echo "[1/3] Building the TESSERA image (first run downloads ~1.5 GB, be patient)..."

docker compose build init
if [ $? -ne 0 ]; then
    echo "[X] Image build failed - see the messages above."
    exit 1
fi

echo "      Starting containers..."

docker compose up -d
if [ $? -ne 0 ]; then
    echo "[X] docker compose failed - see the messages above."
    exit 1
fi

echo "[2/3] Waiting for the dashboard on http://localhost:8000 ..."

tries=0
ready=false

while [ "$tries" -lt 90 ]; do
    tries=$((tries + 1))

    if command -v curl >/dev/null 2>&1 &&
       curl -s -o /dev/null http://localhost:8000/; then
        ready=true
        break
    fi

    sleep 2
done

if [ "$ready" = false ]; then
    echo "[!] Dashboard not answering yet."
    echo "    Check: docker compose ps"
    echo "    Check logs using your logs.sh script or: docker compose logs"
else
    echo "[3/3] Pipeline is up."
fi

echo
echo "  Dashboard ........ http://localhost:8000"
echo "  Kafka UI ......... http://localhost:8085"
echo "  BLR GTFS-RT ...... http://localhost:8090/VehiclePositions.pb (India mode: simulated Bengaluru feed)"
echo "  TimescaleDB ...... localhost:5433 (db/user/password: tessera)"
echo
echo "  The first windows close about 70 s after start; the first incident is injected after ~45 s."
echo "  Useful: status.sh  logs.sh  demo-kafka.sh  demo-fault-tolerance.sh  scale.sh 3  stop.sh"
echo

# Open dashboard in the default browser
if command -v xdg-open >/dev/null 2>&1; then
    xdg-open http://localhost:8000 >/dev/null 2>&1 &
elif command -v open >/dev/null 2>&1; then
    open http://localhost:8000
else
    echo "Open http://localhost:8000 in your browser."
fi

exit 0
