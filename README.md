# 🚦 Smart City Traffic Analytics

A full-stack big-data analytics platform with real-time traffic monitoring,
congestion detection, and an admin control panel.

## Stack

| Layer        | Technology                        |
|--------------|-----------------------------------|
| Backend API  | Python 3.11 · Flask 3             |
| Auth DB      | MySQL 8.0 (users / auth)          |
| Analytics DB | MongoDB 7 (traffic records)       |
| Streaming    | Apache Kafka 7.5 + Zookeeper      |
| Frontend     | Vanilla JS + Chart.js             |
| Container    | Docker Compose                    |

---

## Quick Start (VS Code + Docker Desktop)

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- VS Code with the **Docker** extension (optional but helpful)

### 1 · Clone / unzip the project

```
cd Smart-City-Traffic-Analytics
```

### 2 · Start everything

```bash
docker compose up --build
```

First build takes ~2 min (downloading images + pip install).
Subsequent starts take ~20 s.

### 3 · Open the app

| URL                              | Description            |
|----------------------------------|------------------------|
| http://localhost:5000            | Login page             |
| http://localhost:5000/dashboard.html | User dashboard    |
| http://localhost:5000/admin-dashboard.html | Admin panel |

### 4 · Create accounts

**Regular user** → click *Create new account* on the login page.

**Admin user** → click *Admin registration*, use secret key **`ADMIN123`**.

### 5 · Add traffic data

Once logged in, use the **⚡ Add 20 Records** button on the dashboard,
or wait ~15 s for the simulator container to auto-populate data.

---

## Services & Ports

| Service    | Container           | Host Port |
|------------|---------------------|-----------|
| Flask API  | traffic_flask       | 5000      |
| MongoDB    | traffic_mongodb     | 27017     |
| MySQL      | traffic_mysql       | 3306      |
| Kafka      | traffic_kafka       | 9092      |
| Zookeeper  | traffic_zookeeper   | 2181      |
| Simulator  | traffic_simulator   | —         |

---

## Useful Commands

```bash
# Start (detached)
docker compose up -d --build

# View logs for the Flask API
docker compose logs -f flask

# View simulator output
docker compose logs -f simulator

# Stop everything
docker compose down

# Stop and DELETE all data (volumes)
docker compose down -v

# Rebuild only the Flask image (after code changes)
docker compose up --build flask
```

---

## Environment Variables

All config lives in `docker-compose.yml` → `flask.environment`.
Change values there; no `.env` file is required.

| Variable        | Default         | Description              |
|-----------------|-----------------|--------------------------|
| MONGO_URI       | mongodb://mongodb:27017/ | MongoDB connection |
| MYSQL_HOST      | mysql           | MySQL hostname           |
| MYSQL_PASSWORD  | rootpassword    | MySQL root password      |
| ADMIN_SECRET    | ADMIN123        | Admin registration key   |

---

## Architecture

```
Browser
  └─► Flask :5000 ──► MySQL :3306  (users / auth)
                 └──► MongoDB :27017 (traffic data)

Simulator ──► Flask API  (HTTP /api/simulate-data)
         └──► Kafka :29092 ──► (future Spark consumer)
```
