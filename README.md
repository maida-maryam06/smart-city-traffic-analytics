# 🚦 Smart City Traffic Analytics

> Real-time traffic monitoring and congestion detection platform for smart cities — built with Flask, MongoDB, MySQL, and Apache Kafka, fully containerized with Docker.

[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/Flask-3.0-000000?logo=flask&logoColor=white)](https://flask.palletsprojects.com/)
[![MongoDB](https://img.shields.io/badge/MongoDB-7.0-47A248?logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A full-stack big data analytics system that simulates and analyzes live city traffic — vehicle GPS streams, speed tracking, congestion alerts, and an admin control panel — using a polyglot persistence architecture (MongoDB for high-volume traffic events, MySQL for relational user auth) connected through Kafka.

## ✨ Features

- 🗺️ **Live city map** — real-time vehicle positions rendered on an interactive grid
- 🚨 **Automatic congestion detection** — flags slow-traffic zones by road and severity
- 📊 **Analytics dashboard** — Chart.js visualizations for vehicle distribution & speed heatmaps
- 🔐 **Role-based auth** — separate user and admin panels with bcrypt password hashing
- ⚡ **Data simulator** — generates realistic rush-hour/night traffic patterns across 5 roads
- 🐳 **One-command setup** — `docker compose up --build` and you're running

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
