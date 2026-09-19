# Scalable News Aggregator

![System Architecture Diagram](image.png)

Welcome to the **Scalable News Aggregator** repository!

This system ingests news articles from thousands of publishers, collapses duplicates, groups independent coverage of the same real-world event into a single **story**, ranks stories by credibility and recency, and serves a de-duplicated feed.

---

## 📂 Repository Contents

This repository is organized into three primary sections:

1. 📄 **[DESCRIPTION.md](DESCRIPTION.md)** — Comprehensive technical description, pipeline stages, algorithms, and system design.
2. 🖼️ **`image.png`** — High-resolution System Architecture Banner & Processing Pipeline Diagram.
3. 📁 **[`code/`](code/)** — Complete executable Python application, algorithms, API server, tests, and Docker setup.

---

## ⚡ Quick Start

```bash
cd code

# Setup virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run offline demo
python main.py demo

# Serve HTTP REST API
python main.py serve
```

For complete details, please refer to **[DESCRIPTION.md](DESCRIPTION.md)**.
