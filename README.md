# dlt Workshop

Data pipelines built with [dlt](https://dlthub.com/) (data load tool).

## Pipelines

- **Open Library** (`open_library_pipeline.py`) — Search the Open Library API for Harry Potter books
- **NYC Taxi** (`taxi_pipeline.py`) — Ingest NYC taxi trip data via paginated REST API

## Usage

```bash
uv run open_library_pipeline.py
uv run taxi_pipeline.py
```
