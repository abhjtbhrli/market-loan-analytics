# Market Loan Analytics Platform

Streamlit app for analysing SDL borrowings, interest rate trends, and reissuance scenarios.

## App Entry Point

- `streamlit_app.py`

## Main Modules

- `Assam`
- `State-wise comparison`
- `Reissuances`
- `Interest Rate Movements`

## Data Files Required

These files should be present in the repo for the app to run:

- `data/processed/borrowings-clean.csv`
- `data/processed/SDL.csv`
- `data/processed/Tenor Average (1).csv`
- `data/processed/gsec/*.csv`

The app rebuilds its DuckDB tables from these source files.

## Run Locally

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Deploy to Streamlit Cloud

1. Push this project to a GitHub repository.
2. In Streamlit Cloud, create a new app from that repository.
3. Set the main file path to:

```text
streamlit_app.py
```

## Notes

- Do not commit `.venv/`
- The local DuckDB file under `db/` is not required for deployment
- Streamlit Cloud will install dependencies from `requirements.txt`
