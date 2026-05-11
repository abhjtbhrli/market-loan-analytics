# Market Loan Analytics Platform

Streamlit app for SDL borrowing analytics.

## Entry Point

`streamlit_app.py`

## Pages

- `Assam`
- `State-wise comparison`
- `Reissuances`
- `Interest Rate Movements`

## Required Data

- `data/processed/borrowings-clean.csv`
- `data/processed/SDL.csv`
- `data/processed/Tenor Average (1).csv`
- `data/processed/gsec/*.csv`

The app rebuilds DuckDB tables from these files.

## Local Run

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Streamlit Cloud

Main file path:

`streamlit_app.py`
