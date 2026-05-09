# Power BI Build And Refresh Guide

The report pages are already generated in PBIR. There should be no manual visual placement required.

## Open The Report

Open this file from File Explorer:

`Housing Affordability Dashboard.pbip`

Avoid Power BI Desktop recent files if you previously opened a stale copy.

## Refresh The Data

From the project root:

```powershell
python "Source Data\scripts\download_uk_housing_affordability_data.py"
python "Source Data\scripts\prepare_housing_affordability_model_data.py"
python .build\validate_report.py
```

Then open the PBIP and run `Refresh` in Power BI Desktop.

## Expected Tables

Dimension tables:
- `Dim Date`
- `Dim Geography`

Annual fact tables:
- `Fact Purchase Local`
- `Fact Purchase Region`
- `Fact Rental Local`
- `Fact Rental Region`
- `Fact UK Purchase Country Region Median`

Monthly fact tables:
- `Fact HPI Monthly`
- `Fact Bank Rate Monthly`
- `Fact UK Market Monthly`
- `Fact Market Events Monthly`

## Expected Pages

- `Executive Summary`
- `House Prices, Rates And Market Cycles`
- `Local Authority Pressure Explorer`
- `Income Distribution And Rental Strain`

## If The Report Fails To Open

Run:

```powershell
python .build\validate_report.py
```

The validator checks visual JSON shape, TMDL indentation, path length, and `pages.json` active-page consistency.

If Desktop opens an old cached model, close Desktop and clear:

`Housing Affordability Dashboard.SemanticModel\.pbi\cache.abf`

## Design Notes

- All core visual values are measure-driven.
- Static text is limited to page titles, subtitles, and visual titles.
- Page 2 uses `Fact UK Market Monthly` so UK house-price, Bank Rate, forecast, and correlation visuals refresh from one clean monthly table.
- The selected market-period labels are generated in the Python prep script and can be adjusted there if the event windows need changing.
