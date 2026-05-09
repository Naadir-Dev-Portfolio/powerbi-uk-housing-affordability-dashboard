from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "Source Data" / "raw"
CURATED_DIR = ROOT / "Source Data" / "curated"
SUMMARY_PATH = CURATED_DIR / "build_summary.json"

WORKPLACE_PATH = RAW_DIR / "ons_house_price_to_workplace_earnings_ratio.xlsx"
RENTAL_PATH = RAW_DIR / "ons_private_rental_affordability.xlsx"
UK_PURCHASE_PATH = RAW_DIR / "ons_uk_purchase_affordability.xlsx"
HPI_MONTHLY_PATH = RAW_DIR / "uk_hpi_monthly_full_2026_02.csv"
BANK_RATE_MONTHLY_PATH = RAW_DIR / "boe_monthly_average_bank_rate.csv"
AWE_MONTHLY_PATH = RAW_DIR / "ons_awe_total_pay_kab9_lms.json"
UNEMPLOYMENT_MONTHLY_PATH = RAW_DIR / "ons_unemployment_rate_mgsx_lms.json"

MONTH_NAME_TO_NUMBER = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}

MARKET_EVENTS = [
    {
        "EventSort": 1,
        "EventLabel": "2001 dot-com slowdown",
        "EventShortLabel": "2001 dot-com",
        "StartDate": "2000-03-01",
        "EndDate": "2002-12-01",
        "EventDescription": "Early-2000s equity-market slump and slowdown period.",
    },
    {
        "EventSort": 2,
        "EventLabel": "2008 global financial crisis",
        "EventShortLabel": "2008 crisis",
        "StartDate": "2007-07-01",
        "EndDate": "2009-06-01",
        "EventDescription": "Credit crunch and housing-market retrenchment around the global financial crisis.",
    },
    {
        "EventSort": 3,
        "EventLabel": "2020 pandemic disruption",
        "EventShortLabel": "2020 pandemic",
        "StartDate": "2020-03-01",
        "EndDate": "2020-06-01",
        "EventDescription": "Initial pandemic shutdown and immediate housing-market disruption period.",
    },
    {
        "EventSort": 4,
        "EventLabel": "2022 mortgage repricing period",
        "EventShortLabel": "2022 repricing",
        "StartDate": "2022-09-01",
        "EndDate": "2023-03-01",
        "EventDescription": "Mortgage repricing and demand reset following the September 2022 mini-budget.",
    },
]


def clean_columns(columns: list[object]) -> list[str]:
    cleaned: list[str] = []
    for value in columns:
        if pd.isna(value):
            cleaned.append("")
            continue
        if isinstance(value, float) and value.is_integer():
            cleaned.append(str(int(value)))
            continue
        cleaned.append(str(value).replace("\n", " ").strip())
    return cleaned


def read_sheet(path: Path, sheet_name: str, header_row: int) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet_name, header=header_row)
    df.columns = clean_columns(df.columns.tolist())
    df = df.dropna(axis=1, how="all").dropna(how="all")
    return df


def parse_year_from_label(label: str) -> int | None:
    label = str(label).strip()
    if re.fullmatch(r"\d{4}", label):
        return int(label)
    match = re.search(r"(19|20)\d{2}$", label)
    if match:
        return int(match.group(0))
    fy_match = re.fullmatch(r"(\d{4})/(\d{2})", label)
    if fy_match:
        start_year = int(fy_match.group(1))
        end_suffix = int(fy_match.group(2))
        end_year = (start_year // 100) * 100 + end_suffix
        if end_year < start_year:
            end_year += 100
        return end_year
    return None


def melt_years(
    df: pd.DataFrame,
    id_columns: list[str],
    value_name: str,
    period_type: str,
    five_year_average_column: str | None = None,
) -> pd.DataFrame:
    year_columns: list[str] = []
    for column in df.columns:
        if column in id_columns:
            continue
        if column == five_year_average_column:
            continue
        if parse_year_from_label(column) is not None:
            year_columns.append(column)

    long_df = df.melt(
        id_vars=id_columns,
        value_vars=year_columns,
        var_name="PeriodLabel",
        value_name=value_name,
    )
    long_df["Year"] = long_df["PeriodLabel"].map(parse_year_from_label)
    long_df[value_name] = pd.to_numeric(long_df[value_name], errors="coerce")
    long_df = long_df.dropna(subset=["Year"])
    long_df["Year"] = long_df["Year"].astype("int64")

    if period_type == "purchase_sep":
        long_df["PeriodLabel"] = long_df["Year"].astype(str)
        long_df["PeriodEndDate"] = pd.to_datetime(
            long_df["Year"].astype(str) + "-09-30"
        )
    elif period_type == "fye_mar":
        long_df["PeriodLabel"] = long_df["PeriodLabel"].astype(str)
        long_df["PeriodEndDate"] = pd.to_datetime(
            long_df["Year"].astype(str) + "-03-31"
        )
    else:
        raise ValueError(f"Unsupported period_type: {period_type}")

    if five_year_average_column and five_year_average_column in df.columns:
        five_year_df = df[id_columns + [five_year_average_column]].copy()
        five_year_df.rename(
            columns={five_year_average_column: "FiveYearAverageAffordabilityRatio"},
            inplace=True,
        )
        five_year_df["FiveYearAverageAffordabilityRatio"] = pd.to_numeric(
            five_year_df["FiveYearAverageAffordabilityRatio"], errors="coerce"
        )
        long_df = long_df.merge(five_year_df, on=id_columns, how="left")

    return long_df


def event_label_for_date(date_value: pd.Timestamp) -> str | pd.NA:
    for event in MARKET_EVENTS:
        start = pd.Timestamp(event["StartDate"])
        end = pd.Timestamp(event["EndDate"])
        if start <= date_value <= end:
            return event["EventLabel"]
    return pd.NA


def market_period_for_date(date_value: pd.Timestamp) -> str:
    event_label = event_label_for_date(date_value)
    if pd.isna(event_label):
        return "Outside selected market stress periods"
    return str(event_label)


def fit_linear_monthly_forecast(
    history: pd.Series, periods: int = 12, lookback: int = 24
) -> list[tuple[pd.Timestamp, float, float | None, float | None]]:
    history = history.dropna().sort_index()
    if len(history) < 12:
        return []

    window = history.tail(min(lookback, len(history)))
    x = np.arange(len(window), dtype=float)
    y = window.to_numpy(dtype=float)
    slope, intercept = np.polyfit(x, y, 1)

    combined = history.copy()
    last_date = history.index.max()
    forecast_rows: list[tuple[pd.Timestamp, float, float | None, float | None]] = []

    for step in range(1, periods + 1):
        forecast_date = last_date + pd.DateOffset(months=step)
        projected = max(float(intercept + slope * (len(window) + step - 1)), 0.0)
        previous_month_date = forecast_date - pd.DateOffset(months=1)
        previous_year_date = forecast_date - pd.DateOffset(months=12)
        previous_month = combined.get(previous_month_date)
        previous_year = combined.get(previous_year_date)
        monthly_change = (
            (projected / previous_month) - 1
            if previous_month is not None and previous_month != 0
            else None
        )
        annual_change = (
            (projected / previous_year) - 1
            if previous_year is not None and previous_year != 0
            else None
        )
        combined.loc[forecast_date] = projected
        forecast_rows.append((forecast_date, projected, monthly_change, annual_change))

    return forecast_rows


def build_purchase_local_fact() -> pd.DataFrame:
    id_columns = [
        "Country/Region code",
        "Country/Region name",
        "Local authority code",
        "Local authority name",
    ]

    median_house = melt_years(
        read_sheet(WORKPLACE_PATH, "5a", header_row=1),
        id_columns,
        "MedianHousePrice",
        "purchase_sep",
    )
    median_earnings = melt_years(
        read_sheet(WORKPLACE_PATH, "5b", header_row=1),
        id_columns,
        "MedianWorkplaceEarnings",
        "purchase_sep",
    )
    median_ratio = melt_years(
        read_sheet(WORKPLACE_PATH, "5c", header_row=1),
        id_columns,
        "MedianAffordabilityRatio",
        "purchase_sep",
        five_year_average_column="5-Year Average",
    )
    lower_house = melt_years(
        read_sheet(WORKPLACE_PATH, "6a", header_row=1),
        id_columns,
        "LowerQuartileHousePrice",
        "purchase_sep",
    )
    lower_earnings = melt_years(
        read_sheet(WORKPLACE_PATH, "6b", header_row=1),
        id_columns,
        "LowerQuartileWorkplaceEarnings",
        "purchase_sep",
    )
    lower_ratio = melt_years(
        read_sheet(WORKPLACE_PATH, "6c", header_row=1),
        id_columns,
        "LowerQuartileAffordabilityRatio",
        "purchase_sep",
        five_year_average_column="5-Year Average",
    )

    join_keys = id_columns + ["Year", "PeriodLabel", "PeriodEndDate"]
    fact = median_house.merge(median_earnings, on=join_keys, how="outer")
    fact = fact.merge(median_ratio, on=join_keys, how="outer")
    fact = fact.merge(lower_house, on=join_keys, how="outer")
    fact = fact.merge(lower_earnings, on=join_keys, how="outer")
    fact = fact.merge(lower_ratio, on=join_keys, how="outer")

    fact.rename(
        columns={
            "Country/Region code": "RegionCode",
            "Country/Region name": "RegionName",
            "Local authority code": "GeographyCode",
            "Local authority name": "GeographyName",
            "FiveYearAverageAffordabilityRatio_x": "MedianFiveYearAverageAffordabilityRatio",
            "FiveYearAverageAffordabilityRatio_y": "LowerQuartileFiveYearAverageAffordabilityRatio",
        },
        inplace=True,
    )

    fact["GeographyLevel"] = "Local Authority"
    fact["AffordabilityThresholdPurchase"] = 5.0
    fact["AffordabilityBandPurchase"] = pd.cut(
        fact["MedianAffordabilityRatio"],
        bins=[-float("inf"), 5, 8, 12, float("inf")],
        labels=["Affordable", "Moderate Pressure", "High Pressure", "Severe Pressure"],
    ).astype("string")
    return fact.sort_values(["GeographyName", "Year"]).reset_index(drop=True)


def build_purchase_region_fact() -> pd.DataFrame:
    id_columns = ["Code", "Name"]

    median_house = melt_years(
        read_sheet(WORKPLACE_PATH, "1a", header_row=1),
        id_columns,
        "MedianHousePrice",
        "purchase_sep",
    )
    median_earnings = melt_years(
        read_sheet(WORKPLACE_PATH, "1b", header_row=1),
        id_columns,
        "MedianWorkplaceEarnings",
        "purchase_sep",
    )
    median_ratio = melt_years(
        read_sheet(WORKPLACE_PATH, "1c", header_row=1),
        id_columns,
        "MedianAffordabilityRatio",
        "purchase_sep",
        five_year_average_column="5-Year Average",
    )
    lower_house = melt_years(
        read_sheet(WORKPLACE_PATH, "2a", header_row=1),
        id_columns,
        "LowerQuartileHousePrice",
        "purchase_sep",
    )
    lower_earnings = melt_years(
        read_sheet(WORKPLACE_PATH, "2b", header_row=1),
        id_columns,
        "LowerQuartileWorkplaceEarnings",
        "purchase_sep",
    )
    lower_ratio = melt_years(
        read_sheet(WORKPLACE_PATH, "2c", header_row=1),
        id_columns,
        "LowerQuartileAffordabilityRatio",
        "purchase_sep",
        five_year_average_column="5-Year Average",
    )

    join_keys = id_columns + ["Year", "PeriodLabel", "PeriodEndDate"]
    fact = median_house.merge(median_earnings, on=join_keys, how="outer")
    fact = fact.merge(median_ratio, on=join_keys, how="outer")
    fact = fact.merge(lower_house, on=join_keys, how="outer")
    fact = fact.merge(lower_earnings, on=join_keys, how="outer")
    fact = fact.merge(lower_ratio, on=join_keys, how="outer")

    fact.rename(
        columns={
            "Code": "GeographyCode",
            "Name": "GeographyName",
            "FiveYearAverageAffordabilityRatio_x": "MedianFiveYearAverageAffordabilityRatio",
            "FiveYearAverageAffordabilityRatio_y": "LowerQuartileFiveYearAverageAffordabilityRatio",
        },
        inplace=True,
    )
    fact["GeographyLevel"] = "Country or Region"
    fact["AffordabilityThresholdPurchase"] = 5.0
    return fact.sort_values(["GeographyName", "Year"]).reset_index(drop=True)


def build_rental_region_fact() -> pd.DataFrame:
    id_columns = ["Country or region code", "Country or region name"]
    rent = melt_years(
        read_sheet(RENTAL_PATH, "Table 1", header_row=2),
        id_columns,
        "MeanMonthlyRent",
        "fye_mar",
    )
    income = melt_years(
        read_sheet(RENTAL_PATH, "Table 2", header_row=2),
        id_columns,
        "MedianPrivateRenterIncome",
        "fye_mar",
    )
    ratio = melt_years(
        read_sheet(RENTAL_PATH, "Table 3", header_row=2),
        id_columns,
        "RentalAffordabilityRatio",
        "fye_mar",
    )

    join_keys = id_columns + ["Year", "PeriodLabel", "PeriodEndDate"]
    fact = rent.merge(income, on=join_keys, how="outer").merge(
        ratio, on=join_keys, how="outer"
    )
    fact.rename(
        columns={
            "Country or region code": "GeographyCode",
            "Country or region name": "GeographyName",
        },
        inplace=True,
    )
    fact["GeographyLevel"] = "Country or Region"
    fact["AffordabilityThresholdRent"] = 0.3
    fact["RentalPressureBand"] = pd.cut(
        fact["RentalAffordabilityRatio"],
        bins=[-float("inf"), 0.3, 0.4, float("inf")],
        labels=["Below 30%", "30%-40%", "Above 40%"],
    ).astype("string")
    return fact.sort_values(["GeographyName", "Year"]).reset_index(drop=True)


def build_rental_local_fact() -> pd.DataFrame:
    df = read_sheet(RENTAL_PATH, "Table 4", header_row=2)
    id_columns = ["LA code", "LA name", "Country or region code", "Region name"]
    fact = melt_years(
        df,
        id_columns,
        "RentalAffordabilityRatio",
        "fye_mar",
    )
    fact.rename(
        columns={
            "LA code": "GeographyCode",
            "LA name": "GeographyName",
            "Country or region code": "RegionCode",
            "Region name": "RegionName",
        },
        inplace=True,
    )
    fact["GeographyLevel"] = "Local Authority"
    fact["AffordabilityThresholdRent"] = 0.3
    fact["RentalPressureBand"] = pd.cut(
        fact["RentalAffordabilityRatio"],
        bins=[-float("inf"), 0.3, 0.4, float("inf")],
        labels=["Below 30%", "30%-40%", "Above 40%"],
    ).astype("string")
    return fact.sort_values(["GeographyName", "Year"]).reset_index(drop=True)


def build_uk_purchase_country_region_median_fact() -> pd.DataFrame:
    price = read_sheet(UK_PURCHASE_PATH, "1", header_row=2)
    income = read_sheet(UK_PURCHASE_PATH, "2", header_row=2)
    ratio = read_sheet(UK_PURCHASE_PATH, "4", header_row=2)

    price = price[price["House price decile"] == "50th percentile"].copy()
    income = income[income["Income decile"] == "50th percentile"].copy()
    ratio = ratio[
        (ratio["Income decile"] == "50th percentile")
        & (ratio["House price decile"] == "50th percentile")
    ].copy()

    id_columns = ["Country/Region code", "Country/Region name"]
    price_long = melt_years(price, id_columns + ["House price decile"], "MedianHousePrice", "fye_mar")
    income_long = melt_years(income, id_columns + ["Income decile"], "MedianEquivalisedDisposableIncome", "fye_mar")
    ratio_long = melt_years(
        ratio,
        id_columns + ["Income decile", "House price decile"],
        "MedianPurchaseAffordabilityRatioUK",
        "fye_mar",
    )

    price_long = price_long.drop(columns=["House price decile"])
    income_long = income_long.drop(columns=["Income decile"])
    ratio_long = ratio_long.drop(columns=["Income decile", "House price decile"])

    join_keys = id_columns + ["Year", "PeriodLabel", "PeriodEndDate"]
    fact = price_long.merge(income_long, on=join_keys, how="outer").merge(
        ratio_long, on=join_keys, how="outer"
    )
    fact.rename(
        columns={
            "Country/Region code": "GeographyCode",
            "Country/Region name": "GeographyName",
        },
        inplace=True,
    )
    fact["GeographyLevel"] = "Country or Region"
    fact["AffordabilityThresholdPurchase"] = 5.0
    return fact.sort_values(["GeographyName", "Year"]).reset_index(drop=True)


def build_hpi_monthly_fact(dim_geography: pd.DataFrame) -> pd.DataFrame:
    hpi = pd.read_csv(HPI_MONTHLY_PATH)
    hpi["Date"] = pd.to_datetime(hpi["Date"], dayfirst=True)

    allowed_codes = set(dim_geography["GeographyCode"].dropna().astype(str))
    hpi = hpi[hpi["AreaCode"].astype(str).isin(allowed_codes)].copy()
    hpi.rename(
        columns={
            "AreaCode": "GeographyCode",
            "RegionName": "GeographyName",
            "IndexSA": "IndexSeasonallyAdjusted",
            "1m%Change": "MonthlyChangePct",
            "12m%Change": "AnnualChangePct",
        },
        inplace=True,
    )

    hpi = hpi.merge(
        dim_geography[["GeographyCode", "GeographyLevel"]],
        on="GeographyCode",
        how="left",
    )
    hpi["PeriodLabel"] = hpi["Date"].dt.strftime("%Y-%m")
    hpi["Year"] = hpi["Date"].dt.year.astype("int64")
    hpi["MonthNumber"] = hpi["Date"].dt.month.astype("int64")
    hpi["Scenario"] = "Actual"
    hpi["ScenarioSort"] = 1
    hpi["ForecastMethod"] = pd.NA
    hpi["MonthlyChangePct"] = pd.to_numeric(hpi["MonthlyChangePct"], errors="coerce") / 100.0
    hpi["AnnualChangePct"] = pd.to_numeric(hpi["AnnualChangePct"], errors="coerce") / 100.0
    hpi["DisplayPrice"] = pd.to_numeric(hpi["AveragePriceSA"], errors="coerce").fillna(
        pd.to_numeric(hpi["AveragePrice"], errors="coerce")
    )
    hpi["CrisisPeriod"] = hpi["Date"].apply(event_label_for_date).astype("string")

    actual = hpi[
        [
            "Date",
            "PeriodLabel",
            "Year",
            "MonthNumber",
            "GeographyCode",
            "GeographyName",
            "GeographyLevel",
            "Scenario",
            "ScenarioSort",
            "ForecastMethod",
            "AveragePrice",
            "AveragePriceSA",
            "DisplayPrice",
            "Index",
            "IndexSeasonallyAdjusted",
            "MonthlyChangePct",
            "AnnualChangePct",
            "SalesVolume",
            "CrisisPeriod",
        ]
    ].copy()

    forecastable_levels = {"Country", "Region", "England and Wales"}
    forecastable = dim_geography[
        dim_geography["GeographyLevel"].isin(forecastable_levels)
    ][["GeographyCode", "GeographyName", "GeographyLevel"]].drop_duplicates()
    forecast_rows: list[dict[str, object]] = []

    for row in forecastable.itertuples(index=False):
        history = actual[
            (actual["GeographyCode"] == row.GeographyCode)
            & (actual["Scenario"] == "Actual")
        ][["Date", "DisplayPrice"]].dropna()
        history_series = history.set_index("Date")["DisplayPrice"].sort_index()
        for forecast_date, projected, monthly_change, annual_change in fit_linear_monthly_forecast(
            history_series
        ):
            forecast_rows.append(
                {
                    "Date": forecast_date,
                    "PeriodLabel": forecast_date.strftime("%Y-%m"),
                    "Year": forecast_date.year,
                    "MonthNumber": forecast_date.month,
                    "GeographyCode": row.GeographyCode,
                    "GeographyName": row.GeographyName,
                    "GeographyLevel": row.GeographyLevel,
                    "Scenario": "Forecast",
                    "ScenarioSort": 2,
                    "ForecastMethod": "Trailing 24-month linear trend",
                    "AveragePrice": projected,
                    "AveragePriceSA": projected,
                    "DisplayPrice": projected,
                    "Index": np.nan,
                    "IndexSeasonallyAdjusted": np.nan,
                    "MonthlyChangePct": monthly_change,
                    "AnnualChangePct": annual_change,
                    "SalesVolume": np.nan,
                    "CrisisPeriod": pd.NA,
                }
            )

    forecast = pd.DataFrame(forecast_rows, columns=actual.columns)
    fact = pd.concat([actual, forecast], ignore_index=True, sort=False)
    return fact.sort_values(["GeographyName", "Date", "ScenarioSort"]).reset_index(drop=True)


def build_market_events_monthly_fact() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for event in MARKET_EVENTS:
        for event_date in pd.date_range(
            event["StartDate"], event["EndDate"], freq="MS"
        ):
            rows.append(
                {
                    "Date": event_date,
                    "EventSort": event["EventSort"],
                    "EventLabel": event["EventLabel"],
                    "EventShortLabel": event["EventShortLabel"],
                    "EventDescription": event["EventDescription"],
                    "EventFlag": 1,
                }
            )
    return pd.DataFrame(rows).sort_values(["Date", "EventSort"]).reset_index(drop=True)


def build_bank_rate_monthly_fact() -> pd.DataFrame:
    bank_rate = pd.read_csv(BANK_RATE_MONTHLY_PATH)
    bank_rate.rename(columns={"DATE": "ReportedDate", "IUMABEDR": "BankRatePct"}, inplace=True)
    bank_rate["ReportedDate"] = pd.to_datetime(bank_rate["ReportedDate"], format="%d %b %Y")
    bank_rate["Date"] = bank_rate["ReportedDate"].dt.to_period("M").dt.to_timestamp()
    bank_rate["PeriodLabel"] = bank_rate["Date"].dt.strftime("%Y-%m")
    bank_rate["Year"] = bank_rate["Date"].dt.year.astype("int64")
    bank_rate["MonthNumber"] = bank_rate["Date"].dt.month.astype("int64")
    bank_rate["BankRatePct"] = pd.to_numeric(bank_rate["BankRatePct"], errors="coerce") / 100.0
    bank_rate = bank_rate.sort_values("Date").reset_index(drop=True)
    bank_rate["BankRatePctPointChange12M"] = bank_rate["BankRatePct"] - bank_rate["BankRatePct"].shift(12)
    bank_rate["BankRatePctPointChange1M"] = bank_rate["BankRatePct"] - bank_rate["BankRatePct"].shift(1)
    return bank_rate[
        [
            "Date",
            "ReportedDate",
            "PeriodLabel",
            "Year",
            "MonthNumber",
            "BankRatePct",
            "BankRatePctPointChange1M",
            "BankRatePctPointChange12M",
        ]
    ]


def build_uk_market_monthly_fact(
    hpi_monthly: pd.DataFrame, bank_rate_monthly: pd.DataFrame
) -> pd.DataFrame:
    uk_market = hpi_monthly[hpi_monthly["GeographyCode"].eq("K02000001")].copy()
    uk_market = uk_market.merge(
        bank_rate_monthly[
            [
                "Date",
                "BankRatePct",
                "BankRatePctPointChange1M",
                "BankRatePctPointChange12M",
            ]
        ],
        on="Date",
        how="left",
    )
    uk_market["MarketPeriod"] = uk_market["Date"].apply(market_period_for_date)
    uk_market["IsMarketStressPeriod"] = ~uk_market["CrisisPeriod"].isna()
    uk_market["ActualPriceForPeriod"] = np.where(
        uk_market["Scenario"].eq("Actual"), uk_market["DisplayPrice"], np.nan
    )
    uk_market["ForecastPrice"] = np.where(
        uk_market["Scenario"].eq("Forecast"), uk_market["DisplayPrice"], np.nan
    )
    correlations: list[float] = []
    actual_market = uk_market["Scenario"].eq("Actual")
    for idx in range(len(uk_market)):
        if not actual_market.iloc[idx]:
            correlations.append(np.nan)
            continue
        window = uk_market.iloc[max(0, idx - 35) : idx + 1]
        window = window[
            window["Scenario"].eq("Actual")
            & window["BankRatePct"].notna()
            & window["AnnualChangePct"].notna()
        ]
        correlations.append(
            window["BankRatePct"].corr(window["AnnualChangePct"])
            if len(window) >= 24
            else np.nan
        )
    uk_market["RatePriceCorrelation36M"] = correlations
    uk_market["BankRateLag12M"] = uk_market["BankRatePct"].shift(12)
    uk_market["HousePriceAnnualGrowthLead12M"] = np.where(
        uk_market["Scenario"].eq("Actual"),
        uk_market["AnnualChangePct"].shift(-12),
        np.nan,
    )
    rate_change = uk_market["BankRatePctPointChange12M"]
    uk_market["BankRateChangeBand12M"] = np.select(
        [
            rate_change <= -0.01,
            rate_change < 0.0025,
            rate_change < 0.01,
            rate_change >= 0.01,
        ],
        [
            "Rate down by 1pp+",
            "Rate broadly flat",
            "Rate up by 0-1pp",
            "Rate up by 1pp+",
        ],
        default="Insufficient rate history",
    )
    uk_market["BankRateChangeBandSort"] = np.select(
        [
            rate_change <= -0.01,
            rate_change < 0.0025,
            rate_change < 0.01,
            rate_change >= 0.01,
        ],
        [1, 2, 3, 4],
        default=9,
    )
    return uk_market[
        [
            "Date",
            "PeriodLabel",
            "Year",
            "MonthNumber",
            "Scenario",
            "ScenarioSort",
            "ForecastMethod",
            "AveragePrice",
            "AveragePriceSA",
            "DisplayPrice",
            "ActualPriceForPeriod",
            "ForecastPrice",
            "Index",
            "IndexSeasonallyAdjusted",
            "MonthlyChangePct",
            "AnnualChangePct",
            "SalesVolume",
            "BankRatePct",
            "BankRatePctPointChange1M",
            "BankRatePctPointChange12M",
            "BankRateLag12M",
            "RatePriceCorrelation36M",
            "HousePriceAnnualGrowthLead12M",
            "BankRateChangeBand12M",
            "BankRateChangeBandSort",
            "CrisisPeriod",
            "MarketPeriod",
            "IsMarketStressPeriod",
        ]
    ].sort_values(["Date", "ScenarioSort"]).reset_index(drop=True)


def read_ons_monthly_timeseries(
    path: Path, value_column: str, value_scale: float = 1.0
) -> pd.DataFrame:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows: list[dict[str, object]] = []
    for item in payload.get("months", []):
        month_number = MONTH_NAME_TO_NUMBER.get(str(item.get("month", "")))
        if not month_number:
            continue
        year = int(item["year"])
        value = pd.to_numeric(item.get("value"), errors="coerce")
        if pd.isna(value):
            continue
        rows.append(
            {
                "Date": pd.Timestamp(year=year, month=month_number, day=1),
                "PeriodLabel": f"{year}-{month_number:02d}",
                "Year": year,
                "MonthNumber": month_number,
                value_column: float(value) * value_scale,
                f"{value_column}SourcePeriod": item.get("label"),
                f"{value_column}UpdateDate": item.get("updateDate"),
            }
        )
    return pd.DataFrame(rows).sort_values("Date").reset_index(drop=True)


def add_common_base_index(
    df: pd.DataFrame, value_columns: list[str], base_date: pd.Timestamp
) -> pd.DataFrame:
    indexed = df.copy()
    base_row = indexed[indexed["Date"].eq(base_date)]
    if base_row.empty:
        return indexed
    for column in value_columns:
        base_value = pd.to_numeric(base_row[column], errors="coerce").iloc[0]
        output_column = f"{column}IndexCommonBase"
        indexed[output_column] = np.where(
            base_value and not pd.isna(base_value),
            indexed[column] / base_value * 100.0,
            np.nan,
        )
    return indexed


def build_labour_market_monthly_fact(
    hpi_monthly: pd.DataFrame, bank_rate_monthly: pd.DataFrame
) -> pd.DataFrame:
    earnings = read_ons_monthly_timeseries(
        AWE_MONTHLY_PATH, "AverageWeeklyEarnings"
    )
    unemployment = read_ons_monthly_timeseries(
        UNEMPLOYMENT_MONTHLY_PATH, "UnemploymentRate", value_scale=0.01
    )
    uk_hpi = hpi_monthly[
        hpi_monthly["GeographyCode"].eq("K02000001")
        & hpi_monthly["Scenario"].eq("Actual")
    ][
        [
            "Date",
            "DisplayPrice",
            "Index",
            "MonthlyChangePct",
            "AnnualChangePct",
        ]
    ].copy()
    uk_hpi.rename(
        columns={
            "DisplayPrice": "UKAverageHousePrice",
            "Index": "UKHPIIndex",
            "MonthlyChangePct": "UKHousePriceMonthlyGrowth",
            "AnnualChangePct": "UKHousePriceAnnualGrowth",
        },
        inplace=True,
    )

    fact = uk_hpi.merge(earnings, on="Date", how="outer").merge(
        unemployment, on="Date", how="outer"
    )
    fact = fact.merge(
        bank_rate_monthly[
            ["Date", "BankRatePct", "BankRatePctPointChange12M"]
        ],
        on="Date",
        how="left",
    )
    fact = fact.sort_values("Date").reset_index(drop=True)
    fact["PeriodLabel"] = fact["Date"].dt.strftime("%Y-%m")
    fact["Year"] = fact["Date"].dt.year.astype("int64")
    fact["MonthNumber"] = fact["Date"].dt.month.astype("int64")
    fact["AverageWeeklyEarningsAnnualGrowth"] = fact[
        "AverageWeeklyEarnings"
    ].pct_change(12)
    fact["UnemploymentRatePointChange12M"] = (
        fact["UnemploymentRate"] - fact["UnemploymentRate"].shift(12)
    )
    fact["AnnualisedWeeklyEarnings"] = fact["AverageWeeklyEarnings"] * 52.0
    fact["HousePriceToAnnualWageRatio"] = fact["UKAverageHousePrice"] / fact[
        "AnnualisedWeeklyEarnings"
    ]
    fact["HousePriceGrowthLessWageGrowth"] = (
        fact["UKHousePriceAnnualGrowth"]
        - fact["AverageWeeklyEarningsAnnualGrowth"]
    )

    common_base = fact.dropna(
        subset=[
            "UKAverageHousePrice",
            "AverageWeeklyEarnings",
            "UnemploymentRate",
        ]
    )["Date"].min()
    if pd.notna(common_base):
        fact = add_common_base_index(
            fact,
            [
                "UKAverageHousePrice",
                "AverageWeeklyEarnings",
                "UnemploymentRate",
            ],
            common_base,
        )
        fact["CommonIndexBaseDate"] = common_base
    else:
        fact["CommonIndexBaseDate"] = pd.NaT

    return fact[
        [
            "Date",
            "PeriodLabel",
            "Year",
            "MonthNumber",
            "UKAverageHousePrice",
            "UKHPIIndex",
            "UKHousePriceMonthlyGrowth",
            "UKHousePriceAnnualGrowth",
            "AverageWeeklyEarnings",
            "AverageWeeklyEarningsAnnualGrowth",
            "AnnualisedWeeklyEarnings",
            "UnemploymentRate",
            "UnemploymentRatePointChange12M",
            "BankRatePct",
            "BankRatePctPointChange12M",
            "HousePriceToAnnualWageRatio",
            "HousePriceGrowthLessWageGrowth",
            "UKAverageHousePriceIndexCommonBase",
            "AverageWeeklyEarningsIndexCommonBase",
            "UnemploymentRateIndexCommonBase",
            "CommonIndexBaseDate",
            "AverageWeeklyEarningsSourcePeriod",
            "AverageWeeklyEarningsUpdateDate",
            "UnemploymentRateSourcePeriod",
            "UnemploymentRateUpdateDate",
        ]
    ]


def build_macro_indicator_monthly_fact(
    labour_market_monthly: pd.DataFrame,
) -> pd.DataFrame:
    indicator_specs = [
        (
            1,
            "UK average house price",
            "Index: house price",
            "UKAverageHousePrice",
            "UKAverageHousePriceIndexCommonBase",
            "UKHousePriceAnnualGrowth",
            "GBP",
        ),
        (
            2,
            "Average weekly earnings",
            "Index: weekly earnings",
            "AverageWeeklyEarnings",
            "AverageWeeklyEarningsIndexCommonBase",
            "AverageWeeklyEarningsAnnualGrowth",
            "GBP per week",
        ),
        (
            3,
            "Unemployment rate",
            "Index: unemployment rate",
            "UnemploymentRate",
            "UnemploymentRateIndexCommonBase",
            "UnemploymentRatePointChange12M",
            "Percent",
        ),
    ]
    rows: list[pd.DataFrame] = []
    for sort_order, indicator, label, value_column, index_column, movement_column, unit in indicator_specs:
        frame = labour_market_monthly[
            [
                "Date",
                "PeriodLabel",
                "Year",
                "MonthNumber",
                value_column,
                index_column,
                movement_column,
                "CommonIndexBaseDate",
            ]
        ].copy()
        frame.rename(
            columns={
                value_column: "ActualValue",
                index_column: "IndexedValue",
                movement_column: "AnnualMovementValue",
            },
            inplace=True,
        )
        frame["IndicatorSort"] = sort_order
        frame["Indicator"] = indicator
        frame["IndicatorLabel"] = label
        frame["Unit"] = unit
        rows.append(frame)
    return (
        pd.concat(rows, ignore_index=True)
        .dropna(subset=["IndexedValue"])
        .query("Date >= CommonIndexBaseDate")
        .sort_values(["Date", "IndicatorSort"])
        .reset_index(drop=True)
    )


def classify_geography(code: str, name: str) -> tuple[str, str | None, str | None]:
    if code == "K02000001":
        return "Country", None, None
    if code == "K04000001":
        return "England and Wales", None, None
    if code in {"E92000001", "W92000004", "S92000003", "N92000002"}:
        return "Country", None, None
    if code.startswith("E12"):
        return "Region", "E92000001", "England"
    if code.startswith("E0") or code.startswith("W0"):
        if name and code.startswith("W"):
            return "Local Authority", "W92000004", "Wales"
        return "Local Authority", None, None
    return "Other", None, None


def build_geography_dimension(
    purchase_local: pd.DataFrame,
    purchase_region: pd.DataFrame,
    rental_region: pd.DataFrame,
    rental_local: pd.DataFrame,
    uk_purchase: pd.DataFrame,
) -> pd.DataFrame:
    local = pd.concat(
        [
            purchase_local[
                ["GeographyCode", "GeographyName", "RegionCode", "RegionName"]
            ],
            rental_local[
                ["GeographyCode", "GeographyName", "RegionCode", "RegionName"]
            ],
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["GeographyCode"])
    local["GeographyLevel"] = "Local Authority"
    local["ParentGeographyCode"] = local["RegionCode"]
    local["ParentGeographyName"] = local["RegionName"]
    local["CountryCode"] = local["RegionCode"].apply(
        lambda code: "E92000001" if str(code).startswith("E12") else "W92000004"
    )
    local["CountryName"] = local["CountryCode"].map(
        {"E92000001": "England", "W92000004": "Wales"}
    )

    higher = pd.concat(
        [
            purchase_region[["GeographyCode", "GeographyName"]],
            rental_region[["GeographyCode", "GeographyName"]],
            uk_purchase[["GeographyCode", "GeographyName"]],
            pd.DataFrame(
                [{"GeographyCode": "K02000001", "GeographyName": "United Kingdom"}]
            ),
        ],
        ignore_index=True,
    ).drop_duplicates()

    higher[["DerivedLevel", "DerivedCountryCode", "DerivedCountryName"]] = higher.apply(
        lambda row: pd.Series(classify_geography(row["GeographyCode"], row["GeographyName"])),
        axis=1,
    )
    higher["GeographyLevel"] = higher["DerivedLevel"]
    higher["ParentGeographyCode"] = higher["DerivedCountryCode"]
    higher["ParentGeographyName"] = higher["DerivedCountryName"]
    higher["CountryCode"] = higher.apply(
        lambda row: row["GeographyCode"]
        if row["DerivedLevel"] == "Country"
        else row["DerivedCountryCode"],
        axis=1,
    )
    higher["CountryName"] = higher.apply(
        lambda row: row["GeographyName"]
        if row["DerivedLevel"] == "Country"
        else row["DerivedCountryName"],
        axis=1,
    )
    higher = higher[
        [
            "GeographyCode",
            "GeographyName",
            "GeographyLevel",
            "ParentGeographyCode",
            "ParentGeographyName",
            "CountryCode",
            "CountryName",
        ]
    ]

    geography = pd.concat(
        [
            local[
                [
                    "GeographyCode",
                    "GeographyName",
                    "GeographyLevel",
                    "ParentGeographyCode",
                    "ParentGeographyName",
                    "CountryCode",
                    "CountryName",
                ]
            ],
            higher,
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["GeographyCode"])

    sort_map = {
        "England and Wales": 1,
        "Country": 2,
        "Region": 3,
        "Local Authority": 4,
        "Other": 9,
    }
    geography["GeographyLevelSort"] = geography["GeographyLevel"].map(sort_map).fillna(9)
    geography["MapCountry"] = "United Kingdom"
    geography["GeoLabel"] = geography["GeographyName"] + ", " + geography["MapCountry"]
    geography["IsEngland"] = geography["CountryCode"].eq("E92000001")
    geography["IsWales"] = geography["CountryCode"].eq("W92000004")
    geography["IsScotland"] = geography["CountryCode"].eq("S92000003")
    geography["IsNorthernIreland"] = geography["CountryCode"].eq("N92000002")

    return geography.sort_values(
        ["GeographyLevelSort", "CountryName", "ParentGeographyName", "GeographyName"]
    ).reset_index(drop=True)


def build_date_dimension(max_date: pd.Timestamp | None = None) -> pd.DataFrame:
    end_date = pd.Timestamp("2027-12-31")
    if max_date is not None and pd.notna(max_date):
        max_date = pd.Timestamp(max_date)
        end_date = max(end_date, pd.Timestamp(year=max_date.year + 1, month=12, day=31))
    dates = pd.date_range("1968-04-01", end_date, freq="D")
    df = pd.DataFrame({"Date": dates})
    df["DateKey"] = df["Date"].dt.strftime("%Y%m%d").astype("int64")
    df["Year"] = df["Date"].dt.year.astype("int64")
    df["MonthNumber"] = df["Date"].dt.month.astype("int64")
    df["MonthNameShort"] = df["Date"].dt.strftime("%b")
    df["MonthNameLong"] = df["Date"].dt.strftime("%B")
    df["Quarter"] = "Q" + df["Date"].dt.quarter.astype(str)
    df["YearMonth"] = df["Date"].dt.strftime("%Y-%m")
    df["YearQuarter"] = df["Year"].astype(str) + "-" + df["Quarter"]
    df["DayOfMonth"] = df["Date"].dt.day.astype("int64")
    df["DayOfWeekNumber"] = (df["Date"].dt.dayofweek + 1).astype("int64")
    df["DayOfWeekName"] = df["Date"].dt.strftime("%A")
    df["IsMonthEnd"] = df["Date"].dt.is_month_end
    df["IsQuarterEnd"] = df["Date"].dt.is_quarter_end
    df["IsYearEnd"] = df["Date"].dt.is_year_end
    df["FinancialYearEnding"] = (
        df["Date"].dt.year + (df["Date"].dt.month > 3).astype("int64")
    ).astype("int64")
    df["FinancialYearLabel"] = (
        (df["FinancialYearEnding"] - 1).astype(str).str[-2:]
        + "/"
        + df["FinancialYearEnding"].astype(str).str[-2:]
    )
    df["HousingPurchasePeriodEndYear"] = df["Year"]
    return df


def write_csv(df: pd.DataFrame, file_name: str) -> None:
    output_path = CURATED_DIR / file_name
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Wrote {output_path.relative_to(ROOT)} ({len(df):,} rows)")


def main() -> None:
    CURATED_DIR.mkdir(parents=True, exist_ok=True)

    purchase_local = build_purchase_local_fact()
    purchase_region = build_purchase_region_fact()
    rental_region = build_rental_region_fact()
    rental_local = build_rental_local_fact()
    uk_purchase = build_uk_purchase_country_region_median_fact()
    dim_geography = build_geography_dimension(
        purchase_local, purchase_region, rental_region, rental_local, uk_purchase
    )
    hpi_monthly = build_hpi_monthly_fact(dim_geography)
    market_events = build_market_events_monthly_fact()
    bank_rate_monthly = build_bank_rate_monthly_fact()
    uk_market_monthly = build_uk_market_monthly_fact(hpi_monthly, bank_rate_monthly)
    labour_market_monthly = build_labour_market_monthly_fact(
        hpi_monthly, bank_rate_monthly
    )
    macro_indicator_monthly = build_macro_indicator_monthly_fact(
        labour_market_monthly
    )
    max_fact_date = max(
        purchase_local["PeriodEndDate"].max(),
        purchase_region["PeriodEndDate"].max(),
        rental_region["PeriodEndDate"].max(),
        rental_local["PeriodEndDate"].max(),
        uk_purchase["PeriodEndDate"].max(),
        hpi_monthly["Date"].max(),
        bank_rate_monthly["Date"].max(),
        labour_market_monthly["Date"].max(),
    )
    dim_date = build_date_dimension(max_fact_date)

    write_csv(purchase_local, "fact_purchase_local.csv")
    write_csv(purchase_region, "fact_purchase_region.csv")
    write_csv(rental_region, "fact_rental_region.csv")
    write_csv(rental_local, "fact_rental_local.csv")
    write_csv(uk_purchase, "fact_uk_purchase_country_region_median.csv")
    write_csv(hpi_monthly, "fact_hpi_monthly.csv")
    write_csv(market_events, "fact_market_events_monthly.csv")
    write_csv(bank_rate_monthly, "fact_bank_rate_monthly.csv")
    write_csv(uk_market_monthly, "fact_uk_market_monthly.csv")
    write_csv(labour_market_monthly, "fact_labour_market_monthly.csv")
    write_csv(macro_indicator_monthly, "fact_macro_indicator_monthly.csv")
    write_csv(dim_geography, "dim_geography.csv")
    write_csv(dim_date, "dim_date.csv")

    summary = {
        "latest_purchase_year": int(purchase_local["Year"].max()),
        "latest_rental_year": int(rental_local["Year"].max()),
        "latest_hpi_month": hpi_monthly.loc[
            hpi_monthly["Scenario"].eq("Actual"), "Date"
        ].max().strftime("%Y-%m-%d"),
        "latest_bank_rate_month": bank_rate_monthly["Date"].max().strftime("%Y-%m-%d"),
        "local_authorities_in_purchase_fact": int(purchase_local["GeographyCode"].nunique()),
        "local_authorities_in_rental_fact": int(rental_local["GeographyCode"].nunique()),
        "geographies_in_monthly_hpi_fact": int(hpi_monthly["GeographyCode"].nunique()),
        "monthly_hpi_rows": int(len(hpi_monthly)),
        "bank_rate_monthly_rows": int(len(bank_rate_monthly)),
        "uk_market_monthly_rows": int(len(uk_market_monthly)),
        "labour_market_monthly_rows": int(len(labour_market_monthly)),
        "macro_indicator_monthly_rows": int(len(macro_indicator_monthly)),
        "latest_labour_market_month": labour_market_monthly["Date"].max().strftime("%Y-%m-%d"),
        "market_event_rows": int(len(market_events)),
        "geographies_in_dimension": int(dim_geography["GeographyCode"].nunique()),
        "date_rows": int(len(dim_date)),
    }
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {SUMMARY_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
