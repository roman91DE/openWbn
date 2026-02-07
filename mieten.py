from pathlib import Path
import altair as alt
import duckdb
import polars as pl


file = Path() / "angebotsmieten_2007_bis_2024.csv"
assert file.exists(), "missing file :-("


con = duckdb.connect()
raw_data = con.read_csv(file, decimal=",")

cols = [
    "jahr",
    "anzahl_der_angebotenen_mietwohnungen",
    "durchschnittsmiete_median_in_euro_je_qm",
    "durchschnittsmiete_median_in_euro_0_40_qm",
    "durchschnittsmiete_median_in_euro_40_60_qm",
    "durchschnittsmiete_median_in_euro_60_80_qm",
    "durchschnittsmiete_median_in_euro_80_100_qm",
    "durchschnittsmiete_median_in_euro_100_qm",
]

mietpreise = raw_data.select(*cols)

df = mietpreise.pl()
melted = df.unpivot(
    index="jahr",
    on=cols[3:],
    variable_name="qm",
    value_name="median_preis",
).with_columns(
    pl.col("qm").replace(
        {
            "durchschnittsmiete_median_in_euro_0_40_qm": "0–40 qm",
            "durchschnittsmiete_median_in_euro_40_60_qm": "40–60 qm",
            "durchschnittsmiete_median_in_euro_60_80_qm": "60–80 qm",
            "durchschnittsmiete_median_in_euro_80_100_qm": "80–100 qm",
            "durchschnittsmiete_median_in_euro_100_qm": "100+ qm",
        }
    )
)

chart = (
    alt.Chart(melted)
    .mark_line()
    .encode(x="jahr:O", y="median_preis:Q", color="qm:N")
    .properties(title="Median Angebotsmieten nach Wohnungsgröße (2007–2024)")
)

chart.save("mietpreise.html")

print("Finished mieten.py")
