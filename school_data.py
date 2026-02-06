from pathlib import Path

import duckdb
import polars as pl
import altair as alt


file = Path() / "school_data.csv"
assert file.exists(), "missing file :-("


con = duckdb.connect()
raw_data = con.read_csv(file)

df = (
    raw_data.select(
        "schuljahr",
        "hauptschuelerinnen_und_hauptschueler_an_allgemeinbildenden_schulen",
        "realschuelerinnen_und_realschueler_an_allgemeinbildenden_schulen",
        "gymnasiastinnen_und_gymnasiasten_an_allgemeinbildenden_schulen",
        "schuelerinnen_und_schueler_in_integrierten_gesamtschulen_an_allgemeinbildenden_schulen",
    )
    .pl()
    .rename(
        {
            "hauptschuelerinnen_und_hauptschueler_an_allgemeinbildenden_schulen": "haupt",
            "realschuelerinnen_und_realschueler_an_allgemeinbildenden_schulen": "real",
            "gymnasiastinnen_und_gymnasiasten_an_allgemeinbildenden_schulen": "gym",
            "schuelerinnen_und_schueler_in_integrierten_gesamtschulen_an_allgemeinbildenden_schulen": "igs",
        }
    )
    .with_columns(gesamt=pl.sum_horizontal(pl.exclude("schuljahr")))
    .with_columns(
        [
            (pl.col("haupt") / pl.col("gesamt")).alias("haupt_rel"),
            (pl.col("real") / pl.col("gesamt")).alias("real_rel"),
            (pl.col("gym") / pl.col("gesamt")).alias("gym_rel"),
            (pl.col("igs") / pl.col("gesamt")).alias("igs_rel"),
        ]
    )
)
melted = df.unpivot(
    index="schuljahr",
    on=["haupt_rel", "real_rel", "gym_rel", "igs_rel"],
    variable_name="school_form",
    value_name="relative_share",
)

chart = (
    alt.Chart(melted)
    .mark_line()
    .encode(x="schuljahr:O", y="relative_share:Q", color="school_form:N")
    .properties(title="Relative Share of School Forms Over Time")
)

chart.save("school_forms_over_time.html")
print("Chart saved as school_forms_over_time.html")


print("Finished main.py")
