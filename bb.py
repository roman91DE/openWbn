import duckdb


file = "./bb_regwbz.csv"
helper_file = "./ortsbezirke_wiesbaden.csv"

con = duckdb.connect()
main_data = con.read_csv(file, decimal=",").select(
    "*, substr(wahlbezirk_id, 1, 2) AS ortsbezirk_id"
)


helper_data = con.read_csv(helper_file)
data = main_data.join(other_rel=helper_data, how="left", condition="ortsbezirk_id")
invalid = data.filter("ortsbezirk_name IS NULL").select("wahlbezirk_id, ortsbezirk_id")
matched = data.filter("ortsbezirk_name IS NOT NULL")
res = matched.select(
    "wahlbezirk_id, ortsbezirk_name, personen_mit_migrationshintergrund / bevoelkerungsbestand AS anteil_migration,auslaender_innen / bevoelkerungsbestand AS anteil_auslaender "
).order("anteil_migration DESC")
