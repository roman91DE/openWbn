import polars as pl
import duckdb as ddb


main_file = "./avg_age.csv"
helper_file = "./ortsbezirke_wiesbaden.csv"

con = ddb.connect()
main_data = con.read_csv(main_file, decimal=",").select(
    "*, substr(wahlbezirk_id, 1, 2) as ortsbezirk_id"
)
helper_data = con.read_csv(helper_file)

data = main_data.join(other_rel=helper_data, how="left", condition="ortsbezirk_id")

invalid = data.filter("ortsbezirk_name is null").select("wahlbezirk_id, ortsbezirk_id")
filename = "Nicht-matchbare_wahlbezirke.csv"
invalid.write_csv(filename)
print(f"Nicht-matchbare Wahlbezirke in '{filename}' exportiert")
