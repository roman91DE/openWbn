import duckdb as ddb


main_file = "./avg_age.csv"
helper_file = "./ortsbezirke_wiesbaden.csv"

con = ddb.connect()
main_data = con.read_csv(main_file, decimal=",").select(
    "*, SUBSTR(wahlbezirk_id, 1, 2) AS ortsbezirk_id"
)
helper_data = con.read_csv(helper_file)

data = main_data.join(other_rel=helper_data, how="left", condition="ortsbezirk_id")

invalid = data.filter("ortsbezirk_name IS NULL").select("wahlbezirk_id, ortsbezirk_id")
filename = "Nicht-matchbare_wahlbezirke.csv"
invalid.write_csv(filename)
print(f"Nicht-matchbare Wahlbezirke in '{filename}' exportiert")


matched = data.filter("ortsbezirk_name IS NOT NULL")

agg_cols = [
    "durchschnittsalter_bevoelkerung",
    "durchschnittsalter_frauen",
    "durchschnittsalter_maenner",
    "durchschnittsalter_deutsche",
    "durchschnittsalter_auslaender_innen",
    "durchschnittsalter_personen_mit_migrationshintergrund",
]

aggregation_expression = """
    ortsbezirk_name,
    AVG({0}) AS avg_age_all,
    STDDEV_POP({0}) AS std_age_all,
    AVG({1}) AS avg_age_female,
    STDDEV_POP({1}) AS std_age_female,
    AVG({2}) AS avg_age_male,
    STDDEV_POP({2}) AS std_age_male,
    AVG({3}) AS avg_age_germans,
    STDDEV_POP({3}) AS std_age_germans,
    AVG({4}) AS avg_age_foreign,
    STDDEV_POP({4}) AS std_age_foreign,
    AVG({5}) AS avg_age_migrated,
    STDDEV_POP({5}) AS std_age_migrated
""".format(*agg_cols)

result = matched.aggregate(aggregation_expression, group_expr="ALL")
