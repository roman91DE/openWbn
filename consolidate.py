import duckdb

DB_PATH = "wbn.duckdb"

con = duckdb.connect(DB_PATH)

# --- 1. Ortsbezirke: population & migration (already at district level) ---
con.execute("DROP TABLE IF EXISTS bevoelkerung")
con.execute("""
    CREATE TABLE bevoelkerung AS
    SELECT
        ortsbezirk_id,
        ortsbezirk_name,
        bevoelkerungsbestand,
        frauen,
        maenner,
        deutsche,
        auslaender_innen,
        personen_mit_migrationshintergrund,
        ROUND(auslaender_innen * 100.0 / bevoelkerungsbestand, 2) AS anteil_auslaender,
        ROUND(personen_mit_migrationshintergrund * 100.0 / bevoelkerungsbestand, 2) AS anteil_migration,
        datum
    FROM read_csv(
        'geodata/data/bb_regobz_jan26.csv',
        delim=';', header=true
    )
    WHERE ortsbezirk_id != '00'
""")
n = con.execute("SELECT COUNT(*) FROM bevoelkerung").fetchone()[0]
print(f"bevoelkerung: {n} ortsbezirke")

# --- 2. Age: aggregate from wahlbezirk to ortsbezirk level ---
con.execute("DROP TABLE IF EXISTS alter_ortsbezirke")
con.execute("""
    CREATE TABLE alter_ortsbezirke AS
    WITH wahlbezirke AS (
        SELECT
            *,
            SUBSTR(wahlbezirk_id, 1, 2) AS ortsbezirk_id
        FROM read_csv(
            'avg_age.csv',
            delim=';', header=true, decimal_separator=','
        )
        WHERE wahlbezirk_id != '00'
    ),
    lookup AS (
        SELECT ortsbezirk_id, ortsbezirk_name
        FROM read_csv('ortsbezirke_wiesbaden.csv', delim=';', header=true)
        WHERE ortsbezirk_id != '00'
    ),
    matched AS (
        SELECT w.*, l.ortsbezirk_name
        FROM wahlbezirke w
        JOIN lookup l ON w.ortsbezirk_id = l.ortsbezirk_id
    )
    SELECT
        ortsbezirk_id,
        ortsbezirk_name,
        ROUND(AVG(durchschnittsalter_bevoelkerung), 2) AS avg_alter_gesamt,
        ROUND(AVG(durchschnittsalter_frauen), 2) AS avg_alter_frauen,
        ROUND(AVG(durchschnittsalter_maenner), 2) AS avg_alter_maenner,
        ROUND(AVG(durchschnittsalter_deutsche), 2) AS avg_alter_deutsche,
        ROUND(AVG(durchschnittsalter_auslaender_innen), 2) AS avg_alter_auslaender,
        ROUND(AVG(durchschnittsalter_personen_mit_migrationshintergrund), 2) AS avg_alter_migration,
        COUNT(*) AS anzahl_wahlbezirke
    FROM matched
    GROUP BY ortsbezirk_id, ortsbezirk_name
    ORDER BY ortsbezirk_id
""")
n = con.execute("SELECT COUNT(*) FROM alter_ortsbezirke").fetchone()[0]
print(f"alter_ortsbezirke: {n} ortsbezirke")

# --- 3. Rent prices & social housing by district (2014-2023) ---
con.execute("DROP TABLE IF EXISTS mieten_sozialwohnungen")
con.execute("""
    CREATE TABLE mieten_sozialwohnungen AS
    SELECT
        ortsbezirk_id,
        ortsbezirk_name,
        jahr,
        sozialwohnungen,
        anzahl_der_angebotenen_mietwohnungen,
        angebotsmieten
    FROM read_csv(
        'geodata/data/oeffentlich_geforderter_wohnungsbau_mietpreise_ortsbezirke_2014_bis_2023.csv',
        delim=';', header=true, decimal_separator=','
    )
    WHERE ortsbezirk_id != '00'
    ORDER BY ortsbezirk_id, jahr
""")
n = con.execute("SELECT COUNT(*) FROM mieten_sozialwohnungen").fetchone()[0]
print(f"mieten_sozialwohnungen: {n} rows (district x year)")

# --- 4. City-wide rent trends (2007-2024) ---
con.execute("DROP TABLE IF EXISTS angebotsmieten_stadt")
con.execute("""
    CREATE TABLE angebotsmieten_stadt AS
    SELECT *
    FROM read_csv(
        'angebotsmieten_2007_bis_2024.csv',
        delim=';', header=true, decimal_separator=','
    )
    ORDER BY jahr
""")
n = con.execute("SELECT COUNT(*) FROM angebotsmieten_stadt").fetchone()[0]
print(f"angebotsmieten_stadt: {n} years")

# --- 5. Consolidated view: latest snapshot per ortsbezirk ---
con.execute("DROP VIEW IF EXISTS uebersicht")
con.execute("""
    CREATE VIEW uebersicht AS
    SELECT
        b.ortsbezirk_id,
        b.ortsbezirk_name,
        -- population & migration
        b.bevoelkerungsbestand,
        b.auslaender_innen,
        b.personen_mit_migrationshintergrund,
        b.anteil_auslaender,
        b.anteil_migration,
        -- age
        a.avg_alter_gesamt,
        a.avg_alter_frauen,
        a.avg_alter_maenner,
        a.avg_alter_deutsche,
        a.avg_alter_auslaender,
        a.avg_alter_migration,
        -- rent & social housing (latest year = 2023)
        m.angebotsmieten AS miete_2023,
        m.sozialwohnungen AS sozialwohnungen_2023,
        m.anzahl_der_angebotenen_mietwohnungen AS angebote_2023
    FROM bevoelkerung b
    LEFT JOIN alter_ortsbezirke a USING (ortsbezirk_id)
    LEFT JOIN mieten_sozialwohnungen m
        ON b.ortsbezirk_id = m.ortsbezirk_id AND m.jahr = 2023
    ORDER BY b.ortsbezirk_id
""")
print("\nuebersicht (consolidated view):")
result = con.execute("SELECT * FROM uebersicht").fetchdf()
print(result.to_string())

con.close()
print(f"\nDatabase written to {DB_PATH}")
