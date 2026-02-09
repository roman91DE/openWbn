import json
import duckdb


def osm_to_geojson(path):
    with open(path) as f:
        data = json.load(f)

    features = []
    for el in data["elements"]:
        if el["type"] != "relation":
            continue

        name = el["tags"].get("name", "")
        # assemble outer ring from member ways
        ways = {}
        for m in el["members"]:
            if m["type"] == "way" and m.get("role", "outer") == "outer":
                coords = [(n["lon"], n["lat"]) for n in m["geometry"]]
                ways[m["ref"]] = coords

        ring = _chain_ways(list(ways.values()))
        if not ring:
            continue

        features.append(
            {
                "type": "Feature",
                "properties": {"name": name},
                "geometry": {"type": "Polygon", "coordinates": [ring]},
            }
        )

    return {"type": "FeatureCollection", "features": features}


def _chain_ways(segments):
    """Chain way segments into a closed ring."""
    if not segments:
        return []
    ring = list(segments.pop(0))
    while segments:
        last = ring[-1]
        found = False
        for i, seg in enumerate(segments):
            if seg[0] == last:
                ring.extend(seg[1:])
                segments.pop(i)
                found = True
                break
            elif seg[-1] == last:
                ring.extend(reversed(seg[:-1]))
                segments.pop(i)
                found = True
                break
        if not found:
            break
    return ring


# name mapping: OSM name -> CSV ortsbezirk_name
NAME_MAP = {
    "Mainz-Amöneburg": "Amöneburg",
    "Mainz-Kastel": "Kastel",
    "Mainz-Kostheim": "Kostheim",
    "Westend / Bleichstraße": "Westend, Bleichstraße",
    "Rheingauviertel / Hollerborn": "Rheingauviertel, Hollerborn",
}

geojson = osm_to_geojson("data/ortsbezirke_osm.json")

# apply name mapping
for f in geojson["features"]:
    n = f["properties"]["name"]
    f["properties"]["name"] = NAME_MAP.get(n, n)

with open("data/ortsbezirke.geojson", "w") as f:
    json.dump(geojson, f)

print(f"{len(geojson['features'])} polygons written")

# load into duckdb
con = duckdb.connect("data/wiesbaden.duckdb")
con.execute("INSTALL spatial")
con.execute("LOAD spatial")

con.execute("DROP TABLE IF EXISTS ortsbezirke")

pop = (
    con.read_csv("data/bb_regobz_jan26.csv", delimiter=";", header=True)
    .filter("ortsbezirk_id != '00'")
    .project("""
        ortsbezirk_id,
        ortsbezirk_name,
        bevoelkerungsbestand,
        auslaender_innen,
        personen_mit_migrationshintergrund,
        ROUND(auslaender_innen * 100.0 / bevoelkerungsbestand, 2) AS anteil_auslaender,
        ROUND(personen_mit_migrationshintergrund * 100.0 / bevoelkerungsbestand, 2) AS anteil_migrationshintergrund,
        datum
    """)
)
pop.create("ortsbezirke")

con.execute("DROP TABLE IF EXISTS mieten")
mieten = (
    con.read_csv(
        "data/oeffentlich_geforderter_wohnungsbau_mietpreise_ortsbezirke_2014_bis_2023.csv",
        delimiter=";", header=True, decimal=","
    )
    .project("""
        ortsbezirk_id,
        ortsbezirk_name,
        jahr,
        sozialwohnungen,
        anzahl_der_angebotenen_mietwohnungen,
        angebotsmieten
    """)
)
mieten.create("mieten")
print("mieten table created")

con.execute("DROP TABLE IF EXISTS geo")
con.execute("""
    CREATE TABLE geo AS
    SELECT name, geom FROM st_read('data/ortsbezirke.geojson')
""")

# check join coverage
unmatched = con.execute("""
    SELECT o.ortsbezirk_name FROM ortsbezirke o
    LEFT JOIN geo g ON o.ortsbezirk_name = g.name
    WHERE g.name IS NULL
""").fetchall()
if unmatched:
    print("WARNING unmatched:", [r[0] for r in unmatched])

matched = con.execute(
    "SELECT COUNT(*) FROM ortsbezirke o JOIN geo g ON o.ortsbezirk_name = g.name"
).fetchone()[0]
print(f"{matched}/26 ortsbezirke matched with geodata")

con.close()
