import csv


def parse_cell2info(tsv_path: str) -> dict:
    taxonomy = []  # taxonomy_id, organism
    cells = []  # cell_id, taxonomy_id, name, tissue
    synonyms = []  # cell_id, synonym

    with open(tsv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            # cells
            cell_id = int(row["CellID"])
            name = row.get("Name") or None
            tissue = row.get("Tissue") or None

            # taxonomy
            organism = row.get("Organism") or None
            taxonomy_id = int(row["TaxonomyID"]) if row["TaxonomyID"] else None
            cell_synonyms = row["Synonyms"].split("|") if row["Synonyms"] else []

            if taxonomy_id:
                taxonomy.append({"taxonomy_id": taxonomy_id, "organism": organism})
            cells.append(
                {
                    "cell_id": cell_id,
                    "taxonomy_id": taxonomy_id,
                    "name": name,
                    "tissue": tissue,
                }
            )
            for synonym in cell_synonyms:
                synonym = synonym.strip()
                if synonym:
                    synonyms.append({"cell_id": cell_id, "synonym": synonym})

    # taxonomy 중복 제거
    seen = set()
    unique_taxonomy = []
    for row in taxonomy:
        key = (row["taxonomy_id"], row["organism"])
        if key not in seen:
            seen.add(key)
            unique_taxonomy.append(row)

    return {
        "taxonomy": unique_taxonomy,
        "cells": cells,
        "synonyms": synonyms,
    }
