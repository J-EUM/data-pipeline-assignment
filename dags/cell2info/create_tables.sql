CREATE TABLE IF NOT EXISTS taxonomy (
    taxonomy_id INTEGER PRIMARY KEY,
    organism TEXT
);

CREATE TABLE IF NOT EXISTS cells (
    cell_id INTEGER PRIMARY KEY,
    taxonomy_id INTEGER,
    name TEXT,
    tissue TEXT,
    FOREIGN KEY (taxonomy_id) REFERENCES taxonomy(taxonomy_id)
);

CREATE TABLE IF NOT EXISTS synonyms (
    cell_id INTEGER,
    synonym TEXT,
    PRIMARY KEY (cell_id, synonym)
);
