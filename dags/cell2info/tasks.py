import gzip
import json
import logging
import os
import shutil
import sqlite3

import requests
from airflow.models import Variable
from cell2info.utils import parse_cell2info

DATA_PATH = Variable.get("data_path")
TEMP_DIR = os.path.join(DATA_PATH, "temp")
SQLITE_DB_PATH = Variable.get("sqlite_db_path")


def download_cell2info(ts_nodash):
    url = "https://ftp.ncbi.nlm.nih.gov/pubchem/Target/cell2info.gz"
    response = requests.get(url)

    gz_filename = f"cell2info_{ts_nodash}.gz"
    gz_path = os.path.join(DATA_PATH, gz_filename)

    with open(gz_path, "wb") as f:
        f.write(response.content)

    return gz_path


def extract_gz_to_tsv(**context):
    os.makedirs(TEMP_DIR, exist_ok=True)
    tsv_path = os.path.join(TEMP_DIR, "cell2info.tsv")
    gz_path = context["ti"].xcom_pull(task_ids="download_data")

    with gzip.open(gz_path, "rb") as f_in, open(tsv_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    return tsv_path


def parse_and_prepare(**context):
    tsv_path = context["ti"].xcom_pull(task_ids="prepare_data.extract_gz_to_tsv")
    parsed_data = parse_cell2info(tsv_path)

    # JSON으로 저장
    json_paths = {
        "taxonomy": os.path.join(TEMP_DIR, "taxonomy.json"),
        "cells": os.path.join(TEMP_DIR, "cells.json"),
        "synonyms": os.path.join(TEMP_DIR, "synonyms.json"),
    }

    with open(json_paths["taxonomy"], "w", encoding="utf-8") as f:
        json.dump(parsed_data["taxonomy"], f)
    with open(json_paths["cells"], "w", encoding="utf-8") as f:
        json.dump(parsed_data["cells"], f)
    with open(json_paths["synonyms"], "w", encoding="utf-8") as f:
        json.dump(parsed_data["synonyms"], f)

    # XCom으로 파일 경로 넘김
    return json_paths


def load_cells_to_sqlite(**context):
    json_paths = context["ti"].xcom_pull(task_ids="prepare_data.parse_and_prepare")
    with open(json_paths["cells"], "r", encoding="utf-8") as f:
        cells = json.load(f)

    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cells (
            cell_id INTEGER PRIMARY KEY,
            taxonomy_id INTEGER,
            name TEXT,
            tissue TEXT,
            FOREIGN KEY (taxonomy_id) REFERENCES taxonomy(taxonomy_id)
        );
    """
    )

    cursor.executemany(
        "INSERT OR IGNORE INTO cells (cell_id, taxonomy_id, name, tissue) VALUES (?, ?, ?, ?)",
        [
            (cell["cell_id"], cell["taxonomy_id"], cell["name"], cell["tissue"])
            for cell in cells
        ],
    )

    conn.commit()
    conn.close()


def load_taxonomy_to_sqlite(**context):
    json_paths = context["ti"].xcom_pull(task_ids="prepare_data.parse_and_prepare")
    with open(json_paths["taxonomy"], "r", encoding="utf-8") as f:
        taxonomy = json.load(f)

    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS taxonomy (
            taxonomy_id INTEGER PRIMARY KEY,
            organism TEXT
        );
    """
    )

    cursor.executemany(
        "INSERT OR IGNORE INTO taxonomy (taxonomy_id, organism) VALUES (?, ?)",
        [(cell["taxonomy_id"], cell["organism"]) for cell in taxonomy],
    )

    conn.commit()
    conn.close()


def load_synonyms_to_sqlite(**context):
    json_paths = context["ti"].xcom_pull(task_ids="prepare_data.parse_and_prepare")
    with open(json_paths["synonyms"], "r", encoding="utf-8") as f:
        synonyms = json.load(f)

    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS synonyms (
            cell_id INTEGER,
            synonym TEXT,
            PRIMARY KEY (cell_id, synonym)
        );
    """
    )

    cursor.executemany(
        "INSERT OR IGNORE INTO synonyms (cell_id, synonym) VALUES (?, ?)",
        [(cell["cell_id"], cell["synonym"]) for cell in synonyms],
    )

    conn.commit()
    conn.close()


def delete_temp_directory():
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
        logging.info(f"Deleted existing temp directory: {TEMP_DIR}")
