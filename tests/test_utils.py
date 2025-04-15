from dags.cell2info.utils import parse_cell2info


def test_parse_cell2info(tmp_path):
    test_file = tmp_path / "cell2info.tsv"
    test_file.write_text(
        "CellID\tName\tTissue\tOrganism\tTaxonomyID\tSynonyms\n"
        "1\tHEL\tblood\tHomo sapiens (human)\t9606\tHel|GM06141|GM06141B|Human ErythroLeukemia\n"
        "161709\tMyeloid\tprogenitor\tcells\t\t\n"
    )

    result = parse_cell2info(str(test_file))

    assert len(result["cells"]) == 2
    assert len(result["taxonomy"]) == 1
    assert len(result["synonyms"]) == 4
