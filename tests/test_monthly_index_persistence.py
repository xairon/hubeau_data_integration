from hubeau_pipeline.ml.monthly_index_persistence import _CREATE, _INSERT

def test_ddl_has_composite_pk_on_type_code_month():
    assert "gold.fct_monthly_index" in _CREATE
    assert "PRIMARY KEY (type, code, month)" in _CREATE

def test_insert_targets_fct_monthly_index_with_six_value_columns():
    assert "INSERT INTO gold.fct_monthly_index" in _INSERT
    assert _INSERT.count("%s") == 6
