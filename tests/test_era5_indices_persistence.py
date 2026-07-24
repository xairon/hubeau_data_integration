from hubeau_pipeline.ml import era5_indices_persistence as p


def test_create_and_upsert_include_spei():
    assert "spei" in p._CREATE
    assert "spei = EXCLUDED.spei" in p._UPSERT
    # 7 value placeholders + now()
    assert p._TEMPLATE.count("%s") == 7


def test_alter_adds_spei_idempotently():
    assert "ADD COLUMN IF NOT EXISTS spei" in p._ALTER_ADD_SPEI
