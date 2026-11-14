"""GH #1457: Malawi sales no harvest row holds go on a Farm-level row.

Modules I and Q record a sale per household and crop code, in its own unit and state.
The ladder attaches one to a single-plot crop's harvest row in the same unit.  Every other
sale now has ONE row of its own: ``plot_id='Farm-level'``, no ``Quantity``, ``u='Unknown'``,
``condition='unknown_condition'``, its own ``Unit_sold`` / ``Condition_sold``.

``TestRules`` needs no data.  ``TestOracle`` reads the module files directly and imports
nothing from ``Malawi/_/``.
"""
from __future__ import annotations

import copy
import importlib
import importlib.util
import sys
import warnings

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

MALAWI = countries_root() / "Malawi"
FARM = "Farm-level"
KG, BAG = "Kilogramme", "50 kg Bag"

SALES = {
    "2010-11": [("Data/Full_Sample/Agriculture/ag_mod_i.dta", "ag_i02a", "ag_i03"),
                ("Data/Full_Sample/Agriculture/ag_mod_q.dta", "ag_q02a", "ag_q03")],
    "2013-14": [("Data/AG_MOD_I_13.dta", "ag_i02a", "ag_i03"),
                ("Data/AG_MOD_Q_13.dta", "ag_q02a", "ag_q03")],
    "2016-17": [("Data/Cross_Sectional/ag_mod_i.dta", "ag_i02a", "ag_i03"),
                ("Data/Cross_Sectional/ag_mod_q.dta", "ag_q02a", "ag_q03"),
                ("Data/Panel/ag_mod_i_16.dta", "ag_i02a", "ag_i03"),
                ("Data/Panel/ag_mod_q_16.dta", "ag_q02a", "ag_q03")],
    "2019-20": [("Data/Cross_Sectional/ag_mod_i.dta", "ag_i02a", "ag_i03"),
                ("Data/Cross_Sectional/ag_mod_q.dta", "ag_q02a", "ag_q03"),
                ("Data/Panel/ag_mod_i_19.dta", "ag_i02a", "ag_i03"),
                ("Data/Panel/ag_mod_q_19.dta", "ag_q02a", "ag_q03")],
}


@pytest.fixture(scope="module")
def malawi():
    sys.path.insert(0, str(MALAWI / "_"))
    spec = importlib.util.spec_from_file_location("malawi_1457", MALAWI / "_" / "malawi.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _harvest(rows):
    df = pd.DataFrame(rows, columns=["i", "plot_id", "crop", "_crop_code", "u", "condition", "Quantity"])
    return df.assign(
        t="2010-11", production_obs=lambda d: "reported:G:" + d["_crop_code"].astype(str),
        crop_variety="x", planting_month=pd.array([pd.NA] * len(df), dtype="Int64"),
        harvest_month=pd.array([pd.NA] * len(df), dtype="Int64"),
        intercropped=pd.array([pd.NA] * len(df), dtype="boolean"),
        perennial=pd.array([False] * len(df), dtype="boolean"),
        _crop_code=df["_crop_code"].astype("Int64"), u=df["u"].astype("string"),
        condition=df["condition"].astype("string"), Quantity=df["Quantity"].astype("Float64"))


def _sale(rows):
    df = pd.DataFrame(rows, columns=["i", "_crop_code", "u", "condition_sold", "Quantity_sold", "Value_sold"])
    return df.astype({"_crop_code": "Int64", "u": "string", "condition_sold": "string",
                      "Quantity_sold": "Float64", "Value_sold": "Float64"})


def _assemble(malawi, harvest, sale):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return malawi.assemble_crop_production("2010-11", [_harvest(harvest)], [_sale(sale)])


class TestRules:

    def test_a_multi_plot_sale_is_one_farm_level_row(self, malawi):
        out = _assemble(malawi,
                        [("h", "R1", "Maize", 1, KG, "shelled", 10.0), ("h", "R2", "Maize", 1, KG, "shelled", 10.0)],
                        [("h", 1, KG, "shelled", 3.0, 300.0)])
        farm = out.xs(FARM, level="plot_id")
        assert len(farm) == 1
        row = farm.iloc[0]
        assert (row.Quantity_sold, row.Value_sold, row.Unit_sold, row.Condition_sold) == (3.0, 300.0, KG, "shelled")
        assert pd.isna(row.Quantity) and row.u == "Unknown" and row.condition == "unknown_condition"
        assert row.production_obs == "reported:I:1" and pd.isna(row.Derivation)
        assert out[out.index.get_level_values("plot_id") != FARM]["Value_sold"].isna().all()
        assert out.attrs["sale_farm_level"]["by_cause"]["no_candidate_row"]["sales"] == 1

    def test_the_ladder_keeps_what_it_attached_and_states_the_sale(self, malawi):
        out = _assemble(malawi, [("h", "R1", "Maize", 1, KG, "shelled", 10.0)],
                        [("h", 1, KG, "shelled", 3.0, 300.0)])
        assert len(out) == 1
        row = out.iloc[0]
        assert (row.Value_sold, row.Unit_sold, row.Condition_sold) == (300.0, KG, "shelled")
        assert pd.isna(row.Derivation)

    def test_one_row_in_another_unit_goes_to_farm_level_unmoved(self, malawi):
        """User approved 2026-11-10: on the row, in its own Unit_sold, keyed."""
        out = _assemble(malawi, [("h", "R1", "Maize", 1, BAG, "shelled", 10.0)],
                        [("h", 1, KG, "shelled", 3.0, 300.0)])
        assert FARM not in out.index.get_level_values("plot_id")
        row = out.iloc[0]
        assert (row.Value_sold, row.Quantity_sold, row.Unit_sold) == (300.0, 3.0, KG)
        assert row.u == BAG and row.Quantity == 10.0
        assert row.Derivation == "Malawi::crop_production::sale-other-unit-single-row"

    def test_a_refused_basis_goes_to_farm_level_in_its_own_state(self, malawi):
        out = _assemble(malawi, [("h", "R1", "Groundnut", 11, KG, "shelled", 10.0)],
                        [("h", 11, KG, "unshelled", 3.0, 300.0)])
        assert out.loc[out.index.get_level_values("plot_id") == "R1", "Value_sold"].isna().all()
        assert out.xs(FARM, level="plot_id")["Condition_sold"].tolist() == ["unshelled"]
        assert out.attrs["sale_basis_mismatch"]["sales"] == 1

    def test_two_records_of_one_code_keep_two_rows_and_no_sum(self, malawi):
        out = _assemble(malawi,
                        [("h", "R1", "Maize", 1, BAG, "shelled", 10.0), ("h", "R2", "Maize", 1, BAG, "shelled", 10.0)],
                        [("h", 1, KG, "shelled", 3.0, 300.0), ("h", 1, "Bale", "shelled", 1.0, 50.0)])
        farm = out.xs(FARM, level="plot_id")
        assert sorted(farm["Value_sold"]) == [50.0, 300.0]
        assert sorted(farm["production_obs"]) == ["reported:I:1/row:0", "reported:I:1/row:1"]

    def test_a_did_not_sell_line_serves_nothing_new(self, malawi):
        out = _assemble(malawi, [("h", "R1", "Maize", 1, KG, "shelled", 10.0), ("h", "R2", "Maize", 1, KG, "shelled", 5.0)],
                        [("h", 1, pd.NA, "unknown_condition", 0.0, 0.0)])
        assert FARM not in out.index.get_level_values("plot_id")

    def test_a_perennial_sale_is_named_for_its_module(self, malawi):
        out = _assemble(malawi, [("h", "R1", "Mango", 1004, KG, "unknown_condition", 10.0),
                                 ("h", "R2", "Mango", 1004, KG, "unknown_condition", 5.0)],
                        [("h", 1004, KG, "unknown_condition", 1.0, 10.0)])
        row = out.xs(FARM, level="plot_id").iloc[0]
        assert row.production_obs == "reported:Q:4" and row.perennial == True  # noqa: E712

    def test_a_sale_on_a_sentinel_plot_is_not_on_it(self, malawi):
        out = _assemble(malawi, [("h", "unknown_plot", "Mango", 1004, KG, "unknown_condition", 10.0)],
                        [("h", 1004, KG, "unknown_condition", 1.0, 3600.0)])
        assert out.loc[out.index.get_level_values("plot_id") == "unknown_plot", "Value_sold"].isna().all()
        assert out.xs(FARM, level="plot_id")["Value_sold"].tolist() == [3600.0]


@pytest.fixture(scope="module")
def served():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return ll.Country("Malawi").crop_production().reset_index()


def _source(wave):
    value = quantity = 0.0
    n = 0
    for name, qty, val in SALES[wave]:
        raw = get_dataframe(str(MALAWI / wave / name), convert_categoricals=False)
        raw = raw[~raw.duplicated(keep="first")]       # exact repeats (IHS3 2010-11)
        q = pd.to_numeric(raw[qty], errors="coerce").astype("float64")
        v = pd.to_numeric(raw[val], errors="coerce").astype("float64")
        value += float(v.sum())
        quantity += float(q.sum())
        n += int(((q > 0) | (v > 0)).sum())
    return value, quantity, n


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", list(SALES))
class TestOracle:

    def test_value_and_quantity_equal_source(self, served, wave):
        value, quantity, _ = _source(wave)
        g = served[served.t == wave]
        assert float(g.Value_sold.sum()) == pytest.approx(value, abs=1e-6)
        assert float(g.Quantity_sold.sum()) == pytest.approx(quantity, abs=1e-6)

    def test_a_farm_level_row_states_only_a_sale(self, served, wave):
        g = served[(served.t == wave) & (served.plot_id == FARM)]
        assert len(g)
        assert g.Quantity.isna().all() and g.u.eq("Unknown").all() and g.condition.eq("unknown_condition").all()
        assert ((g.Quantity_sold > 0) | (g.Value_sold > 0)).all()
        key = "Malawi::crop_production::sale-basis-unresolved"
        refused = {"2010-11": 94, "2013-14": 21, "2016-17": 89, "2019-20": 137}[wave]
        # GH #1228 ("Register all waves", @ligon 2026-11-13): a Farm-level code-48 sale
        # that took its household's cowpea label also carries the crop-text key, alone or
        # '+'-joined beside the ladder's; it marks the label, so it is set aside here.
        label = "Malawi::crop_production::cowpea-from-crop-text"
        keys = g.Derivation.astype("string").str.split("+")
        ladder = keys.map(lambda ks: [k for k in ks if k != label] if isinstance(ks, list) else [])
        assert [k for ks in ladder for k in ks] == [key] * refused
        assert int(ladder.map(len).eq(0).sum()) == len(g) - refused
        labelled = keys.map(lambda ks: isinstance(ks, list) and label in ks).to_numpy(dtype=bool)
        assert (labelled == g.crop.astype(str).eq("Cowpea (Khobwe)").to_numpy(dtype=bool)).all()
        assert g.production_obs.str.fullmatch(r"reported:[IQ]:\d+(/row:\d+)?").all()

    def test_a_sale_row_says_its_unit_and_a_harvest_rows_is_its_u(self, served, wave):
        g = served[(served.t == wave) & (served.plot_id != FARM)]
        sold = (g.Quantity_sold.astype(float) > 0) | (g.Value_sold.astype(float) > 0)
        assert g.loc[~sold, "Unit_sold"].isna().all() and g.loc[~sold, "Condition_sold"].isna().all()
        key = g.Derivation.astype("string").str.split('+').apply(
            lambda keys: "Malawi::crop_production::sale-other-unit-single-row" in keys
            if isinstance(keys, list) else False)
        known = sold & g.u.notna() & ~key
        assert (g.loc[known, "Unit_sold"] == g.loc[known, "u"]).all()
        assert (g.loc[key, "Unit_sold"] != g.loc[key, "u"]).all()

    def test_no_sentinel_row_holds_a_sale(self, served, wave):
        g = served[(served.t == wave) & (served.plot_id == "unknown_plot")]
        assert not (g.Value_sold.astype(float) > 0).any()


class TestReview:
    """F3 and F4 of the #1457 review."""

    def test_a_sale_takes_its_households_label_for_the_code(self, malawi):
        """Code 48 beside lines whose text says cowpea is not a second crop (GH #1228)."""
        cowpea = "Cowpea (Khobwe)"
        out = _assemble(malawi, [("h", "R1", cowpea, 48, KG, "unknown_condition", 5.0),
                                 ("h", "R2", cowpea, 48, KG, "unknown_condition", 5.0)],
                        [("h", 48, KG, "unknown_condition", 1.0, 10.0)])
        assert out.xs(FARM, level="plot_id").index.get_level_values("crop").tolist() == [cowpea]

    def test_two_labels_on_a_households_code_fall_back_to_the_table(self, malawi):
        out = _assemble(malawi, [("h", "R1", "Cowpea (Khobwe)", 48, KG, "unknown_condition", 5.0),
                                 ("h", "R2", "Other", 48, KG, "unknown_condition", 5.0)],
                        [("h", 48, KG, "unknown_condition", 1.0, 10.0)])
        assert out.xs(FARM, level="plot_id").index.get_level_values("crop").tolist() == ["Other"]

    def test_a_blank_cell_stays_blank_on_a_farm_level_row_only(self, malawi):
        # What _sale_block returns for a line with a value and no quantity: sum 0, none recorded.
        sale = _sale([("h", 1, KG, "shelled", 0.0, 300.0), ("g", 1, KG, "shelled", 0.0, 300.0)]).assign(
            _from_module_q=False, _quantity_recorded=False, _value_recorded=True)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = malawi.assemble_crop_production(
                "2010-11", [_harvest([("h", "R1", "Maize", 1, KG, "shelled", 10.0),
                                      ("h", "R2", "Maize", 1, KG, "shelled", 10.0),
                                      ("g", "R1", "Maize", 1, KG, "shelled", 10.0)])], [sale])
        assert out.xs(FARM, level="plot_id")["Quantity_sold"].isna().all()
        on_row = out[out.index.get_level_values("i") == "g"]
        assert on_row["Quantity_sold"].tolist() == [0.0] and on_row["Value_sold"].tolist() == [300.0]


# R3 provenance controls use invented production-typed pieces; no native reader.
def _r3_frames(sales=None, harvests=None, raws=None):
    hs = harvests or [('h', 'R1', 'Groundnut', 11, KG, 'shelled', 10.)]
    ss = sales if sales is not None else [('h', 11, KG, 'unshelled', 3., 300.)]
    h, s = _harvest(hs), _sale(ss)
    h['i'] = h['i'].astype('string')
    s['i'] = s['i'].astype('string')
    rows = []
    for n, (i, code, u, condition, quantity, value) in enumerate(raws if raws is not None else ss):
        rows.append(dict(t='2010-11', i=i, crop='Groundnut' if code == 11 else 'Other',
                         u=u, condition_sold=condition, _crop_code=code, module='I', half='full',
                         case_id='native-' + str(i), ag_i0b=code, ag_i02a=quantity, ag_i02b=1,
                         ag_i02c=2, ag_i03=value, source_line=n))
    raw = pd.DataFrame(rows).astype({'i': 'string', 'crop': 'string', 'u': 'string',
                                    'condition_sold': 'string', '_crop_code': 'Int64'})
    return [h], [s], raw


def _r3_inputs(malawi, pieces, monkeypatch, *, prior=False):
    calls = []
    def supply(wave):
        calls.append(wave)
        return copy.deepcopy(pieces)
    monkeypatch.setattr(malawi, '_crop_pieces', supply)
    if prior:
        out = malawi._inputs_sale_basis('2010-11', malawi.SALE_UNRESOLVED)
    else:
        out = malawi.inputs_sale_basis_unresolved('2010-11')
    assert calls == ['2010-11']
    return out


def _r3_assembled(malawi, pieces):
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter('always')
        out = malawi.assemble_crop_production('2010-11', copy.deepcopy(pieces[0]),
                                             copy.deepcopy(pieces[1]))
    return out, seen


class TestR3Inputs:
    @pytest.mark.parametrize('kind', ['refusal', 'multi', 'otherunit', 'ambiguous',
                                     'exact', 'wildcard', 'nonsale'])
    def test_only_refused_served_rows_gain_key(self, malawi, kind):
        hs = [('h', 'R1', 'Groundnut', 11, KG, 'shelled', 10.)]
        ss = [('h', 11, KG, 'unshelled', 3., 300.)]
        if kind == 'multi':
            hs.append(('h', 'R2', 'Groundnut', 11, KG, 'shelled', 5.))
        if kind == 'otherunit':
            ss = [('h', 11, BAG, 'unshelled', 3., 300.)]
        if kind == 'ambiguous':
            hs.append(('h', 'R1', 'Groundnut', 11, KG, 'unshelled', 5.))
            ss = [('h', 11, KG, 'unknown_condition', 3., 300.)]
        if kind == 'exact':
            ss = [('h', 11, KG, 'shelled', 3., 300.)]
        if kind == 'wildcard':
            ss = [('h', 11, KG, 'unknown_condition', 3., 300.)]
        if kind == 'nonsale':
            ss = [('h', 11, KG, 'unshelled', 0., 0.)]
        pieces = _r3_frames(ss, hs)
        original = copy.deepcopy(pieces)
        out, seen = _r3_assembled(malawi, pieces)
        farm = out.index.get_level_values('plot_id') == FARM
        if kind in ('refusal', 'multi', 'otherunit', 'ambiguous'):
            row = out[farm].iloc[0]
            assert row.Quantity_sold == 3. and row.Value_sold == 300.
            assert row.Unit_sold == ss[0][2] and row.Condition_sold == ss[0][3]
            assert pd.isna(row.Quantity) and row.u == 'Unknown' and row.condition == 'unknown_condition'
            assert row.production_obs == 'reported:I:11'
            assert out[~farm].Value_sold.isna().all()
        else:
            assert not farm.any()
        expected = ({malawi.SALE_BASIS_UNRESOLVED: 1} if kind == 'refusal' else
                    {malawi.SALE_BASIS_WILDCARD: 1} if kind == 'wildcard' else {})
        assert out.Derivation.dropna().value_counts().to_dict() == expected
        if kind == 'refusal':
            assert out.attrs['sale_basis_mismatch'] == {'wave': '2010-11', 'sales': 1, 'rows': 1, 'value': 300.}
            assert any(isinstance(w.message, malawi.SaleAttachmentWarning) for w in seen)
        if kind == 'ambiguous':
            assert out.attrs['sale_suppressed']['sales'] == 1
        for before, after in zip(original, pieces):
            if isinstance(before, list):
                for left, right in zip(before, after):
                    pd.testing.assert_frame_equal(left, right, check_exact=True)
            else:
                pd.testing.assert_frame_equal(before, after, check_exact=True)

    def test_raw_multiplicity_annotations_and_exact_payload_preserved(self, malawi, monkeypatch):
        pieces = _r3_frames(raws=[('h', 11, KG, 'unshelled', 1., 100.),
                                 ('h', 11, KG, 'unshelled', 2., 200.)])
        prior = _r3_inputs(malawi, pieces, monkeypatch, prior=True).reset_index()
        result = _r3_inputs(malawi, pieces, monkeypatch).reset_index()
        assert len(result) == 2 and result.production_obs.tolist() == ['reported:I:11'] * 2
        pd.testing.assert_frame_equal(prior, result[prior.columns], check_exact=True)
        assert {'outcome', 'rung', 'basis'} <= set(result)
        assert result.source_line.tolist() == [0, 1] and result.ag_i02a.tolist() == [1., 2.]
        mutated = result[prior.columns].copy()
        mutated.loc[0, 'ag_i02a'] = np.nextafter(1., 2.)
        with pytest.raises(AssertionError):
            pd.testing.assert_frame_equal(prior, mutated, check_exact=True)

    def test_suffix_generated_over_full_farm_set(self, malawi, monkeypatch):
        pieces = _r3_frames(sales=[('h', 11, KG, 'unshelled', 3., 300.),
                                  ('h', 11, 'Bale', 'unshelled', 1., 50.)])
        farm = _r3_assembled(malawi, pieces)[0].xs(FARM, level='plot_id')
        result = _r3_inputs(malawi, pieces, monkeypatch).reset_index()
        assert result.production_obs.tolist() == ['reported:I:11/row:1']
        assert set(farm.production_obs) == {'reported:I:11/row:0', 'reported:I:11/row:1'}
        assert farm.loc[farm.Derivation.eq(malawi.SALE_BASIS_UNRESOLVED), 'production_obs'].tolist() == result.production_obs.tolist()

    def test_served_own_crop_preserves_native_code(self, malawi, monkeypatch):
        pieces = _r3_frames(sales=[('h', 48, KG, 'unshelled', 3., 300.)],
                            harvests=[('h', 'R1', 'Cowpea (Khobwe)', 48, KG, 'shelled', 10.)])
        result = _r3_inputs(malawi, pieces, monkeypatch).reset_index()
        assert result.crop.tolist() == ['Cowpea (Khobwe)'] and result.ag_i0b.tolist() == [48]
        assert pieces[2].crop.tolist() == ['Other']

    def test_zero_zero_input_retained_without_served_observation(self, malawi, monkeypatch):
        pieces = _r3_frames(sales=[('h', 11, KG, 'unshelled', 0., 0.)])
        prior = _r3_inputs(malawi, pieces, monkeypatch, prior=True).reset_index()
        result = _r3_inputs(malawi, pieces, monkeypatch).reset_index()
        assert len(prior) == len(result) == 1 and result.production_obs.isna().all()
        pd.testing.assert_frame_equal(prior, result[prior.columns], check_exact=True)
        assert FARM not in _r3_assembled(malawi, pieces)[0].index.get_level_values('plot_id')

    def test_empty_selection_has_typed_named_index(self, malawi, monkeypatch):
        out = _r3_inputs(malawi, _r3_frames(sales=[('h', 11, KG, 'shelled', 3., 300.)]), monkeypatch)
        assert out.empty and out.index.names == ['t', 'i', 'crop', 'production_obs']
        assert str(out.index.get_level_values('production_obs').dtype) == 'string'

    @pytest.mark.parametrize('missing', ['u', 'condition_sold', 'crop', 'i'])
    def test_nullable_join_keys_preserve_prior_lines(self, malawi, monkeypatch, missing):
        pieces = _r3_frames(raws=[('h', 11, KG, 'unshelled', 1., 100.),
                                 ('h', 11, KG, 'unshelled', 2., 200.)])
        h, s, raw = pieces
        harv, dec, _ = malawi._attach_sales('2010-11', malawi._consolidate_harvest(h), pd.concat(s, ignore_index=True))
        if missing in ('u', 'condition_sold', 'i'):
            raw[missing] = pd.array([pd.NA] * len(raw), dtype='string')
            dec[missing] = pd.array([pd.NA] * len(dec), dtype='string')
        else:
            raw[missing] = pd.NA
        monkeypatch.setattr(malawi, '_attach_sales', lambda *a: (harv.copy(), dec.copy(), {}))
        prior = _r3_inputs(malawi, pieces, monkeypatch, prior=True).reset_index()
        result = _r3_inputs(malawi, pieces, monkeypatch).reset_index()
        assert len(prior) == len(result) == 2
        if missing == 'crop':
            assert result.crop.tolist() == ['Groundnut'] * 2
        else:
            pd.testing.assert_frame_equal(prior, result[prior.columns], check_exact=True)

    @pytest.mark.parametrize('join', ['decision', 'served'])
    def test_duplicate_right_key_refuses_cardinality(self, malawi, monkeypatch, join):
        pieces = _r3_frames()
        if join == 'decision':
            attach = malawi._attach_sales
            def duplicate(*args):
                harv, dec, tally = attach(*args)
                return harv, pd.concat([dec, dec], ignore_index=True), tally
            monkeypatch.setattr(malawi, '_attach_sales', duplicate)
        else:
            farm = malawi._farm_level_sales
            def duplicate(*args):
                frame, tally = farm(*args)
                return pd.concat([frame, frame], ignore_index=True), tally
            monkeypatch.setattr(malawi, '_farm_level_sales', duplicate)
        with pytest.raises(pd.errors.MergeError, match='not a many-to-one merge'):
            _r3_inputs(malawi, pieces, monkeypatch)

    def test_shared_loader_keeps_duplicate_missing_observation(self, monkeypatch):
        d = importlib.import_module('lsms_library.derivations')
        from types import SimpleNamespace
        from lsms_library.local_tools import id_walk
        index = pd.MultiIndex.from_tuples([('2010-11', 'h', 'Groundnut', pd.NA)] * 2,
                                          names=['t', 'i', 'crop', 'production_obs'])
        original = pd.DataFrame({'ag_i02a': [0., 0.], 'source_line': [0, 1]}, index=index)
        monkeypatch.setattr(d, 'resolve_callable', lambda spec: lambda wave: original.copy())
        out = d.call_inputs(SimpleNamespace(inputs='invented:fn', waves=['2010-11']), wave='2010-11')
        pd.testing.assert_frame_equal(original, out, check_exact=True)
        walked = id_walk(out, {'2010-11': {'h': 'canonical'}}, state='raw')
        assert len(walked) == 2 and walked.index.get_level_values('production_obs').isna().all()
        assert walked.index.get_level_values('i').tolist() == ['canonical'] * 2

    def test_full_frame_summary_counts_refusal_and_other_deletions(self, malawi):
        d = importlib.import_module('lsms_library.derivations')
        frame = _r3_assembled(malawi, _r3_frames())[0]
        key = malawi.SALE_BASIS_UNRESOLVED
        summary = d.summary_for_frame(frame, 'Malawi', 'crop_production')
        assert summary[key]['rows'] == 1 and d.derivation_records('Malawi')[key].rows is None
        assert d.derivation_records('Malawi')['Malawi::plot_inputs::nonseed-group-no-payload'].rows == 0


class TestR1OwnUnit:
    @pytest.mark.parametrize('harvest_state,sale_state', [
        ('shelled', 'shelled'), ('unknown_condition', 'unshelled'),
        ('shelled', 'unknown_condition')])
    def test_compatible_state_keeps_own_sale_unit_and_money(self, malawi, harvest_state, sale_state):
        pieces = _r3_frames(sales=[('h', 11, BAG, sale_state, 3., 300.)],
                            harvests=[('h', 'R1', 'Groundnut', 11, KG, harvest_state, 10.)])
        before = copy.deepcopy(pieces)
        out, _ = _r3_assembled(malawi, pieces)
        assert len(out) == 1 and FARM not in out.index.get_level_values('plot_id')
        row = out.iloc[0]
        assert (row.Quantity, row.u, row.condition) == (10., KG, harvest_state)
        assert (row.Quantity_sold, row.Value_sold, row.Unit_sold, row.Condition_sold) == (3., 300., BAG, sale_state)
        assert row.Derivation == malawi.SALE_OTHER_UNIT
        pd.testing.assert_frame_equal(before[2], pieces[2], check_exact=True)

    @pytest.mark.parametrize('state', ['unshelled', 'shell_not_applicable'])
    def test_contradictory_basis_stays_farm_level(self, malawi, state):
        out = _assemble(malawi, [('h', 'R1', 'Groundnut', 11, KG, 'shelled', 10.)],
                        [('h', 11, BAG, state, 3., 300.)])
        assert out.loc[out.index.get_level_values('plot_id') == 'R1', 'Value_sold'].isna().all()
        row = out.xs(FARM, level='plot_id').iloc[0]
        assert (row.Quantity_sold, row.Value_sold, row.Unit_sold, row.Condition_sold) == (3., 300., BAG, state)
        assert pd.isna(row.Derivation)

    @pytest.mark.parametrize('other_quantity', [0., pd.NA])
    def test_multiple_rows_do_not_select_the_positive_row(self, malawi, other_quantity):
        out = _assemble(malawi,
                        [('h', 'R1', 'Groundnut', 11, KG, 'shelled', 10.),
                         ('h', 'R2', 'Groundnut', 11, KG, 'shelled', other_quantity)],
                        [('h', 11, BAG, 'shelled', 3., 300.)])
        assert out.xs(FARM, level='plot_id')['Value_sold'].tolist() == [300.]
        assert out.loc[out.index.get_level_values('plot_id') != FARM, 'Value_sold'].isna().all()

    def test_two_rows_on_same_plot_are_not_one_native_row(self, malawi):
        out = _assemble(malawi,
                        [('h', 'R1', 'Groundnut', 11, KG, 'shelled', 10.),
                         ('h', 'R1', 'Groundnut', 11, 'Bale', 'shelled', 5.)],
                        [('h', 11, BAG, 'shelled', 3., 300.)])
        assert out.xs(FARM, level='plot_id')['Value_sold'].tolist() == [300.]

    @pytest.mark.parametrize('plot', ['unknown_plot', pd.NA])
    def test_unknown_or_missing_plot_does_not_qualify(self, malawi, plot):
        out = _assemble(malawi, [('h', plot, 'Groundnut', 11, KG, 'shelled', 10.)],
                        [('h', 11, BAG, 'shelled', 3., 300.)])
        assert out.xs(FARM, level='plot_id')['Value_sold'].tolist() == [300.]
        assert malawi.SALE_OTHER_UNIT not in out.Derivation.dropna().tolist()

    def test_two_sales_compete_without_reducing_money(self, malawi):
        out = _assemble(malawi, [('h', 'R1', 'Groundnut', 11, KG, 'shelled', 10.)],
                        [('h', 11, BAG, 'shelled', 3., 300.),
                         ('h', 11, 'Bale', 'shelled', 1., 50.)])
        assert sorted(out.xs(FARM, level='plot_id')['Value_sold'].tolist()) == [50., 300.]
        assert out.loc[out.index.get_level_values('plot_id') == 'R1', 'Value_sold'].isna().all()
        assert out.Value_sold.sum() == 350.

    @pytest.mark.parametrize('occupied_quantity,occupied_value', [(1., 900.), (0., 0.)])
    def test_occupied_row_is_never_overwritten(self, malawi, occupied_quantity, occupied_value):
        out = _assemble(malawi, [('h', 'R1', 'Groundnut', 11, KG, 'shelled', 10.)],
                        [('h', 11, KG, 'shelled', occupied_quantity, occupied_value),
                         ('h', 11, BAG, 'shelled', 3., 300.)])
        assert out.loc[out.index.get_level_values('plot_id') == 'R1', 'Value_sold'].tolist() == [occupied_value]
        assert out.xs(FARM, level='plot_id')['Value_sold'].tolist() == [300.]
        assert out.Value_sold.sum() == occupied_value + 300.

    def test_same_harmonized_label_does_not_merge_native_codes(self, malawi):
        out = _assemble(malawi,
                        [('h', 'R1', 'Groundnut', 11, KG, 'shelled', 10.),
                         ('h', 'R2', 'Groundnut', 12, KG, 'shelled', 5.)],
                        [('h', 11, BAG, 'shelled', 3., 300.),
                         ('h', 12, BAG, 'shelled', 2., 250.)])
        assert FARM not in out.index.get_level_values('plot_id')
        assert out.reset_index().set_index('plot_id').Value_sold.to_dict() == {'R1': 300., 'R2': 250.}
        assert out.Derivation.tolist() == [malawi.SALE_OTHER_UNIT] * 2

    def test_r1_and_r3_replay_same_current_full_farm_set(self, malawi, monkeypatch):
        pieces = _r3_frames(sales=[('h', 11, BAG, 'shelled', 3., 300.),
                                  ('h', 11, KG, 'unshelled', 2., 200.)])
        snapshot = copy.deepcopy(pieces)
        out, _ = _r3_assembled(malawi, pieces)
        farm = out.xs(FARM, level='plot_id')
        assert farm.production_obs.tolist() == ['reported:I:11']
        assert farm.Derivation.tolist() == [malawi.SALE_BASIS_UNRESOLVED]
        assert out.loc[out.index.get_level_values('plot_id') == 'R1', 'Derivation'].tolist() == [malawi.SALE_OTHER_UNIT]
        assert out.Value_sold.sum() == 500. and out.Quantity_sold.sum() == 5.
        refusal = _r3_inputs(malawi, pieces, monkeypatch).reset_index()
        assert refusal.production_obs.tolist() == farm.production_obs.tolist()
        assert refusal.u.tolist() == [KG] and refusal.ag_i03.tolist() == [200.]
        monkeypatch.setattr(malawi, '_crop_pieces', lambda wave: copy.deepcopy(pieces))
        r1 = malawi.inputs_sale_other_unit_single_row('2010-11').reset_index()
        plot = out[out.index.get_level_values('plot_id') == 'R1'].reset_index()
        assert r1[['t', 'i', 'crop', 'production_obs']].to_dict('records') == plot[['t', 'i', 'crop', 'production_obs']].to_dict('records')
        assert r1.u.tolist() == [BAG] and r1.ag_i03.tolist() == [300.]
        assert r1.outcome.tolist() == [malawi.SALE_OTHER_UNIT]
        for left, right in zip(snapshot[0] + snapshot[1] + [snapshot[2]], pieces[0] + pieces[1] + [pieces[2]]):
            pd.testing.assert_frame_equal(left, right, check_exact=True)

    @pytest.mark.parametrize('code,crop', [(11, 'Groundnut'), (48, 'Cowpea (Khobwe)'), (1004, 'Mango')])
    def test_inputs_preserve_native_lines_and_served_own_crop(self, malawi, monkeypatch, code, crop):
        pieces = _r3_frames(sales=[('h', code, BAG, 'shelled', 3., 300.)],
                            harvests=[('h', 'R1', crop, code, KG, 'shelled', 10.)],
                            raws=[('h', code, BAG, 'shelled', 1., 100.),
                                  ('h', code, BAG, 'shelled', 2., 200.)])
        raw = pieces[2].copy(deep=True)
        monkeypatch.setattr(malawi, '_crop_pieces', lambda wave: copy.deepcopy(pieces))
        result = malawi.inputs_sale_other_unit_single_row('2010-11').reset_index()
        assert result.crop.tolist() == [crop] * 2
        assert result.production_obs.tolist() == ['reported:G:' + str(code)] * 2
        pd.testing.assert_frame_equal(raw.drop(columns='crop'), result[raw.columns].drop(columns='crop'), check_exact=True)
        assert {'outcome', 'rung', 'basis'} <= set(result)
        mutated = result.copy()
        mutated.loc[0, 'ag_i02a'] = np.nextafter(1., 2.)
        with pytest.raises(AssertionError):
            pd.testing.assert_frame_equal(raw.drop(columns='crop'), mutated[raw.columns].drop(columns='crop'), check_exact=True)
        pd.testing.assert_frame_equal(raw, pieces[2], check_exact=True)

    def test_registry_resolves_r1_without_a_new_deletion(self, malawi):
        d = importlib.import_module('lsms_library.derivations')
        rec = d.derivation_records('Malawi')[malawi.SALE_OTHER_UNIT]
        assert rec.rows is None and rec.columns == ['Quantity_sold', 'Value_sold', 'Unit_sold', 'Condition_sold']
        assert d.resolve_callable(rec.function).__name__ == 'derive_other_unit_sale'
        assert d.resolve_callable(rec.inputs).__name__ == 'inputs_sale_other_unit_single_row'


    def test_unmapped_harvest_inputs_do_not_fabricate_served_coordinate(self, malawi, monkeypatch):
        pieces = _r3_frames(sales=[('h', 999, BAG, 'shelled', 3., 300.)],
                            harvests=[('h', 'R1', pd.NA, 999, KG, 'shelled', 10.)])
        pieces[2]['crop'] = pd.array([pd.NA], dtype='string')
        before = pieces[2].copy(deep=True)
        monkeypatch.setattr(malawi, '_crop_pieces', lambda wave: copy.deepcopy(pieces))
        result = malawi.inputs_sale_other_unit_single_row('2010-11').reset_index()
        assert len(result) == 1 and result.production_obs.isna().all()
        pd.testing.assert_frame_equal(before, result[before.columns], check_exact=True)
        out, _ = _r3_assembled(malawi, pieces)
        assert out.xs(FARM, level='plot_id')['Value_sold'].tolist() == [300.]
        assert out.Derivation.isna().all()


class TestR1ReportedUnitConstraint:
    @pytest.mark.parametrize('native_unit', [pd.NA, 13], ids=['missing-sale-unit', 'reported-other-unit'])
    def test_actual_sale_block_assembly_preserves_unit_without_borrowing(self, malawi, native_unit):
        from lsms_library.transformations import _sold_kg_factors
        raw = pd.DataFrame({'hhid': pd.array(['h'], dtype='string'),
                            'crop_code': pd.array([11], dtype='Int64'),
                            'sold': [1], 'quantity': pd.array([3.], dtype='Float64'),
                            'value': pd.array([300.], dtype='Float64'),
                            'unit': pd.array([native_unit], dtype='Int64'), 'state': [1]})
        raw.attrs = {'invented': ('native-sale', False)}
        before = copy.deepcopy(raw)
        sale = malawi._sale_block(raw, hhid='hhid', cropcode='crop_code', sold_flag='sold',
                                  qty_sold='quantity', value_sold='value', unit_sold='unit',
                                  condition='state')
        if pd.isna(native_unit):
            assert sale.u.isna().all()
        else:
            assert sale.u.tolist() == [malawi._malawi_code_map('harmonize_crop_unit')[13]]
        harvest = _harvest([('h', 'R1', 'Groundnut', 11, KG, 'shelled', 10.)])
        harvest['i'] = harvest['i'].astype('string')
        with warnings.catch_warnings():
            warnings.simplefilter('error', malawi.SaleAttachmentWarning)
            out = malawi.assemble_crop_production('2010-11', [harvest], [sale])
        assert out.Value_sold.sum() == 300. and out.Quantity_sold.sum() == 3.
        if pd.isna(native_unit):
            farm = out.xs(FARM, level='plot_id')
            assert farm.Unit_sold.isna().all() and farm.u.tolist() == ['Unknown']
            assert farm.Derivation.isna().all()
            assert out.loc[out.index.get_level_values('plot_id') == 'R1', 'Value_sold'].isna().all()
            factor, counts = _sold_kg_factors(farm)
            assert factor.isna().all()
            assert (counts['harvest_fallback_attempted'], counts['harvest_fallback'], counts['unresolved']) == (1, 0, 1)
            # Independent consumer counterexample: placing this NA sale on Kg
            # would enable the unchanged fallback that R1 must not manufacture.
            misplaced = farm.assign(u=KG)
            borrowed, counter = _sold_kg_factors(misplaced)
            assert borrowed.tolist() == [1.] and counter['harvest_fallback'] == 1
        else:
            assert FARM not in out.index.get_level_values('plot_id')
            assert out.Unit_sold.tolist() == sale.u.tolist() and out.u.tolist() == [KG]
            assert out.Derivation.tolist() == [malawi.SALE_OTHER_UNIT]
            factor, counts = _sold_kg_factors(out)
            assert factor.isna().all()
            assert (counts['harvest_fallback_attempted'], counts['harvest_fallback'], counts['unresolved']) == (0, 0, 1)
        pd.testing.assert_frame_equal(raw, before, check_exact=True)
        assert raw.attrs == before.attrs and type(raw.attrs['invented']) is tuple
