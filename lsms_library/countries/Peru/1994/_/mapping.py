"""Peru ENNIV 1994 formatting functions and per-table df_edit hooks.

Read ``Peru/_/CONTENTS.org`` and this wave's ``data_info.yml`` before editing:
they carry the evidence for every decision made here, quoted from
``dicciona.pdf`` (Data Dictionary: ENNIV94), ``m-encues.pdf`` (MANUAL DEL
ENCUESTADOR) and ``pe94hhq1.pdf`` (the household questionnaire).

Identity (GH #684).  The survey's own universal key -- the items COMMON to
every record type -- is ``(RECTYPE, SEGMENTO, VIVIENDA, HOGAR)``.  The
household is therefore ``(segmento, vivienda, hogar)``, exactly unique across
REG01's 3,623 rows, and the sampling cluster is ``segmento`` ALONE: the
enumerator's manual labels ``Segmento No / Vivienda No`` the "UBICACION
MUESTRAL" and Departamento/Provincia/Distrito/Centro Poblado the "UBICACION
GEOGRAFICA" -- two different boxes copied from one listing.  ``segmento`` is a
national serial (357 of 364 map to a single department), so it is NOT
composited with the department: doing so would split seven real clusters on
single-household transcription typos.

Why this file exists at all: ``i`` is a COMPOSITE idxvar, and ``format_id`` is
auto-applied to every idxvar, so without an ``i()`` of our own it receives a
Series and raises ``ValueError: The truth value of a Series is ambiguous``.
Every other composite-``i`` country (Guyana ``[ED, SN, HH]``, the EHCVM
countries ``[grappe, menage]``) defines the same function.

It is a WAVE-level module, not a country-level one, on purpose: 1985 and 1991
are World-Bank-processed extracts with an entirely different, scalar household
id (``hhid`` / ``hid``), so a country-level ``i()`` expecting a three-part
Series would break them when they are wired.

A module-level function whose name matches a declared ``data_scheme`` table is
dispatched by the framework as that table's ``df_edit`` hook: it receives the
grabbed-and-indexed frame before ``_normalize_dataframe_index`` sees it.  The
two hooks below exist so that every non-uniqueness in this wave is resolved by
an EXPLICIT, declared policy.  Anything still non-unique at normalize time is
collapsed with a silent ``groupby().first()``, which is precisely the GH #323
data loss.
"""
import warnings
from pathlib import Path

import pandas as pd

from lsms_library.local_tools import code_label_map, format_id


# Field widths from dicciona.pdf's COMMON block: SEGMENTO N 3-5 (3),
# VIVIENDA N 6-7 (2), HOGAR N 8-9 (2).
_I_WIDTHS = (3, 2, 2)

# Peru/_/ -- an ABSOLUTE path off this module, not a cwd-relative one: a
# df_edit hook runs in-process with an arbitrary working directory, unlike a
# `make`-driven wave script.
_COUNTRY_DIR = Path(__file__).resolve().parents[2] / '_'


def _labels(tablename):
    """`Code -> Preferred Label` from Peru/_/categorical_mapping.org.

    ``code_label_map`` is used rather than a hand-rolled reader because it
    keys the result by BOTH the string and the integer form of each numeric
    code; a key-type mismatch here is the GhanaLSS failure (GH #372 / #377 /
    #348), where the decode silently resolved to ``{}`` and the column came
    back 100% null with nothing raised.

    That silence is exactly what this function refuses to inherit: an empty
    result raises instead of returning.
    """
    m = code_label_map(tablename, dirs=[str(_COUNTRY_DIR)],
                       value='Preferred Label')
    if not m:
        raise RuntimeError(
            f"Peru 1994: table '{tablename}' not found (or empty) in "
            f"{_COUNTRY_DIR / 'categorical_mapping.org'}. Refusing to return "
            f"an empty decode -- that yields a 100% null column silently."
        )
    return m


def i(value):
    """Composite household id ``SEGMENTO-VIVIENDA-HOGAR``, e.g. ``001-01-11``.

    Zero-padded to the widths the ENNIV data dictionary itself declares for the
    three fields, so the string id sorts in the same order as the numeric key
    (``002-...`` before ``010-...``) rather than lexicographically.

    HOGAR is a two-digit FRACTION, not a sequence number: numerator = this
    household's serial within the dwelling, denominator = the number of
    households in it (``11`` = 1 of 1, ``23`` = 2 of 3).  See data_info.yml for
    the manual's verbatim rule and the check that confirms it holds in 3,622 of
    3,623 rows.
    """
    if len(value) != len(_I_WIDTHS):
        raise ValueError(
            f"Peru 1994 i(): expected {len(_I_WIDTHS)} id parts "
            f"(segmento, vivienda, hogar), got {len(value)}: {list(value)}"
        )
    parts = [format_id(value.iloc[k], zeropadding=_I_WIDTHS[k])
             for k in range(len(value))]
    if any(p is None for p in parts):
        return None
    return '-'.join(parts)


def cluster_features(df):
    """Reduce REG01 (household grain) to the (t, v) segment grain by MODE.

    REG01 is the household cover page: 3,623 rows, 364 segmentos.  Declared at
    (t, v), so it must be projected onto the segment.  Left to the framework
    that is a silent ``groupby().first()`` which destroys 187 of 3,623 rows and
    lets ROW ORDER decide the Region/Rural of every segment whose households
    disagree.

    They disagree in 17 segments, and in each the minority is a SINGLE
    household (two in segmento 125):

      * a01 DPTO varies in  7 of 364 -- and in each of those the rest of the
        geography (provincia, distrito, centro poblado, categoria, area) is
        IDENTICAL across the split while the two departments are not adjacent
        (segmento 106: 11 households in Piura, 1 in Ucayali).
      * a06 AREA varies in 10 of 364 -- minority exactly 1 household in all ten.

    Those are transcription errors in the cover page's "UBICACION GEOGRAFICA"
    box, not segments that really straddle a department.  The DECLARED reducer
    is therefore the MODE, not ``first``: ``first`` would let row order
    sometimes return the mis-keyed value.  Measured: ZERO of the 364 segments
    has a tie in either column, so the mode is unambiguous everywhere in this
    file.  18 household rows carry a Region or Rural that differs from their
    segment's modal value and are thus overruled here; that is the intended
    effect, and it is the reason the count is written down.

    A genuine TIE is NOT guessed: the value becomes ``pd.NA`` (loudly missing)
    and a warning names the segment.

    The codes are decoded to labels HERE, at build time, from the ``Region``
    and ``Rural`` tables in ``Peru/_/categorical_mapping.org`` -- one source of
    truth, referenced rather than copied.  Two reasons it happens here and not
    through the framework's by-name auto-dispatch:

      * ``a01`` arrives ZERO-PADDED (``'06'``) while the org reader parses the
        table's ``Code`` column as int64, stripping the leading zeros.  The
        lookup therefore has to normalise the raw value first; without that
        step it is a SILENT no-op and raw numeric codes reach the user.
      * The auto-dispatch runs at API read time, so the cached parquet would
        keep the bare codes.  ``diagnostics._check_declared_spellings`` grades
        the PARQUET, and a `Rural` column of 1s and 2s fails it -- correctly,
        since anything reading the cache directly sees no labels at all.
    """
    df = df.copy()

    for col in ('Region', 'Rural'):
        if col in df.columns:
            codes = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            decode = _labels(col)
            unknown = sorted({int(k) for k in codes.dropna().unique()
                              if int(k) not in decode})
            if unknown:
                raise RuntimeError(
                    f"Peru 1994 cluster_features: {col} code(s) {unknown} are "
                    f"not in the '{col}' table of "
                    f"{_COUNTRY_DIR / 'categorical_mapping.org'}."
                )
            df[col] = codes.map(lambda k: decode.get(int(k)) if pd.notna(k)
                                else pd.NA).astype('string')

    levels = list(df.index.names)
    grouped = df.groupby(level=levels, observed=True)

    ties: list[str] = []
    overruled = 0                 # household rows whose value != their segment's mode
    conflicting: list[str] = []   # (col, segment) pairs that disagreed at all
    out = {}
    for col in df.columns:
        vals = {}
        for key, s in grouped[col]:
            m = s.dropna().mode()
            if len(m) == 1:
                vals[key] = m.iloc[0]
                n_diff = int((s.dropna() != m.iloc[0]).sum())
                if n_diff:
                    overruled += n_diff
                    conflicting.append(f"{col}@{key}")
            else:
                vals[key] = pd.NA
                if len(m) > 1:
                    ties.append(f"{col}@{key}")
        out[col] = pd.Series(vals)

    res = pd.DataFrame(out)
    res.index = grouped.size().index

    # Report the reduction EVERY build, not just on a tie.  The framework's own
    # GrainCollapse audit cannot see this projection -- the hook runs upstream
    # of `_normalize_dataframe_index`, so moving the collapse here also moved it
    # above the auditor.  Without this the counts would live only in a docstring
    # measured once on 2026-08-21, and a re-pushed REG01 or a changed key would
    # drift silently.  (GH #691 review, N2.)
    n_rows, n_groups = len(df), len(res)
    if overruled or conflicting:
        warnings.warn(
            f"Peru 1994 cluster_features: projected {n_rows} household rows onto "
            f"{n_groups} segments by MODE; {len(conflicting)} (column, segment) "
            f"pair(s) disagreed and {overruled} household value(s) were overruled "
            f"by their segment's majority. Expected as of 2026-08-21: 17 pairs, "
            f"18 values. A CHANGE HERE MEANS THE SOURCE OR THE KEY MOVED -- "
            f"re-read `_/CONTENTS.org` before trusting the result. "
            f"Disagreeing: {', '.join(sorted(conflicting)[:8])}",
            RuntimeWarning,
        )
    if ties:
        warnings.warn(
            f"Peru 1994 cluster_features: {len(ties)} cluster attribute(s) had "
            f"no majority value within the segment and were set to NA rather "
            f"than guessed: {', '.join(sorted(ties)[:8])}",
            RuntimeWarning,
        )
    return res


# B03-ANOS and B08-AUSENTE both use 99 for non-response; see
# ``_null_sentinels`` for the evidence and its residual uncertainty.
_SENTINELS = {'Age': 99, 'MonthsAway': 99}


def household_roster(df):
    """df_edit hook for `household_roster`: sentinels, then the one bad pid.

    Runs before ``_normalize_dataframe_index``, so it is the last chance to
    resolve a non-unique (t, i, pid) explicitly instead of letting
    ``groupby().first()`` silently delete a person.
    """
    df = _null_sentinels(df)
    df = _repair_duplicate_pids(df)
    return df


def _null_sentinels(df):
    """Map the 99 non-response codes in Age / MonthsAway to NA.

    ``dicciona.pdf`` prints 99 as an explicit standalone value -- i.e. not part
    of a range -- for B04D-DIA, B04M-MES, B04A-ANO and B08-AUSENTE.  For
    B03-ANOS it prints "AGNO 00:99" as a plain range, which read literally
    would make 99 a real age.  The data says otherwise:

      * ALL SIX people with b03 = 99 also have b04a (year of birth) = 99, the
        dictionary's own explicit non-response code, and four of them are one
        household's four "otro pariente" members -- a household whose relatives
        were not enumerated;
      * ages 96, 97 and 98 have 1, 1 and 2 people respectively, so the six at
        99 are a spike, not a tail.

    They are mapped to NA.  That is a READING of the source, not a statement in
    it, and the tension with the dictionary's declared range is recorded in
    ``Peru/_/CONTENTS.org`` rather than hidden.

    MonthsAway (B08-AUSENTE, "AUSENTE 00:12" plus a separate 99) has three rows
    at 99; there the dictionary is unambiguous.

    No warning: decoding a documented non-response code is a value mapping, not
    data loss, and it is deterministic.  The affected row counts are written
    down in CONTENTS.org.
    """
    df = df.copy()
    for col, sentinel in _SENTINELS.items():
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors='coerce').astype('Float64')
        df[col] = s.where(s != sentinel, pd.NA)
    return df


def _repair_duplicate_pids(df):
    """Restore B00-ORDEN where the roster duplicated it, or drop if unsure.

    B00-ORDEN is the person's line number on the FICHA DEL HOGAR and is the
    join key every other person-level module uses.  It must be a bijection onto
    1..N within a household.  In REG02 it is, except for ONE household:

        (segmento 81, vivienda 2, hogar 11) -- three people numbered 1, 3, 3.
        Row 1 is the 63-year-old head; rows 2 and 3 are a 61-year-old conyuge
        (b01=2) and a 15-year-old daughter (b01=3).  ORDEN 2 is missing.

    This is not resolved by guesswork.  The same household is numbered 1, 2, 3
    in THREE other person-level modules shipped in the same dataset -- REG05
    (e00), REG07 (g00) and REG08 (h00 / h00a) -- and REG05 records its code-3
    person as attending school (e01=1, e03=1) while codes 1 and 2 are not,
    which matches the 15-year-old being code 3.  The questionnaire's own
    listing order ("A) JEFE DE HOGAR, B) CONYUGE, C) HIJOS SOLTEROS...") and
    the file's row order agree.  The conyuge's ORDEN was mis-keyed 3-for-2.

    Renumbering by row order therefore RESTORES the cross-module join for both
    people.  Guyana's `housing` precedent drops an irreconcilable duplicate
    rather than let ``first()`` pick one, and that fallback is kept below for
    any duplicate that fails the precondition -- but this one IS reconcilable,
    and dropping it would orphan two people's records in four other modules.

    Precondition for repair, checked per household and never assumed:
      * every existing pid parses as a positive integer;
      * pids are non-decreasing in file order (the cedula is filled top-down);
      * renumbering 1..N by row order changes ONLY rows that belong to a
        duplicated-pid group.
    Anything else is dropped with a warning -- loudly missing rather than
    silently wrong.
    """
    if 'pid' not in (df.index.names or []):
        return df
    hh_levels = [n for n in df.index.names if n != 'pid']
    if not hh_levels:
        return df

    flat = df.reset_index()

    repaired: list[str] = []
    dropped: list[str] = []
    drop_labels: list = []

    for key, grp in flat.groupby(hh_levels, sort=False, observed=True):
        pids = [str(p) for p in grp['pid'].tolist()]
        if len(set(pids)) == len(pids):
            continue
        label = '/'.join(str(k) for k in (key if isinstance(key, tuple) else (key,)))
        n = len(pids)
        new = [str(k) for k in range(1, n + 1)]
        dup_mask = pd.Series(pids).duplicated(keep=False).tolist()
        try:
            nums = [int(p) for p in pids]
        except (TypeError, ValueError):
            nums = None
        ok = (
            nums is not None
            and all(x > 0 for x in nums)
            and all(a <= b for a, b in zip(nums, nums[1:]))
            and all(dup_mask[k] for k in range(n) if new[k] != pids[k])
        )
        if ok:
            flat.loc[grp.index, 'pid'] = new
            changed = [f"{pids[k]}->{new[k]}" for k in range(n) if new[k] != pids[k]]
            repaired.append(f"{label} ({', '.join(changed)})")
        else:
            drop_labels.extend(lbl for k, lbl in enumerate(grp.index) if dup_mask[k])
            dropped.append(label)

    if repaired:
        warnings.warn(
            f"Peru 1994 household_roster: restored B00-ORDEN by cedula row "
            f"order for {len(repaired)} household(s) whose roster duplicated a "
            f"person id; the numbering is corroborated by REG05 / REG07 / "
            f"REG08 (see _repair_duplicate_pids): {'; '.join(repaired)}",
            RuntimeWarning,
        )
    if dropped:
        flat = flat.drop(index=drop_labels)
        warnings.warn(
            f"Peru 1994 household_roster: dropped the duplicated rows of "
            f"{len(dropped)} household(s) whose B00-ORDEN could not be "
            f"reconstructed from row order (loudly missing rather than "
            f"silently first()-collapsed -- GH #323): {'; '.join(dropped)}",
            RuntimeWarning,
        )

    out = flat.set_index(list(df.index.names))
    out.attrs = dict(df.attrs)
    return out
