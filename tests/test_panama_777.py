"""Panama code 77 denotes money, so its echoed quantity is not physical (#777)."""
import pandas as pd
import pytest

from lsms_library import Country
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

pytestmark = pytest.mark.requires_s3


def test_valor_quantities_are_absent_but_expenditure_is_preserved():
    food = Country("Panama").food_acquired().xs("1997", level="t")
    valor = food.loc[food.index.get_level_values("u") == "value"]
    assert len(valor) == 10614
    assert valor.Quantity.isna().all()
    assert set(valor.index.get_level_values("s")) == {"purchased"}
    raw = get_dataframe(countries_root() / "Panama/1997/Data/GAST-A.DTA",
                        convert_categoricals=False)
    expenditure = pd.to_numeric(raw.ga106c, errors="coerce")
    expected = expenditure.loc[expenditure.lt(9999)].sum()
    assert expected == pytest.approx(356079.56, rel=0, abs=0.01)
    assert food.Expenditure.sum() == pytest.approx(expected, rel=0, abs=0.01)
    # The library's separate capital-Value convention still carries Q=E.
    monetary = food.loc[food.index.get_level_values("u") == "Value"]
    assert len(monetary) == 46
    assert monetary.Quantity.eq(monetary.Expenditure).all()
