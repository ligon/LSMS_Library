import pandas as pd

from lsms_library.local_tools import format_id


def i(value):
    """Format composite household id from pop_pt + hhid."""
    parts = [str(int(value.iloc[k])) for k in range(len(value))]
    return format_id('-'.join(parts))


def individual_education(df):
    """Map the out-of-ladder s3q2 code 0 (1 obs) to Unknown.

    The 1999 inline mapping (data_info.yml) decodes the questionnaire ladder
    codes 1-9; code 0 is not part of that ladder and survives the mapping as
    the literal string '0.0'.  It is undecodable (GH #493) -> Unknown.
    """
    if 'Educational Attainment' in df.columns:
        # The unmapped code survives as the float 0.0 here (stringified to
        # '0.0' only in the API output), so replace both forms.
        df['Educational Attainment'] = df['Educational Attainment'].replace(
            {0.0: 'Unknown', '0.0': 'Unknown'}
        )
    return df
