# Formatting Functions for Guinea-Bissau
import lsms_library.local_tools as tools


def i(value):
    '''Formatting household id from (grappe, menage).'''
    return tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)
