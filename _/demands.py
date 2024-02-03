#!/usr/bin/env python3

from harmony import main as mydata
import numpy as np
from cfe.regression import Regression
import argparse

def main(country,alltm=True):
    x,z,p = mydata(country)

    try:
        x = x.sum(axis=1)
    except ValueError: # Perhaps a series?
        pass

    y = np.log(x.replace(0,np.nan).dropna()).squeeze()

    r = Regression(y=y,d=z,alltm=alltm)
    r.get_beta()

    return r

if __name__=='__main__':
    parser = argparse.ArgumentParser('Estimate CFE demand system for country.')
    parser.add_argument("country")
    parser.add_argument('--alltm', action='store_true', help='Require all goods to be observed in all periods and markets.')

    args = parser.parse_args()

    r = main(args.country,args.alltm)
