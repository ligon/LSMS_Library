#!/usr/bin/env python3

from harmony import main as mydata
import numpy as np
from cfe.regression import Regression
import argparse

def main(country):
    x,z,p = mydata(country)

    y = np.log(x.replace(0,np.nan)).squeeze()

    r = Regression(y=y,d=z)

    return r

if __name__=='__main__':
    parser = argparse.ArgumentParser('Estimate CFE demand system for country.')
    parser.add_argument("country")

    args = parser.parse_args()

    r = main(args.country)
