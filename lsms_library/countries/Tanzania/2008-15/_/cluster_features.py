#!/usr/bin/env python
"""Thin wrapper: cluster_features is built by other_features.py."""
import runpy
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
runpy.run_path('other_features.py', run_name='__main__')
