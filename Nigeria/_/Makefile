
rounds := $(shell ls -d ../2*-*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

all: $(parquet)

food_expenditures = $(shell find $(rounds) -name food_expenditures.py)
food_expenditures_parquet := $(food_expenditures:.py=.parquet)

food_expenditures.parquet: food_expenditures.py $(food_expenditures_parquet)
	python food_expenditures.py

../2010-11/_/food_expenditures.parquet:
	(cd ../2010-11/_; python food_expenditures.py)

../2012-13/_/food_expenditures.parquet:
	(cd ../2012-13/_; python food_expenditures.py)

../2015-16/_/food_expenditures.parquet:
	(cd ../2015-16/_; python food_expenditures.py)

../2018-19/_/food_expenditures.parquet:
	(cd ../2018-19/_; python food_expenditures.py)
