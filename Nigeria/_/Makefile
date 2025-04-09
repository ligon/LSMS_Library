
rounds := $(shell ls -d ../2*-*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

var = ../var/food_expenditures.parquet ../var/food_quantities.parquet ../var/unitvalues.parquet \
	  ../var/household_roster.parquet ../var/household_characteristics.parquet ../var/food_prices.parquet

all: $(var)

../var/household_characteristics.parquet: ../var/household_roster.parquet household_roster.py household_characteristics.py
	python household_characteristics.py 

food_expenditures = $(shell find $(rounds) -name food_expenditures.py)
food_expenditures_parquet := $(food_expenditures:.py=.parquet)

../var/food_expenditures.parquet: food_expenditures.py $(food_expenditures_parquet)
	python food_expenditures.py

../2010-11/_/food_expenditures.parquet:
	(cd ../2010-11/_; python food_expenditures.py)

../2012-13/_/food_expenditures.parquet:
	(cd ../2012-13/_; python food_expenditures.py)

../2015-16/_/food_expenditures.parquet:
	(cd ../2015-16/_; python food_expenditures.py)

../2018-19/_/food_expenditures.parquet:
	(cd ../2018-19/_; python food_expenditures.py)

nonfood_expenditures = $(shell find $(rounds) -name nonfood_expenditures.py)
nonfood_expenditures_parquet := $(nonfood_expenditures:.py=.parquet)

../var/nonfood_expenditures.parquet: nonfood_expenditures.py $(nonfood_expenditures_parquet)
	python nonfood_expenditures.py

../2010-11/_/nonfood_expenditures.parquet: ../2010-11/_/nonfood_expenditures.py
	(cd ../2010-11/_; python nonfood_expenditures.py)

../2012-13/_/nonfood_expenditures.parquet: ../2012-13/_/nonfood_expenditures.py
	(cd ../2012-13/_; python nonfood_expenditures.py)

../2015-16/_/nonfood_expenditures.parquet: ../2015-16/_/nonfood_expenditures.py
	(cd ../2015-16/_; python nonfood_expenditures.py)

../2018-19/_/nonfood_expenditures.parquet: ../2018-19/_/nonfood_expenditures.py
	(cd ../2018-19/_; python nonfood_expenditures.py)


household_roster = $(shell find $(rounds) -name household_roster.py)
household_roster_parquet := $(household_roster:.py=.parquet)

../var/household_roster.parquet: household_roster.py $(household_roster_parquet)
	python household_roster.py

../2010-11/_/household_roster.parquet: ../2010-11/_/household_roster.py
	(cd ../2010-11/_; python household_roster.py)

../2012-13/_/household_roster.parquet: ../2012-13/_/household_roster.py
	(cd ../2012-13/_; python household_roster.py)

../2015-16/_/household_roster.parquet: ../2015-16/_/household_roster.py
	(cd ../2015-16/_; python household_roster.py)

../2018-19/_/household_roster.parquet: ../2018-19/_/household_roster.py
	(cd ../2018-19/_; python household_roster.py)

food_quantities = $(shell find $(rounds) -name food_expenditures.py)
food_quantities_parquet := $(food_quantities:.py=.parquet)

../var/food_quantities.parquet: food_quantities.py $(food_quantities_parquet)
	python food_quantities.py

../2010-11/_/food_quantities.parquet: ../2010-11/_/food_expenditures.py
	(cd ../2010-11/_; python food_expenditures.py)

../2012-13/_/food_quantities.parquet: ../2012-13/_/food_expenditures.py
	(cd ../2012-13/_; python food_expenditures.py)

../2015-16/_/food_quantities.parquet: ../2015-16/_/food_expenditures.py
	(cd ../2015-16/_; python food_expenditures.py)

../2018-19/_/food_quantities.parquet: ../2018-19/_/food_expenditures.py
	(cd ../2018-19/_; python food_expenditures.py)

unitvalues = $(shell find $(rounds) -name food_expenditures.py)
unitvalues_parquet := $(unitvalues:.py=.parquet)

../var/unitvalues.parquet: unitvalues.py $(unitvalues_parquet)
	python unitvalues.py

../2010-11/_/unitvalues.parquet: ../2010-11/_/food_expenditures.py
	(cd ../2010-11/_; python food_expenditures.py)

../2012-13/_/unitvalues.parquet: ../2012-13/_/food_expenditures.py
	(cd ../2012-13/_; python food_expenditures.py)

../2015-16/_/unitvalues.parquet: ../2015-16/_/food_expenditures.py
	(cd ../2015-16/_; python food_expenditures.py)

../2018-19/_/unitvalues.parquet: ../2018-19/_/food_expenditures.py
	(cd ../2018-19/_; python food_expenditures.py)

../var/food_prices.parquet: ../var/unitvalues.parquet
	cp $< $@