
rounds := $(shell ls -d ../2*-*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

var = ../var/food_expenditures.parquet ../var/food_quantities.parquet ../var/food_prices.parquet \
	  ../var/household_characteristics.parquet ../var/other_features.parquet \

all: $(parquet) $(var)

../var/food_quantities.parquet ../var/food_expenditures.parquet ../var/food_prices.parquet : ../var/food_acquired.parquet food_prices_quantities_and_expenditures.py
	python food_prices_quantities_and_expenditures.py

food_acquired = $(shell find $(rounds) -name food_acquired.py)
food_acquired_parquet := $(food_acquired:.py=.parquet)

../%/_/food_acquired.parquet: $(food_acquired)
	(cd $(@D) && python food_acquired.py)

../var/food_acquired.parquet: food_acquired.py $(food_acquired_parquet) ../var/other_features.parquet
	python food_acquired.py

household_characteristics = $(shell find $(rounds) -name household_characteristics.py)
household_characteristics_parquet := $(household_characteristics:.py=.parquet)

../%/_/household_characteristics.parquet:
	(cd $(@D) && python household_characteristics.py)

../var/household_characteristics.parquet: household_characteristics.py $(household_characteristics_parquet)
	python household_characteristics.py

other_features = $(shell find $(rounds) -name other_features.py)
other_features_parquet := $(other_features:.py=.parquet)

../%/_/other_features.parquet:
	(cd $(@D) && python other_features.py)

../var/other_features.parquet: other_features.py $(other_features_parquet)
	python other_features.py

../%/_/other_expenditures.parquet:
	(cd $(@D) && python other_expenditures.py)

../var/other_expenditures.parquet: other_expenditures.py $(other_expenditures_parquet)
	python other_expenditures.py



clean:
	-rm $(parquet)
	-rm ../var/*.parquet
