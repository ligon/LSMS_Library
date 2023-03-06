
#rounds := ../2008-09/ ../2010-11/ ../2012-13/ ../2014-15/  #$(shell ls -d ../2*-*)
rounds := $(shell ls -d ../2*-*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)

all: $(parquet)

food_expenditures = $(shell find $(rounds) -name food_expenditures.py)
food_expenditures_parquet := $(food_expenditures:.py=.parquet)

food_expenditures.parquet: food_expenditures.py $(food_expenditures_parquet)
	python food_expenditures.py

food_quantities = $(shell find $(rounds) -name food_quantities.py)
food_quantities_parquet := $(food_quantities:.py=.parquet)

food_quantities.parquet: food_quantities.py $(food_expenditure_parquet)
	python food_quantities.py

unitvalues.parquet: unitvalues.py $(food_expenditure_parquet) $(food_quantities_parquet)
	python unitvalues.py

household_characteristics = $(shell find $(rounds) -name household_characteristics.py)
household_characteristics_parquet := $(household_characteristics:.py=.parquet)

../var/household_characteristics.parquet: household_characteristics.py $(household_characteristics_parquet)
	python household_characteristics.py

../%/_/household_characteristics.parquet:
	(cd $(@D) && python household_characteristics.py)

%.parquet: %.py food_items.org uganda.py
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
	-rm *.parquet

