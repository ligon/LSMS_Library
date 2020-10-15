
rounds := $(shell ls -d ../2*-*)

source = $(shell find $(rounds) -name \*.py)
parquet := $(source:.py=.parquet)


all: $(parquet)

%.parquet: %.py food_items.org
	(cd $(@D) && python ./$(<F))

clean:
	-rm $(parquet)
