install: 
	pip install --upgrade pip && pip install -r requirements.txt 

format: 
	black *.py

lint:
	#pylint --disable=R,C --ignore-patterns=test_.*?py $(wildcard *.py)
	ruff check *.py mylib/*.py

test: 
	python -m pytest -cov=main test_main.py

all: install format lint test

transform:
	python main.py transform  "https://raw.githubusercontent.com/fivethirtyeight/data/master/alcohol-consumption/drinks.csv" "https://raw.githubusercontent.com/fivethirtyeight/data/master/drug-use-by-age/drug-use-by-age.csv"


query1:
	python main.py general "INSERT INTO zw308_drink(country,beer_servings,spirit_servings,wine_servings, total_litres_of_pure_alcohol) VALUES('USC', 10,100,1000,0.1) "

query2:
	python main.py general "UPDATE zw308_drink SET  total_litres_of_pure_alcohol  = -0.1 WHERE country = 'USA'"

query3: 
	python main.py general "SELECT tc.country, tc.total_beer_servings, u.age_group, u.alcohol_use, u.alcohol_frequency FROM (SELECT country, SUM(beer_servings) AS total_beer_servings FROM zw308_drink GROUP BY country ORDER BY total_beer_servings DESC LIMIT 5) AS tc JOIN zw308_drug_use u ON u.alcohol_use = (SELECT MAX(alcohol_use) FROM zw308_drug_use) ORDER BY tc.total_beer_servings DESC, u.alcohol_use DESC;"

setup:
	python setup.py develop --user
