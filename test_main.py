import subprocess
import os


def test_transform():
    result = subprocess.run(
        [
            "python",
            "main.py",
            "transform",
            "https://raw.githubusercontent.com/fivethirtyeight/data/master/alcohol-consumption/drinks.csv",   
            "https://raw.githubusercontent.com/fivethirtyeight/data/master/drug-use-by-age/drug-use-by-age.csv"
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0


def test_create_row():
    result = subprocess.run(
        [
            "python",
            "main.py",
            "create",
            "USB",  # country
            "10",  # beer_servings
            "20",  # spirit_servings
            "30",  # wine_servings
            "10.7",  # total_litres_of_pure_alcohol
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0


def test_read_all():
    result = subprocess.run(
        ["python", "main.py", "read"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0


def test_update_row():
    result = subprocess.run(
        [
            "python",
            "main.py",
            "update",
            "USA",  # country
            "15",  # new beer_servings
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0


def test_delete_row():
    result = subprocess.run(
        ["python", "main.py", "delete", "Canada"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0


def test_general():
    result = subprocess.run(
        [
            "python",
            "main.py",
            "general",
            "SELECT tc.country, tc.total_beer_servings, u.age_group, u.alcohol_use, u.alcohol_frequency FROM (SELECT country, SUM(beer_servings) AS total_beer_servings FROM zw308_drink GROUP BY country ORDER BY total_beer_servings DESC LIMIT 5) AS tc JOIN zw308_drug_use u ON u.alcohol_use = (SELECT MAX(alcohol_use) FROM zw308_drug_use) ORDER BY tc.total_beer_servings DESC, u.alcohol_use DESC;",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0


if __name__ == "__main__":
    test_transform()
    #test_create_row()
    test_read_all()
    #test_update_row()
    #test_delete_row()
    test_general()
