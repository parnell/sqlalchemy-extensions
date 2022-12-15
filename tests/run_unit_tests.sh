find unit_tests -maxdepth 1 -type f | xargs -n 1 python3 -m unittest
# find from_sqlalchemy -maxdepth 1 -type f | xargs -n 1 python3 -m unittest