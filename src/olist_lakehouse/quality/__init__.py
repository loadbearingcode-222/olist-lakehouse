"""The quality gate between Kafka and bronze.

rules.py    the rule catalog: one place names every rule, its scope and its violation, and the
            chaos kind it answers to (pure Python, no Spark)
expect.py   the same rules as Great Expectations expectations (the independent second opinion)
gate.py     Spark: parse, route rows to bronze or quarantine (first matching rule), measure
"""
