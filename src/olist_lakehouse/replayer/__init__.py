"""Replay the Olist CSVs into Postgres as if a live e-commerce app were writing them.

source.py   raw CSV rows → the app's row shapes (clean names, typed values)
timeline.py rows → one deterministic list of insert/update/delete events in business time
chaos.py    which events carry an injected defect (deterministic per seed and sequence)
__main__.py the loop: a compressed business clock, batches, a transactional checkpoint
"""
