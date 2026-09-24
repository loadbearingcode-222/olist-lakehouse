"""A web clickstream that agrees with the replayed orders.

sessions.py  one order → a converting session (views, add-to-cart, checkout, purchase) plus
             anonymous browsing sessions that do not convert; deterministic per order
chaos.py     duplicates, late events, null product ids, schema drift; deterministic per event
__main__.py  poll new orders from Postgres, produce to Kafka, checkpoint + chaos ledger
"""
