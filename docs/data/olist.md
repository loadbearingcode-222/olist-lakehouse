# The Olist dataset

**Source:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(`olistbr/brazilian-ecommerce` on Kaggle). © Olist, licensed
[CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/): attribution,
non-commercial, share-alike. The CSVs are downloaded at runtime by `.\make.ps1 fetch` and are
never committed. `data/olist.sha256` pins their exact bytes.

## Files and grain

Record counts are parsed with a CSV reader (observed 2026-09-24, pinned by the manifest).

| File | Records | Grain | Key |
|---|---:|---|---|
| `olist_orders_dataset.csv` | 99,441 | one order | `order_id` |
| `olist_order_items_dataset.csv` | 112,650 | one line of an order | `order_id` + `order_item_id` |
| `olist_order_payments_dataset.csv` | 103,886 | one payment of an order | `order_id` + `payment_sequential` |
| `olist_order_reviews_dataset.csv` | 99,224 | one review | `review_id` is **not** unique (98,410 distinct) |
| `olist_customers_dataset.csv` | 99,441 | one customer *per order* | `customer_id`; the person is `customer_unique_id` (96,096) |
| `olist_products_dataset.csv` | 32,951 | one product | `product_id` |
| `olist_sellers_dataset.csv` | 3,095 | one seller | `seller_id` |
| `olist_geolocation_dataset.csv` | 1,000,163 | one lat/lng sample | **not unique**: 19,015 distinct zip prefixes |
| `product_category_name_translation.csv` | 71 | Portuguese → English category | `product_category_name` |

## The order lifecycle (what the replayer turns into CDC)

`order_purchase_timestamp` → `order_approved_at` → `order_delivered_carrier_date` →
`order_delivered_customer_date`, with `order_estimated_delivery_date` as the promise. Purchases
span **2016-09-04 → 2018-10-17**.

| `order_status` | Orders |
|---|---:|
| delivered | 96,478 |
| shipped | 1,107 |
| canceled | 625 |
| unavailable | 609 |
| invoiced | 314 |
| processing | 301 |
| created | 5 |
| approved | 2 |

Lifecycle timestamps are missing where the order never got there: `order_approved_at` 160,
`order_delivered_carrier_date` 1,783, `order_delivered_customer_date` 2,965.

## Event time vs processing time

- **Event (business) time** is the real Olist timestamp, 2016–2018. Analytics, FX conversion and
  streaming watermarks all use it, so the numbers stay real.
- **Processing time** is when the replayer wrote the change, which is now. The replayer walks
  business time in order and compresses it (default 1 day = 1 minute). Bronze keeps both.

## Hazards the pipeline must handle

- **BOM:** `product_category_name_translation.csv` starts with a UTF-8 byte-order mark. The
  contract reads everything as `utf-8-sig`.
- **Quoted line breaks:** review comments contain line breaks inside quotes, so 99,224 records
  span 104,719 physical lines. Count and parse records, never lines.
- **Misspelled source columns:** `product_name_lenght` and `product_description_lenght`. Raw
  keeps them verbatim; staging renames them.
- **Customer vs person:** a new `customer_id` is issued for every order. Repeat buyers are
  found via `customer_unique_id`.
- **Duplicate reviews:** 814 `review_id` values repeat. Silver deduplicates with a declared rule.
- **Orders without reviews:** 768.
- **Uncategorised products:** 610 products have no `product_category_name`. Two categories
  (`pc_gamer`, `portateis_cozinha_e_preparadores_de_alimentos`) have no English translation.
- **Payments:** `not_defined` appears 3 times; `voucher` orders often carry several payments.
  Total `payment_value` is **16,008,872.12 BRL**, the reconciliation target for gold (M3-S2).
- **Shipping limits beyond the data window:** `shipping_limit_date` runs to 2020-04-09, which
  is later than any purchase.
- **Geolocation** has many samples per zip prefix with inconsistent city spelling (accents).
  Aggregate it to one point per prefix before joining.
