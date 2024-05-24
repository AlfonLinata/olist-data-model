# Analyses

These analyses used `fact.fact__order_items` table as its source data.
This fact is duplicated fact with all of its order status so the analyses should filter order_status column.
But in this case, because we are not doing real ELT, the data is not duplicated and I'm not include that filter.