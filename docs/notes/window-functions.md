# Window functions cheat sheet

```sql
-- rank within a group
row_number() over (partition by customer_id order by created_at)

-- running total
sum(amount) over (partition by customer_id order by created_at
                  rows between unbounded preceding and current row)

-- moving 7-day average (one row per day)
avg(revenue) over (order by day rows between 6 preceding and current row)

-- previous value, e.g. days since last order
created_at - lag(created_at) over (partition by customer_id order by created_at)

-- share of total
amount / sum(amount) over ()
```

Remember: `rows` counts physical rows, `range` uses values (ties are grouped).
Window functions run after `where` and `group by`, so filter on them in an
outer query or with `qualify` where supported.
