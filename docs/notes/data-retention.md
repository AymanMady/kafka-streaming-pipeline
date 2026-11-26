# Data retention

| Layer | Suggested retention | Reason |
|---|---|---|
| Raw / bronze | long (months to years) | allows full reprocessing |
| Silver | as long as gold needs to be rebuilt | intermediate |
| Gold | business requirement | reporting history |
| Logs and checkpoints | weeks | debugging |
| Quarantine | until triaged | investigation |

## Implementation

- Partition by date so expiry is a cheap partition drop.
- Delta Lake: `VACUUM` removes files no longer referenced; keep the retention
  longer than the longest running query and the time-travel need.
- Object stores: lifecycle rules move old raw data to colder storage.

Personal data may have a legal maximum retention: check it first.
