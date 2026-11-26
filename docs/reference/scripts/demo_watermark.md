# `scripts/demo_watermark.py`

> PHASE 13 - What a watermark accepts, and what it drops.

Reference for the public API of `scripts/demo_watermark.py` (209 lines),
extracted from the source code: signatures and docstrings.

## Imports

- Third-party: `pyspark`
- Project: `src.common`, `src.common.schemas`, `src.streaming.spark_session`, `src.streaming.transform`
- Standard library: `argparse`, `contextlib`, `datetime`, `time`

## Constants

- `DEMO_TOPIC = 'orders-watermark-demo'`

## Functions

### `main`

```python
main(argv: list[str] | None=None) -> int
```

## Usage

This module is also a script:

```bash
python scripts/demo_watermark.py
```
