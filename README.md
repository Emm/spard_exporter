# spard-exporter

`spard-exporter` is an utility which parses Sparkassen Danmark PDF export files and turns them into CSV files.

# Usage

```bash
$ spard_exporter eksport.pdf output.csv
```

This builds a CSV with a header and the following columns:

| Field name | Content | Example |
| -- | -- | -- |
| **date** | Date of the transaction in ISO-8601 format | 2023-12-29 |
| **description** | The text of the transaction | MobilePay Johnny Olsen |
| **amount** | The amount of the transaction (may be negative) in US format | -7000.00 |
| **balance** | The balance of the account after the transaction (may be negative) in US format | -7000.00 |
| **currency** | The currency of the transaction (presumably a 3-letter ISO-4217 code) | DKK |
