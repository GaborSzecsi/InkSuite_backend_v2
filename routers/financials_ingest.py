"""Retired CSV-to-local-JSON rebuild command.

This command is intentionally disabled. Current financial records live in SQL;
distributor integrations must use their configured database ingestion workflow.
"""

def main():
    raise SystemExit("Legacy CSV-to-JSON ingestion is retired; no files were read or written.")

if __name__ == "__main__":
    main()
