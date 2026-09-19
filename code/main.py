#!/usr/bin/env python3
"""Entrypoint.

    python main.py init-db      create the database schema
    python main.py demo         seed an offline corpus, run the pipeline, print the feed
    python main.py serve        run the HTTP API with the live pipeline
    python main.py poll-once    fetch all due sources once, then exit
    python main.py add-source NAME FEED_URL [--reliability 0.9]

See SYSTEM_DESIGN.md for the architecture these commands exercise.
"""

from newsagg.cli import main

if __name__ == "__main__":
    main()
