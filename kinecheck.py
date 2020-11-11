#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This script requires python 3 and several extra modules.
# After installing python3 and python3-pip, run:
# pip3 install --upgrade click mysql-connector-python

import logging
import sys
from datetime import datetime
from time import sleep

import click
import mysql.connector


@click.command()
@click.argument('hostname')
@click.argument('username')
@click.argument('password')
@click.argument('database')
def main(hostname, username, password, database):
    cnx = mysql.connector.connect(host=hostname, user=username, password=password, database=database)
    logging.info(f"Connected to {hostname}/{database}")
    cnx.autocommit = True

    while True:
        kine_ids = dict()
        kine_names = dict()
        prev_revisions = dict()
        faux_target_rev = 0
        cur_compact_rev = 0

        # Print max and compact rev.
        with cnx.cursor() as cursor:
            cursor.execute("""
            SELECT
                MAX(id) AS current_rev,
                (SELECT prev_revision FROM kine WHERE name = 'compact_rev_key' LIMIT 1) AS compact_rev,
                (SELECT COUNT(*) FROM kine WHERE name LIKE 'gap-%') AS gap_count
            FROM kine
            """)

            for (current_rev, compact_rev, gap_count) in cursor:
                cur_compact_rev = compact_rev
                faux_target_rev = current_rev - 1000
                logging.info(f"Compacted to {compact_rev}/{current_rev} - {current_rev - compact_rev} revs back; {gap_count} gaps at {datetime.now().isoformat()}")

        # Validate that all rows pointed at by compact_rev have the same name as the row that referred to them,
        # and that multiple rows don't have the same prev_rev.
        with cnx.cursor() as cursor:
            cursor.execute("""
            SELECT id AS revision, prev_revision, deleted, name
            FROM kine
            ORDER BY name ASC, id ASC
            """)
            rowcount = 0
            deletecount = 0
            for (revision, prev_revision, deleted, name) in cursor:
                if name != 'compact_rev_key' and prev_revision in prev_revisions and prev_revision != 0:
                    logging.warning(f"\tDuplicate prev_revision={prev_revision} in {revision} - also targeted by {prev_revisions[prev_revision]}")
                if name != 'compact_rev_key' and prev_revision in kine_names and kine_names[prev_revision] != name:
                    logging.warning(f"\tName mismatch: id={revision}, prev_revision={prev_revision}, name={name} - prev_revision name={kine_names[prev_revision]}")
                if name != 'compact_rev_key' and prev_revision not in kine_names and prev_revision != 0 and prev_revision > cur_compact_rev:
                    logging.warning(f"\tPrevious revision {prev_revision} missing but uncompacted for id={revision}, name={name} - compact_rev={cur_compact_rev}")
                rowcount += 1
                kine_ids[name] = revision
                kine_names[revision] = name
                prev_revisions[prev_revision] = revision
                if deleted:
                    deletecount += 1
            logging.info(f"\tValidated names on {rowcount} rows ({deletecount} deleted)")

        # Run the compaction inner join query to figure out which rows we would compact.
        # This compacts to a fixed point 1000 rows back from current_rev, as opposed to the rolling checkpoint
        # used by the actual code - which should actually be more aggressive, which is good for finding problems.
        with cnx.cursor() as cursor:
            cursor.execute("""
            SELECT kv.id AS revision, kv.prev_revision AS prev_revision, kv.deleted AS deleted, kv.name AS name
            FROM kine AS kv
            INNER JOIN (
                SELECT kp.prev_revision AS id
                FROM kine AS kp
                WHERE
                    kp.prev_revision != 0 AND
                    kp.id <= %s
                UNION
                SELECT kd.id AS id
                FROM kine AS kd
                WHERE
                    kd.deleted != 0 AND
                    kd.id <= %s
            ) AS ks
            ON
                kv.id = ks.id AND
                kv.name != 'compact_rev_key'
            ORDER BY kv.name ASC, kv.id ASC
            """, (faux_target_rev, faux_target_rev))

            rowcount = 0
            for (revision, prev_revision, deleted, name) in cursor:
                rowcount += 1
                if kine_ids[name] == revision and deleted == 0:
                    logging.warning(f"\tCompact would delete id={revision} prev_revision={prev_revision} name={name} deleted={deleted}")
                    logging.warning("\t\tThis is the most recent revision!")
            logging.info(f"\tCompact from {cur_compact_rev} to {faux_target_rev} would delete {rowcount} of {faux_target_rev - cur_compact_rev} rows")

        sleep(30)


if __name__ == '__main__':
    logging.basicConfig(format="%(levelname).1s %(message)s", level=logging.INFO, stream=sys.stdout)
    try:
        main()
    except (KeyboardInterrupt, BrokenPipeError):
        pass
