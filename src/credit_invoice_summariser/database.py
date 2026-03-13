import sqlite3

import polars as pl
from tkinter import filedialog


def establish_conn():
    try:
        conn = sqlite3.connect("data/db/credit_card.db")
    except sqlite3.OperationalError:
        try:
            conn = sqlite3.connect("../data/db/credit_card.db")
        except sqlite3.OperationalError:
            conn_path = filedialog.askopenfilename()
            conn = sqlite3.connect(conn_path)

    return conn


def init(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            line_number INTEGER,
            date TEXT,
            vendor TEXT,
            card_number TEXT,
            local_currency REAL,
            currency TEXT,
            exchange_rate REAL,
            amount_in_sek REAL,
            filename TEXT,
            UNIQUE(line_number, date, vendor, amount_in_sek, card_number, filename)
            );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_transactions_date
        ON transactions(date);
        """
    )

    conn.commit()


def check_dupes(pdf_paths, conn):
    distinct = (
        pl.read_database("SELECT DISTINCT filename FROM transactions", conn)
        .get_column("filename")
        .to_list()
    )

    if isinstance(distinct, str):
        distinct = [distinct]
    pdf_dir = "D:/Git/credit_invoice_summariser/data/raw"
    existing_pdf_paths = [pdf_dir + "/" + pdf_name for pdf_name in distinct]
    new_pdf_paths = [
        pdf_path
        for pdf_path in pdf_paths
        if pdf_path not in existing_pdf_paths
    ]

    return new_pdf_paths


def ingest_transactions(df, conn):
    df = df.with_columns(pl.col("date").cast(pl.String))
    rows = df.to_dicts()

    conn.executemany(
        """
        INSERT OR IGNORE INTO transactions
        VALUES (
            NULL,
            :line_number,
            :date,
            :vendor,
            :card_number,
            :local_currency,
            :currency,
            :exchange_rate,
            :amount_in_sek,
            :filename
        )
        """,
        rows,
    )

    conn.commit()


def query(conn):
    df = pl.read_database("SELECT * FROM transactions", conn).with_columns(
        pl.col("date").str.to_date("%Y-%m-%d")
    )

    return df


def close_conn(conn):
    conn.close()
