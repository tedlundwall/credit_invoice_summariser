import glob
import os
import sqlite3
from tkinter import filedialog

import polars as pl
import tabula


def startup_message():
    msg = "Program is starting. Working directory: \n" + os.getcwd()
    print(msg)


def select_invoice_path():
    pdf_dir = "D:/Git/credit_invoice_summariser/data/raw"
    # pdf_dir = filedialog.askdirectory()
    pdf_paths = glob.glob(pdf_dir + "/*.pdf")
    pdf_paths = [pdf_path.replace("\\", "/") for pdf_path in pdf_paths]

    return pdf_paths


def check_dupes(pdf_paths):
    conn = sqlite3.connect("data/db/credit_card.db")
    distinct = (
        pl.read_database("SELECT DISTINCT filename FROM transactions", conn)
        .get_column("filename")
        .to_list()
    )
    conn.close()

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


def read_pdf(pdf_path):
    pdf_name = pdf_path.split("/")[-1]
    print("Processing: " + pdf_name)

    pdf = tabula.read_pdf(
        pdf_path,
        pages="all",
        stream=True,
        guess=False,
        columns=[72, 264, 346, 414, 456, 514],
        pandas_options={
            "header": None,
            "names": [
                "date",
                "vendor",
                "card_number",
                "local_currency",
                "currency",
                "exchange_rate",
                "amount_in_sek",
            ],
        },
        force_subprocess=True,
    )

    df_lst = [pl.from_pandas(page) for page in pdf]
    df = pl.concat(df_lst, how="vertical")
    df = (
        df.lazy()
        .filter(pl.col("date").str.contains(r"^\d{2}\.\d{2}\.\d{4}$"))
        .filter(pl.col("card_number").is_not_null())
        .with_columns(
            pl.col("date").str.to_date("%d.%m.%Y"),
            pl.col("local_currency")
            .str.replace(r"\.", "")
            .str.replace(",", ".")
            .cast(pl.Float64),
            pl.col("exchange_rate").cast(pl.Float64),
            pl.col("amount_in_sek")
            .str.replace(r"\.", "")
            .str.replace(",", ".")
            .cast(pl.Float64),
            pl.lit(pdf_name).alias("filename"),
        )
        .sort(pl.col("date"))
        .with_row_index("line_number")
        .collect()
    )

    return df


def bind_invoices(pdf_paths):
    if isinstance(pdf_paths, str):
        pdf_paths = [pdf_paths]

    df_lst = [read_pdf(pdf_path) for pdf_path in pdf_paths]
    df = pl.concat(df_lst, how="vertical")

    return df


def init_database():
    conn = sqlite3.connect("data/db/credit_card.db")

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
    conn.close()


def ingest_transactions(df):
    conn = sqlite3.connect("data/db/credit_card.db")

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
    conn.close()


def query_database():
    conn = sqlite3.connect("data/db/credit_card.db")

    df = pl.read_database("SELECT * FROM transactions", conn)

    conn.close()

    return df


def summarise_transactions(df, column=None):
    summary = (
        df.group_by(column)
        .agg(pl.col("amount_in_sek").sum().round(2))
        .sort("amount_in_sek", descending=True)
    )

    if column is None:
        summary = summary.drop("literal")

    return summary


def main():
    startup_message()
    init_database()
    pdf_paths = select_invoice_path()
    pdf_paths = check_dupes(pdf_paths)
    if not pdf_paths:
        print("No new invoices.")
    else:
        df = bind_invoices(pdf_paths)
        ingest_transactions(df)
    result = query_database()

    globals().update(locals())


if __name__ == "__main__":
    main()
