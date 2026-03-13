import glob

import polars as pl
import tabula


def select_path():
    pdf_dir = "D:/Git/credit_invoice_summariser/data/raw"
    # pdf_dir = filedialog.askdirectory()
    pdf_paths = glob.glob(pdf_dir + "/*.pdf")
    pdf_paths = [pdf_path.replace("\\", "/") for pdf_path in pdf_paths]

    return pdf_paths


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


def bind(pdf_paths):
    if isinstance(pdf_paths, str):
        pdf_paths = [pdf_paths]

    df_lst = [read_pdf(pdf_path) for pdf_path in pdf_paths]
    df = pl.concat(df_lst, how="vertical")

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
