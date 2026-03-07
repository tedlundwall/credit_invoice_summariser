import polars as pl
import tabula
from tkinter import filedialog


def main():
    pdf_path = filedialog.askopenfilename()
    pdf_name = pdf_path.split("/")[-1]
    print("Filename: " + pdf_name)

    pdf = tabula.read_pdf(
        pdf_path,
        pages="all",
        stream=True,
        guess=False,
        columns=[72, 264, 346, 414, 456, 514],
        pandas_options={
            "header": None,
            "names": [
                "Datum",
                "Beskrivning",
                "Kortnr",
                "Lokal Valuta",
                "Valuta",
                "Valutakurs",
                "Belopp i SEK",
            ],
        },
        force_subprocess=True,
    )

    list_of_dfs = list(map(pl.from_pandas, pdf))
    df = pl.concat(list_of_dfs, how="vertical")
    df = (
        df.lazy()
        .filter(pl.col("Datum").str.contains(r"^\d{2}\.\d{2}\.\d{4}$"))
        .filter(pl.col("Kortnr").is_not_null())
        .with_columns(
            pl.col("Datum").str.to_date("%d.%m.%Y"),
            pl.col("Lokal Valuta")
            .str.replace(r"\.", "")
            .str.replace(",", ".")
            .cast(pl.Float64),
            pl.col("Valutakurs").cast(pl.Float64),
            pl.col("Belopp i SEK")
            .str.replace(r"\.", "")
            .str.replace(",", ".")
            .cast(pl.Float64),
        )
        .sort(pl.col("Datum"))
        .collect()
    )

    summary = df.select(pl.col("Belopp i SEK").sum())

    print(f"Total: {summary.get_column('Belopp i SEK').item()} kr")

    return df


if __name__ == "__main__":
    out = main()
