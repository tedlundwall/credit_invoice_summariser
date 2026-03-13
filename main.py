from credit_invoice_summariser import database, invoice, utils


def main():
    utils.startup_message()
    conn = database.establish_conn()
    database.init(conn)
    pdf_paths = invoice.select_path()
    pdf_paths = database.check_dupes(pdf_paths, conn)
    if not pdf_paths:
        print("No new invoices.")
    else:
        df = invoice.bind(pdf_paths)
        database.ingest_transactions(df, conn)
    database.close_conn(conn)

    globals().update(locals())


if __name__ == "__main__":
    main()
