import smallpond


def main():
    sp = smallpond.init()

    df = sp.read_parquet("sample.parquet")
    failures = df.filter("failure = 1") # doesn't work
    failures = df.filter(lambda r: r['failure'] == 1)
    failures.write_parquet("hobbits.parquet")


if __name__ == "__main__":
    main()
