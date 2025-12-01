import polars as pl

AUDIO_BASE_LINK = "https://tatoeba.org/audio/download/"
SENTENCES_WITH_AUDIO_KEY = "sentences_with_audio"


def csv_to_parquet(name: str, mapping: dict[str, str], **kwargs) -> None:
    obj = pl.read_csv(f"{name}.csv", encoding="utf8",
                      separator="	", has_header=False, quote_char=None, **kwargs).rename(mapping)
    if name == SENTENCES_WITH_AUDIO_KEY:
        obj = obj.drop([f"column_{i}" for i in range(3, 6)])
        obj = obj.with_columns(
            (AUDIO_BASE_LINK + pl.col("audio_id").cast(pl.String)).alias("audio")).drop("audio_id")
    obj.write_parquet(f"{name}.parquet")


csv_to_parquet(name="links", mapping={"column_1": "id_1", "column_2": "id_2"})
csv_to_parquet(name="tags", mapping={"column_1": "id", "column_2": "tag"})
csv_to_parquet("sentences", mapping={"column_1": "id", "column_2": "lang",
               "column_3": "sentence"})
csv_to_parquet(name=SENTENCES_WITH_AUDIO_KEY, mapping={
               "column_1": "sentence_id", "column_2": "audio_id"})
