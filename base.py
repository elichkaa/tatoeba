import polars as pl

# NOTE: choose langs here
MAIN_LANG = "ita"
TRANSLATIONS = ["bul", "eng", "ger", "deu", "spa", "por", "bra"]
ALL_LANGS = [MAIN_LANG] + TRANSLATIONS

sentences = pl.read_parquet("sentences.parquet")

translations = sentences.filter(
    pl.col("lang").is_in(TRANSLATIONS))
main = sentences.filter(
    pl.col("lang") == MAIN_LANG)
links = pl.read_parquet("links.parquet")
main = main.join(links, how="inner", left_on="id", right_on="id_1").join(
    translations, how="left", left_on="id_2", right_on="id").filter(pl.col("lang_right").is_not_null())

tags = pl.read_parquet("tags.parquet")
main = main.join(tags, how="left", on="id")
audios = pl.read_parquet("sentences_with_audio.parquet")
main = main.join(audios, how="left", left_on="id", right_on="sentence_id")
main = main.group_by(["id", "id_2"]).agg(
    *[pl.col(col).first() for col in ["lang", "sentence",
                                      "sentence_right", "audio", "lang_right"]],
    pl.concat_list("tag").first()
).rename({"id": f"main_id", "id_2": "translation_id",
          "lang_right": "translation_lang", "sentence_right": "translation"})

main.write_parquet(f"{MAIN_LANG}.parquet")
