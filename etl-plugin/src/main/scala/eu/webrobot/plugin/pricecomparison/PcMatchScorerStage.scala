package eu.webrobot.plugin.pricecomparison

import eu.webrobot.plugin.sdk.{WArgs, WRow, WTransformStage}

/**
 * Tier 1+2 match scoring.
 *
 * Arg positions (all optional):
 *   0 - ean_input_field   (default: "ean")
 *   1 - ean_found_field   (default: "pc_ean_code")
 *   2 - title_input_field (default: "product_name")
 *   3 - title_found_field (default: "pc_title")
 *
 * Confidence values:
 *   0.95        — EAN exact match (digits only, case-insensitive)
 *   0.50–0.85   — Jaccard title similarity > 0 (mapped linearly)
 *   0.0         — no evidence (zero similarity or missing fields)
 *
 * Sets "match_confidence" (Double) on the row.
 */
class PcMatchScorerStage extends WTransformStage {

  override val name: String = "pc_match_scorer"

  override def transform(row: WRow, args: WArgs): WRow = {
    val eanInputField   = args.string(0, "ean")
    val eanFoundField   = args.string(1, "pc_ean_code")
    val titleInputField = args.string(2, "product_name")
    val titleFoundField = args.string(3, "pc_title")

    val confidence = score(row, eanInputField, eanFoundField, titleInputField, titleFoundField)
    row.set("match_confidence", confidence)
  }

  private def score(
      row: WRow,
      eanInputField: String, eanFoundField: String,
      titleInputField: String, titleFoundField: String
  ): Double = {
    val eanInput = row.str(eanInputField).map(normalizeEan).getOrElse("")
    val eanFound = row.str(eanFoundField).map(normalizeEan).getOrElse("")

    if (eanInput.nonEmpty && eanFound.nonEmpty && eanInput == eanFound)
      return 0.95

    val titleInput = row.str(titleInputField).getOrElse("")
    val titleFound = row.str(titleFoundField).getOrElse("")

    if (titleInput.nonEmpty && titleFound.nonEmpty) {
      val sim = jaccardSimilarity(titleInput, titleFound)
      // Only claim a title-based match if there is actual token overlap.
      // sim == 0.0 means completely unrelated titles — return 0.0, not 0.50.
      if (sim > 0.0) return 0.50 + sim * 0.35
    }

    0.0
  }

  private def normalizeEan(s: String): String =
    s.trim.replaceAll("[^0-9]", "")

  private def tokenize(s: String): Set[String] =
    s.toLowerCase.split("[^a-z0-9]+").filter(_.length >= 2).toSet

  private def jaccardSimilarity(a: String, b: String): Double = {
    val tokA = tokenize(a)
    val tokB = tokenize(b)
    val union = tokA.union(tokB).size.toDouble
    if (union == 0.0) return 0.0
    tokA.intersect(tokB).size.toDouble / union
  }
}
