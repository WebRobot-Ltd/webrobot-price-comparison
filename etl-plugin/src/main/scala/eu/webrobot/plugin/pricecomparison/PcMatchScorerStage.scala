package eu.webrobot.plugin.pricecomparison

import eu.webrobot.plugin.sdk.{WArgs, WRow, WTransformStage}

/**
 * Scores confidence that a scraped product matches the input EAN catalog entry.
 *
 * Arg positions (all optional, with defaults):
 *   0 - ean_input_field   (default: "ean")        field holding the reference EAN
 *   1 - ean_found_field   (default: "pc_ean_code") field holding the EAN scraped from the page
 *   2 - title_input_field (default: "product_name") reference product title
 *   3 - title_found_field (default: "pc_title")    title scraped from the page
 *   4 - strategy          (default: "tiered")      only "tiered" is currently implemented
 *
 * Output: sets "match_confidence" (Double in [0.0, 1.0]) on the row.
 *
 * Tiered logic:
 *   1. Exact EAN match (normalized, digits only) → 0.95
 *   2. Title Jaccard similarity → mapped to [0.50, 0.85]
 *   3. No evidence → 0.0
 */
class PcMatchScorerStage extends WTransformStage {

  override val name: String = "pc_match_scorer"

  override def transform(row: WRow, args: WArgs): WRow = {
    val eanInputField   = args.string(0, "ean")
    val eanFoundField   = args.string(1, "pc_ean_code")
    val titleInputField = args.string(2, "product_name")
    val titleFoundField = args.string(3, "pc_title")

    val confidence = scoreConfidence(row, eanInputField, eanFoundField, titleInputField, titleFoundField)
    row.set("match_confidence", confidence)
  }

  private def scoreConfidence(
      row: WRow,
      eanInputField: String,
      eanFoundField: String,
      titleInputField: String,
      titleFoundField: String
  ): Double = {
    val eanInput = row.str(eanInputField).map(normalizeEan).getOrElse("")
    val eanFound = row.str(eanFoundField).map(normalizeEan).getOrElse("")

    if (eanInput.nonEmpty && eanFound.nonEmpty && eanInput == eanFound)
      return 0.95

    val titleInput = row.str(titleInputField).getOrElse("")
    val titleFound = row.str(titleFoundField).getOrElse("")

    if (titleInput.nonEmpty && titleFound.nonEmpty) {
      val sim = jaccardSimilarity(titleInput, titleFound)
      // map [0.0, 1.0] → [0.50, 0.85]
      return 0.50 + sim * 0.35
    }

    0.0
  }

  private def normalizeEan(s: String): String =
    s.trim.replaceAll("[^0-9]", "")

  private def tokenize(s: String): Seq[String] =
    s.toLowerCase.split("[^a-z0-9]+").filter(_.length >= 2).toSeq

  private def jaccardSimilarity(a: String, b: String): Double = {
    val tokA = tokenize(a).toSet
    val tokB = tokenize(b).toSet
    if (tokA.isEmpty && tokB.isEmpty) return 0.0
    val intersection = tokA.intersect(tokB).size.toDouble
    val union        = tokA.union(tokB).size.toDouble
    if (union == 0.0) 0.0 else intersection / union
  }
}
