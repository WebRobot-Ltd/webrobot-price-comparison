package eu.webrobot.plugin.pricecomparison

import eu.webrobot.plugin.sdk.{WArgs, WRow, WSinkStage, WebroStageContext}

/**
 * UPSERT into pc_matches (unique on organization_id, ean, competitor_site).
 * Writes "match_id" (Long) back onto the row for downstream pc_save_price.
 *
 * Arg positions (all optional):
 *   0 - ean_field        (default: "ean")
 *   1 - url_field        (default: "result_link")
 *   2 - site_field       (default: "competitor_site")
 *   3 - confidence_field (default: "match_confidence")
 *
 * organization_id resolved from row field "org_id" / "organization_id",
 * then from ctx.config("webrobot.org.id").
 *
 * match_method is read from the row (set by pc_match_scorer / pc_image_match_stage)
 * and falls back to a threshold-based derivation only when absent.
 */
class PcSaveMatchStage extends WSinkStage {

  override val name: String = "pc_save_match"

  private val upsertSql =
    """INSERT INTO pc_matches
      |  (organization_id, ean, competitor_site, product_url, product_title,
      |   match_confidence, match_method, last_verified_at, updated_at)
      |VALUES (?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
      |ON CONFLICT (organization_id, ean, competitor_site) DO UPDATE
      |  SET product_url      = EXCLUDED.product_url,
      |      product_title    = COALESCE(EXCLUDED.product_title, pc_matches.product_title),
      |      match_confidence = EXCLUDED.match_confidence,
      |      match_method     = EXCLUDED.match_method,
      |      last_verified_at = NOW(),
      |      updated_at       = NOW()""".stripMargin

  private val selectIdSql =
    "SELECT id FROM pc_matches WHERE organization_id = ? AND ean = ? AND competitor_site = ?"

  override def consume(row: WRow, args: WArgs, ctx: WebroStageContext): WRow = {
    val eanField        = args.string(0, "ean")
    val urlField        = args.string(1, "result_link")
    val siteField       = args.string(2, "competitor_site")
    val confidenceField = args.string(3, "match_confidence")

    val orgId      = resolveOrgId(row, ctx)
    val ean        = row.str(eanField).getOrElse(throw new IllegalStateException(s"Field '$eanField' missing"))
    val url        = row.str(urlField).getOrElse(throw new IllegalStateException(s"Field '$urlField' missing"))
    val site       = row.str(siteField).getOrElse(throw new IllegalStateException(s"Field '$siteField' missing"))
    val confidence = row.double(confidenceField).getOrElse(0.0)
    val title      = row.str("pc_title").orElse(row.str("product_name")).orNull

    // Prefer match_method already set by scoring stages; derive only as fallback
    val method = row.str("match_method").filter(_.nonEmpty).getOrElse(deriveMethod(confidence))

    val upsertParams = Seq[Any](orgId, ean, site, url, title, confidence, method)
    ctx.execute(upsertSql, upsertParams)

    // Separate SELECT to retrieve the id — avoids relying on RETURNING via ctx.query()
    val selectParams = Seq[Any](orgId, ean, site)
    val result  = ctx.query(selectIdSql, selectParams)
    val matchId = if (result.hasNext) result.next().get("id").map(_.toString.toLong).getOrElse(0L) else 0L

    row.set("match_id", matchId)
  }

  private def resolveOrgId(row: WRow, ctx: WebroStageContext): Long =
    row.get("org_id").orElse(row.get("organization_id"))
      .map(_.toString.toLong)
      .orElse {
        val v = ctx.config("webrobot.org.id")
        if (v.nonEmpty) Some(v.toLong) else None
      }
      .getOrElse(throw new IllegalStateException(
        "organization_id not found in row ('org_id'/'organization_id') or config 'webrobot.org.id'"))

  private def deriveMethod(confidence: Double): String =
    if (confidence >= 0.90) "ean_exact"
    else if (confidence >= 0.50) "title_sim"
    else "unknown"
}
