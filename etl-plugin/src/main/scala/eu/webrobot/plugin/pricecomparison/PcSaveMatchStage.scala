package eu.webrobot.plugin.pricecomparison

import eu.webrobot.plugin.sdk.{WArgs, WRow, WSinkStage, WebroStageContext}

/**
 * Persists a confirmed product match into pc_matches via UPSERT.
 * Returns the row with "match_id" field set to the database row id.
 *
 * Arg positions (all optional, with defaults):
 *   0 - ean_field        (default: "ean")              row field holding the EAN
 *   1 - url_field        (default: "product_url")      competitor product URL
 *   2 - site_field       (default: "competitor_site")  competitor domain/site key
 *   3 - confidence_field (default: "match_confidence") match confidence score
 *
 * Reads "org_id" from the row; falls back to Spark conf key "webrobot.org.id".
 * Reads "pc_title" or "product_name" from the row as product_title.
 *
 * UNIQUE key: (organization_id, ean, competitor_site) — conflict updates price/confidence.
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
      |      updated_at       = NOW()
      |RETURNING id""".stripMargin

  override def consume(row: WRow, args: WArgs, ctx: WebroStageContext): WRow = {
    val eanField        = args.string(0, "ean")
    val urlField        = args.string(1, "product_url")
    val siteField       = args.string(2, "competitor_site")
    val confidenceField = args.string(3, "match_confidence")

    val orgId      = resolveOrgId(row, ctx)
    val ean        = row.str(eanField).getOrElse(throw new IllegalStateException(s"Field '$eanField' missing from row"))
    val url        = row.str(urlField).getOrElse(throw new IllegalStateException(s"Field '$urlField' missing from row"))
    val site       = row.str(siteField).getOrElse(throw new IllegalStateException(s"Field '$siteField' missing from row"))
    val confidence = row.double(confidenceField).getOrElse(0.0)
    val title      = row.str("pc_title").orElse(row.str("product_name")).orNull
    val method     = matchMethod(confidence)

    val params = Seq[Any](orgId, ean, site, url, title, confidence, method)
    val result = ctx.query(upsertSql, params)

    val matchId = if (result.hasNext) result.next().get("id").map(_.toString.toLong).getOrElse(0L) else 0L
    row.set("match_id", matchId)
  }

  private def resolveOrgId(row: WRow, ctx: WebroStageContext): Long =
    row.get("org_id").orElse(row.get("organization_id"))
      .map(_.toString.toLong)
      .orElse {
        val cfgVal = ctx.config("webrobot.org.id")
        if (cfgVal.nonEmpty) Some(cfgVal.toLong) else None
      }
      .getOrElse(throw new IllegalStateException(
        "organization_id not found in row fields ('org_id'/'organization_id') or config 'webrobot.org.id'"))

  private def matchMethod(confidence: Double): String =
    if (confidence >= 0.90) "ean_exact"
    else if (confidence >= 0.50) "title_sim"
    else "unknown"
}
