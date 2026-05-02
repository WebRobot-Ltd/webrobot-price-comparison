package eu.webrobot.plugin.pricecomparison

import eu.webrobot.plugin.sdk.{WArgs, WRow, WSinkStage, WebroStageContext}

/**
 * Inserts a scraped price observation into pc_price_history.
 * The row passes through unchanged (rows are appended, not mutated by price history).
 *
 * Arg positions (all optional, with defaults):
 *   0 - match_id_field     (default: "match_id")        field holding the pc_matches FK
 *   1 - price_field        (default: "pc_price")        scraped price (numeric)
 *   2 - currency_field     (default: "pc_currency")     ISO 4217 currency code
 *   3 - availability_field (default: "pc_availability") in_stock | out_of_stock | unknown
 *
 * Also reads "org_id"/"organization_id", "ean", "competitor_site" from the row.
 * Reads "discount_pct" from the row if present.
 * Sets "scrape_status" to "ok" on success, "error" on exception (re-throws after recording).
 */
class PcSavePriceStage extends WSinkStage {

  override val name: String = "pc_save_price"

  private val insertSql =
    """INSERT INTO pc_price_history
      |  (match_id, organization_id, ean, competitor_site,
      |   price, currency, availability, discount_pct, scrape_status, scraped_at)
      |VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ok', NOW())""".stripMargin

  override def consume(row: WRow, args: WArgs, ctx: WebroStageContext): WRow = {
    val matchIdField      = args.string(0, "match_id")
    val priceField        = args.string(1, "pc_price")
    val currencyField     = args.string(2, "pc_currency")
    val availabilityField = args.string(3, "pc_availability")

    val matchId      = row.get(matchIdField).map(_.toString.toLong)
      .getOrElse(throw new IllegalStateException(s"Field '$matchIdField' missing — run pc_save_match before pc_save_price"))
    val orgId        = resolveOrgId(row, ctx)
    val ean          = row.str("ean").getOrElse("")
    val site         = row.str("competitor_site").getOrElse("")
    val price        = row.double(priceField).orNull
    val currency     = row.str(currencyField).orNull
    val availability = row.str(availabilityField).getOrElse("unknown")
    val discountPct  = row.double("discount_pct").orNull

    val params = Seq[Any](matchId, orgId, ean, site, price, currency, availability, discountPct)
    ctx.execute(insertSql, params)

    row
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
}
