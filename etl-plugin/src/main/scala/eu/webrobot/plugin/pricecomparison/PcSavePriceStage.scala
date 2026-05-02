package eu.webrobot.plugin.pricecomparison

import eu.webrobot.plugin.sdk.{WArgs, WRow, WSinkStage, WebroStageContext}

/**
 * Inserts a scraped price observation into pc_price_history.
 * Row passes through unchanged.
 *
 * Arg positions (all optional):
 *   0 - match_id_field     (default: "match_id")
 *   1 - price_field        (default: "pc_price")
 *   2 - currency_field     (default: "pc_currency")
 *   3 - availability_field (default: "pc_availability")
 *
 * Requires match_id > 0 in the row — set by pc_save_match.
 * Also reads org_id, ean, competitor_site, discount_pct from the row if present.
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

    val matchId = resolveMatchId(row, matchIdField)
    if (matchId <= 0)
      throw new IllegalStateException(
        s"match_id must be > 0 — ensure pc_save_match ran successfully before pc_save_price (got $matchId)")

    val orgId        = resolveOrgId(row, ctx)
    val ean          = row.str("ean").getOrElse("")
    val site         = row.str("competitor_site").getOrElse("")
    val price        = row.double(priceField).orNull
    val currency     = row.str(currencyField).orNull
    val availability = row.str(availabilityField).getOrElse("unknown")
    val discountPct  = row.double("discount_pct").orNull

    if (price == null)
      ctx.warn(s"pc_save_price: price field '$priceField' is missing for match_id=$matchId — inserting NULL")

    val params = Seq[Any](matchId, orgId, ean, site, price, currency, availability, discountPct)
    val rowsAffected = ctx.execute(insertSql, params)
    if (rowsAffected != 1)
      ctx.warn(s"pc_save_price: expected 1 row inserted for match_id=$matchId, got $rowsAffected")

    row
  }

  private def resolveMatchId(row: WRow, field: String): Long =
    row.get(field) match {
      case Some(v) if v != null && v.toString.nonEmpty =>
        scala.util.Try(v.toString.toLong).getOrElse(
          throw new IllegalStateException(s"Field '$field' is not a valid Long: '${v.toString}'"))
      case _ =>
        throw new IllegalStateException(
          s"Field '$field' is missing or empty — run pc_save_match before pc_save_price")
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
}
