package eu.webrobot.plugin.pricecomparison

import eu.webrobot.plugin.sdk.{WArgs, WRow, WSourceStage, WebroStageContext}

/**
 * Loads all active matches from pc_matches for the current organization.
 *
 * Used as the source stage in the monitoring pipeline — replaces reading all
 * URLs into memory on the REST API side. The query runs inside the Spark job,
 * keeping the data fresh at execution time and scaling to any number of matches.
 *
 * Pipeline position: immediately after load_csv (single trigger row).
 * Replaces the seed row with one WRow per active match.
 *
 * Each produced row contains:
 *   match_id, organization_id, org_id, ean, competitor_site, product_url
 *
 * Arg positions (all optional):
 *   0 - min_confidence  (default: 0.0) — filter out low-confidence matches
 */
class PcLoadMatchesStage extends WSourceStage {

  override val name: String = "pc_load_matches"

  private val selectSql =
    """SELECT id AS match_id, organization_id, ean, competitor_site, product_url,
      |       match_confidence
      |FROM pc_matches
      |WHERE organization_id = ? AND is_active = TRUE
      |ORDER BY ean, competitor_site""".stripMargin

  override def produce(args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val minConfidence = args.double(0, 0.0)
    val orgId         = resolveOrgId(ctx)

    // Materialize eagerly — ctx.query is backed by a live JDBC ResultSet;
    // consuming it lazily across Spark partition boundaries risks reading from a closed connection.
    val rows = ctx.query(selectSql, Seq[Any](orgId))
      .filter { r =>
        val conf = r.double("match_confidence").getOrElse(0.0)
        conf >= minConfidence
      }
      .map { r =>
        WRow(Map(
          "match_id"        -> r.str("match_id").getOrElse("0"),
          "organization_id" -> orgId.toString,
          "org_id"          -> orgId.toString,
          "ean"             -> r.str("ean").getOrElse(""),
          "competitor_site" -> r.str("competitor_site").getOrElse(""),
          "product_url"     -> r.str("product_url").getOrElse(""),
          "match_confidence"-> r.double("match_confidence").getOrElse(0.0)
        ))
      }
      .toList   // eager — close ResultSet before returning

    if (rows.isEmpty)
      ctx.warn(s"pc_load_matches: no active matches for org $orgId (min_confidence=$minConfidence)")
    else
      ctx.log(s"pc_load_matches: loaded ${rows.size} matches for org $orgId")

    rows.iterator
  }

  private def resolveOrgId(ctx: WebroStageContext): Long = {
    val v = ctx.config("webrobot.org.id")
    if (v.isEmpty)
      throw new IllegalStateException(
        "webrobot.org.id not configured — cannot load matches without org context")
    scala.util.Try(v.toLong).getOrElse(
      throw new IllegalStateException(s"webrobot.org.id is not a valid Long: '$v'"))
  }
}
