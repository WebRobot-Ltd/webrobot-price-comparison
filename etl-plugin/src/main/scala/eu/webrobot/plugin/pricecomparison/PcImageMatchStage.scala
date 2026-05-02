package eu.webrobot.plugin.pricecomparison

import eu.webrobot.plugin.sdk.{WArgs, WRow, WTransformStage}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.util.regex.Pattern

/**
 * Third tier of match scoring: compares product images via Groq vision LLM.
 * Skipped if existing match_confidence is already >= min_uncertainty (default 0.90).
 *
 * Reads GROQ_API_KEY from the environment (injected by KubernetesJobCloud via cloud credentials).
 * Model is resolved from GROQ_VISION_MODEL or GROQ_MODEL env vars; falls back to
 * meta-llama/llama-4-scout-17b-16e-instruct (multimodal).
 *
 * Arg positions (all optional):
 *   0 - ref_image_field     (default: "ref_image_url")     reference catalog image URL
 *   1 - scraped_image_field (default: "pc_image_url")      competitor page image URL
 *   2 - min_uncertainty     (default: "0.90")              skip if confidence already above this
 *   3 - model               (default: "")                  override vision model name
 *
 * Output fields set on the row:
 *   image_match_confidence  Double in [0.0, 1.0]
 *   match_confidence        updated to max(existing, image_match_confidence)
 *   match_method            set to "image_llm" if image score is the winner
 */
class PcImageMatchStage extends WTransformStage {

  override val name: String = "pc_image_match_stage"

  @transient private lazy val http: HttpClient =
    HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(10))
      .build()

  private val confidencePattern = Pattern.compile(
    "\"confidence\"\\s*:\\s*([0-9]*\\.?[0-9]+)"
  )

  override def transform(row: WRow, args: WArgs): WRow = {
    val refImageField     = args.string(0, "ref_image_url")
    val scrapedImageField = args.string(1, "pc_image_url")
    val minUncertainty    = args.double(2, 0.90)
    val modelOverride     = args.string(3, "")

    val currentConfidence = row.double("match_confidence").getOrElse(0.0)
    if (currentConfidence >= minUncertainty)
      return row

    val refUrl     = row.str(refImageField).getOrElse("")
    val scrapedUrl = row.str(scrapedImageField).getOrElse("")

    if (refUrl.isEmpty || scrapedUrl.isEmpty)
      return row

    val apiKey = sys.env.getOrElse("GROQ_API_KEY", "")
    if (apiKey.isEmpty)
      return row

    val model = resolveModel(modelOverride)
    val imageScore = callGroqVision(apiKey, model, refUrl, scrapedUrl)

    val updatedConfidence = math.max(currentConfidence, imageScore)
    val updatedMethod     = if (imageScore > currentConfidence) "image_llm"
                            else row.str("match_method").getOrElse("")

    row
      .set("image_match_confidence", imageScore)
      .set("match_confidence", updatedConfidence)
      .set("match_method", updatedMethod)
  }

  private def resolveModel(override_ : String): String =
    if (override_.nonEmpty) override_
    else sys.env.getOrElse("GROQ_VISION_MODEL",
         sys.env.getOrElse("GROQ_MODEL",
         "meta-llama/llama-4-scout-17b-16e-instruct"))

  private def callGroqVision(apiKey: String, model: String, refUrl: String, scrapedUrl: String): Double = {
    val body = buildRequestBody(model, refUrl, scrapedUrl)
    val request = HttpRequest.newBuilder()
      .uri(URI.create("https://api.groq.com/openai/v1/chat/completions"))
      .header("Authorization", s"Bearer $apiKey")
      .header("Content-Type", "application/json")
      .timeout(Duration.ofSeconds(30))
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()

    try {
      val response = http.send(request, HttpResponse.BodyHandlers.ofString())
      if (response.statusCode() == 200) parseConfidence(response.body())
      else 0.0
    } catch {
      case _: Exception => 0.0
    }
  }

  private def buildRequestBody(model: String, refUrl: String, scrapedUrl: String): String = {
    val safeModel      = escapeJson(model)
    val safeRefUrl     = escapeJson(refUrl)
    val safeScrapedUrl = escapeJson(scrapedUrl)
    s"""{
       |  "model": "$safeModel",
       |  "messages": [{
       |    "role": "user",
       |    "content": [
       |      {"type": "text", "text": "Do these two images show the same product? Reply ONLY with valid JSON, no other text: {\\"match\\": true, \\"confidence\\": 0.0}"},
       |      {"type": "image_url", "image_url": {"url": "$safeRefUrl"}},
       |      {"type": "image_url", "image_url": {"url": "$safeScrapedUrl"}}
       |    ]
       |  }],
       |  "max_tokens": 64,
       |  "temperature": 0
       |}""".stripMargin
  }

  private def parseConfidence(responseBody: String): Double = {
    val m = confidencePattern.matcher(responseBody)
    if (m.find()) {
      try m.group(1).toDouble.min(1.0).max(0.0)
      catch { case _: NumberFormatException => 0.0 }
    } else 0.0
  }

  private def escapeJson(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r")
}
