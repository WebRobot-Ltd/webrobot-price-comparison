package org.webrobot.eu.apis.jersey.pricecomparison.plugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.webrobot.eu.kernel.common.domain.orm.*;
import org.webrobot.eu.kernel.common.persistence.mybatis.service.*;
import org.webrobot.eu.kernel.common.domain.orm.ExecutionStatus;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

/**
 * Business logic for the price comparison plugin.
 *
 * Two ETL pipelines are managed per organization:
 *
 *   DISCOVERY — phase 1:
 *     Input CSV: ean, product_name, brand, ref_image_url, competitor_site, org_id
 *     Stages: searchEngine → visit → intelligentExtract → pc_match_scorer → pc_image_match_stage → pc_save_match
 *
 *   MONITORING — phase 2:
 *     Input CSV: match_id, ean, competitor_site, product_url, org_id
 *     Stages: visit → intelligentExtract → pc_save_price
 *
 * Input CSVs are built from DB state and uploaded to MinIO before each job run.
 */
public class PriceComparisonService {

    private static final Logger logger = LoggerFactory.getLogger(PriceComparisonService.class);

    private final MyBatisProjectService  projectService;
    private final MyBatisAgentService    agentService;
    private final MyBatisJobService      jobService;
    private final MyBatisDatasetService  datasetService;
    private final MyBatisCloudCredentialService credentialService;

    private static final String PROJECT_NAME_PREFIX   = "PC-Monitor";
    private static final String AGENT_DISCOVERY_SUFFIX   = "-discovery";
    private static final String AGENT_MONITORING_SUFFIX  = "-monitoring";

    // ── Pipeline YAMLs ────────────────────────────────────────────────────────

    /**
     * Phase 1: search each competitor for the EAN, extract data, score the match, persist confirmed match.
     * Credentials (GOOGLE_SEARCH_API_KEY, GROQ_API_KEY) are injected as env vars by KubernetesJobCloud.
     */
    private static final String DISCOVERY_PIPELINE_YAML =
        "pipeline:\n" +
        "  # Load product x competitor pairs from MinIO CSV\n" +
        "  - stage: load_csv\n" +
        "    args:\n" +
        "      - path: \"${INPUT_CSV_PATH}\"\n" +
        "        header: \"true\"\n" +
        "  # Search competitor site for the EAN using Google (site: operator)\n" +
        "  - stage: searchEngine\n" +
        "    args:\n" +
        "      - provider: \"google\"\n" +
        "        query: \"${ean} ${product_name} site:${competitor_site}\"\n" +
        "        num_results: 3\n" +
        "        enrich: false\n" +
        "  # Visit the top search result\n" +
        "  - stage: visit\n" +
        "    args:\n" +
        "      - \"$result_link\"\n" +
        "  # Extract structured product data from the page\n" +
        "  - stage: iextract\n" +
        "    args:\n" +
        "      - selector: \"body\"\n" +
        "        method: \"code\"\n" +
        "      - \"Extract from this e-commerce product page: EAN or GTIN code if visible (field: pc_ean_code), " +
              "product title (field: pc_title), current price as a number without currency symbol (field: pc_price), " +
              "currency ISO code EUR/USD/GBP (field: pc_currency), " +
              "availability as in_stock or out_of_stock or unknown (field: pc_availability), " +
              "main product image URL (field: pc_image_url). " +
              "If a field is not found return empty string. Preserve all input fields.\"\n" +
        "      - \"pc_\"\n" +
        "  # Tier 1+2 scoring: EAN exact match or title Jaccard similarity\n" +
        "  - stage: pc_match_scorer\n" +
        "    args: []\n" +
        "  # Tier 3: Groq vision LLM image comparison (skipped if already confident)\n" +
        "  - stage: pc_image_match_stage\n" +
        "    args: []\n" +
        "  # Persist confirmed match\n" +
        "  - stage: pc_save_match\n" +
        "    args:\n" +
        "      - \"ean\"\n" +
        "      - \"result_link\"\n" +
        "      - \"competitor_site\"\n" +
        "      - \"match_confidence\"\n" +
        "output:\n" +
        "  format: \"parquet\"\n" +
        "  path: \"${OUTPUT_PARQUET_PATH}\"\n" +
        "  mode: \"overwrite\"\n";

    /**
     * Phase 2: re-visit saved match URLs to collect updated prices.
     */
    private static final String MONITORING_PIPELINE_YAML =
        "pipeline:\n" +
        "  # Load active matches (match_id, ean, competitor_site, product_url, org_id)\n" +
        "  - stage: load_csv\n" +
        "    args:\n" +
        "      - path: \"${INPUT_CSV_PATH}\"\n" +
        "        header: \"true\"\n" +
        "  # Re-visit stored product URL\n" +
        "  - stage: visit\n" +
        "    args:\n" +
        "      - \"$product_url\"\n" +
        "  # Extract current price/availability\n" +
        "  - stage: iextract\n" +
        "    args:\n" +
        "      - selector: \"body\"\n" +
        "        method: \"code\"\n" +
        "      - \"Extract from this product page: current price as a number without currency symbol (field: pc_price), " +
              "currency ISO code EUR/USD/GBP (field: pc_currency), " +
              "availability as in_stock or out_of_stock or unknown (field: pc_availability). " +
              "If a field is not found return empty string. Preserve all input fields.\"\n" +
        "      - \"pc_\"\n" +
        "  # Persist price observation into pc_price_history\n" +
        "  - stage: pc_save_price\n" +
        "    args: []\n" +
        "output:\n" +
        "  format: \"parquet\"\n" +
        "  path: \"${OUTPUT_PARQUET_PATH}\"\n" +
        "  mode: \"overwrite\"\n";

    public PriceComparisonService() {
        this.projectService    = new MyBatisProjectService();
        this.agentService      = new MyBatisAgentService();
        this.jobService        = new MyBatisJobService();
        this.datasetService    = new MyBatisDatasetService();
        this.credentialService = new MyBatisCloudCredentialService();
    }

    // ── Bootstrap ─────────────────────────────────────────────────────────────

    public Map<String, Object> ensureSetup(String orgId) throws Exception {
        Project project          = ensureProject(orgId);
        Agent   discoveryAgent   = ensureAgent(project, AGENT_DISCOVERY_SUFFIX,  DISCOVERY_PIPELINE_YAML);
        Agent   monitoringAgent  = ensureAgent(project, AGENT_MONITORING_SUFFIX, MONITORING_PIPELINE_YAML);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("project_id",          project.getId());
        result.put("discovery_agent_id",  discoveryAgent.getId());
        result.put("monitoring_agent_id", monitoringAgent.getId());
        result.put("status", "ready");
        return result;
    }

    private Project ensureProject(String orgId) {
        String name = PROJECT_NAME_PREFIX + "-org-" + orgId;
        Project existing = projectService.findByName(name);
        if (existing != null) return existing;

        Project p = new Project();
        p.setName(name);
        p.setOrganizationId(orgId);
        p.setDescription("Price comparison monitoring — auto-created by plugin");
        p = projectService.create(p);
        logger.info("Created price-comparison project {} for org {}", p.getId(), orgId);
        return p;
    }

    private Agent ensureAgent(Project project, String suffix, String pipelineYaml) {
        String agentName = PROJECT_NAME_PREFIX + suffix + "-org-" + project.getOrganizationId();
        Agent existing = agentService.findByName(agentName);
        if (existing != null) return existing;

        Agent a = new Agent();
        a.setName(agentName);
        a.setOrganizationId(project.getOrganizationId());
        a.setPipelineYaml(pipelineYaml);
        a = agentService.create(a);
        logger.info("Created agent {} (project {})", a.getId(), project.getId());
        return a;
    }

    // ── Product catalog ───────────────────────────────────────────────────────

    public List<Map<String, Object>> listProducts(String orgId) throws Exception {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "SELECT id, ean, product_name, brand, image_url, created_at, updated_at " +
                "FROM pc_ean_catalog WHERE organization_id = ? ORDER BY ean")) {
            ps.setLong(1, Long.parseLong(orgId));
            return resultSetToList(ps.executeQuery());
        }
    }

    public Map<String, Object> upsertProduct(String orgId, String ean, String productName,
                                              String brand, String imageUrl) throws Exception {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO pc_ean_catalog (organization_id, ean, product_name, brand, image_url, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, NOW()) " +
                "ON CONFLICT (organization_id, ean) DO UPDATE " +
                "  SET product_name = EXCLUDED.product_name, " +
                "      brand        = EXCLUDED.brand, " +
                "      image_url    = COALESCE(EXCLUDED.image_url, pc_ean_catalog.image_url), " +
                "      updated_at   = NOW() " +
                "RETURNING id, ean, product_name, brand, image_url")) {
            ps.setLong(1, Long.parseLong(orgId));
            ps.setString(2, ean);
            ps.setString(3, productName);
            ps.setString(4, brand);
            ps.setString(5, imageUrl.isEmpty() ? null : imageUrl);
            List<Map<String, Object>> rows = resultSetToList(ps.executeQuery());
            return rows.isEmpty() ? Map.of("ean", ean) : rows.get(0);
        }
    }

    public int deleteProduct(String orgId, String ean) throws Exception {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM pc_ean_catalog WHERE organization_id = ? AND ean = ?")) {
            ps.setLong(1, Long.parseLong(orgId));
            ps.setString(2, ean);
            return ps.executeUpdate();
        }
    }

    // ── Competitors ───────────────────────────────────────────────────────────

    public List<Map<String, Object>> listCompetitors(String orgId) throws Exception {
        // Only return active competitors — soft-deleted ones are excluded
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "SELECT id, site_domain, site_name, country_code, created_at " +
                "FROM pc_competitors WHERE organization_id = ? AND is_active = TRUE ORDER BY site_domain")) {
            ps.setLong(1, Long.parseLong(orgId));
            return resultSetToList(ps.executeQuery());
        }
    }

    public Map<String, Object> upsertCompetitor(String orgId, String domain,
                                                  String siteName, String countryCode) throws Exception {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO pc_competitors (organization_id, site_domain, site_name, country_code) " +
                "VALUES (?, ?, ?, ?) " +
                "ON CONFLICT (organization_id, site_domain) DO UPDATE " +
                "  SET site_name    = EXCLUDED.site_name, " +
                "      country_code = EXCLUDED.country_code, " +
                "      is_active    = TRUE " +
                "RETURNING id, site_domain, site_name, country_code, is_active")) {
            ps.setLong(1, Long.parseLong(orgId));
            ps.setString(2, domain);
            ps.setString(3, siteName);
            ps.setString(4, countryCode.isEmpty() ? null : countryCode);
            List<Map<String, Object>> rows = resultSetToList(ps.executeQuery());
            return rows.isEmpty() ? Map.of("site_domain", domain) : rows.get(0);
        }
    }

    public int deleteCompetitor(String orgId, long id) throws Exception {
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "UPDATE pc_competitors SET is_active = FALSE " +
                "WHERE organization_id = ? AND id = ?")) {
            ps.setLong(1, Long.parseLong(orgId));
            ps.setLong(2, id);
            return ps.executeUpdate();
        }
    }

    // ── Discovery job ─────────────────────────────────────────────────────────

    public Map<String, Object> runDiscoveryJob(String orgId, List<String> credentialIds) throws Exception {
        // Build cross-join CSV: every EAN × every active competitor
        List<Map<String, Object>> products    = listProducts(orgId);
        List<Map<String, Object>> competitors = listCompetitors(orgId);

        if (products.isEmpty())
            throw new IllegalStateException("No products configured for org " + orgId);
        if (competitors.isEmpty())
            throw new IllegalStateException("No competitors configured for org " + orgId);

        StringBuilder csv = new StringBuilder("ean,product_name,brand,ref_image_url,competitor_site,org_id\n");
        for (Map<String, Object> p : products) {
            for (Map<String, Object> c : competitors) {
                if (!Boolean.TRUE.equals(c.get("is_active"))) continue;
                csv.append(csvQuote(str(p, "ean"))).append(",")
                   .append(csvQuote(str(p, "product_name"))).append(",")
                   .append(csvQuote(str(p, "brand"))).append(",")
                   .append(csvQuote(str(p, "image_url"))).append(",")
                   .append(csvQuote(str(c, "site_domain"))).append(",")
                   .append(orgId).append("\n");
            }
        }

        String timestamp   = String.valueOf(System.currentTimeMillis());
        String csvPath     = "s3a://sparklogs-data/pc-monitor/org-" + orgId + "/discovery-" + timestamp + "/input.csv";
        String outputPath  = "s3a://sparklogs-data/pc-monitor/org-" + orgId + "/discovery-" + timestamp + "/output";

        uploadCsvToMinIO(csvPath, csv.toString());

        return submitJob(orgId, "discovery", csvPath, outputPath,
                         DISCOVERY_PIPELINE_YAML, credentialIds, timestamp);
    }

    // ── Monitoring job ────────────────────────────────────────────────────────

    public Map<String, Object> runMonitoringJob(String orgId, List<String> credentialIds) throws Exception {
        List<Map<String, Object>> matches = getMatches(orgId, null, null);

        if (matches.isEmpty())
            throw new IllegalStateException("No active matches for org " + orgId + " — run discovery first");

        StringBuilder csv = new StringBuilder("match_id,ean,competitor_site,product_url,org_id\n");
        for (Map<String, Object> m : matches) {
            // getMatches aliases id AS match_id — use that key for clarity
            csv.append(str(m, "match_id")).append(",")
               .append(csvQuote(str(m, "ean"))).append(",")
               .append(csvQuote(str(m, "competitor_site"))).append(",")
               .append(csvQuote(str(m, "product_url"))).append(",")
               .append(orgId).append("\n");
        }

        String timestamp  = String.valueOf(System.currentTimeMillis());
        String csvPath    = "s3a://sparklogs-data/pc-monitor/org-" + orgId + "/monitoring-" + timestamp + "/input.csv";
        String outputPath = "s3a://sparklogs-data/pc-monitor/org-" + orgId + "/monitoring-" + timestamp + "/output";

        uploadCsvToMinIO(csvPath, csv.toString());

        return submitJob(orgId, "monitoring", csvPath, outputPath,
                         MONITORING_PIPELINE_YAML, credentialIds, timestamp);
    }

    // ── Prices & matches ──────────────────────────────────────────────────────

    public List<Map<String, Object>> getCurrentPrices(String orgId, String ean,
                                                       String site, int limit) throws Exception {
        StringBuilder sql = new StringBuilder(
            "SELECT match_id, ean, competitor_site, product_url, product_title, " +
            "match_confidence, price, currency, availability, scraped_at " +
            "FROM pc_current_prices WHERE organization_id = ?");
        List<Object> params = new ArrayList<>();
        params.add(Long.parseLong(orgId));

        if (ean != null && !ean.isBlank()) { sql.append(" AND ean = ?"); params.add(ean); }
        if (site != null && !site.isBlank()) { sql.append(" AND competitor_site = ?"); params.add(site); }
        sql.append(" ORDER BY ean, price ASC LIMIT ?");
        params.add(limit);

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            return resultSetToList(ps.executeQuery());
        }
    }

    public List<Map<String, Object>> getMatches(String orgId, String ean, String site) throws Exception {
        StringBuilder sql = new StringBuilder(
            // Alias id AS match_id so callers use a stable, intention-revealing key
            "SELECT id AS match_id, ean, competitor_site, product_url, product_title, " +
            "match_confidence, match_method, last_verified_at " +
            "FROM pc_matches WHERE organization_id = ? AND is_active = TRUE");
        List<Object> params = new ArrayList<>();
        params.add(Long.parseLong(orgId));

        if (ean != null && !ean.isBlank()) { sql.append(" AND ean = ?"); params.add(ean); }
        if (site != null && !site.isBlank()) { sql.append(" AND competitor_site = ?"); params.add(site); }
        sql.append(" ORDER BY ean, competitor_site");

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            return resultSetToList(ps.executeQuery());
        }
    }

    // ── Internal: job submission ──────────────────────────────────────────────

    private Map<String, Object> submitJob(String orgId, String phase, String csvPath,
                                           String outputPath, String pipelineYaml,
                                           List<String> credentialIds, String timestamp) throws Exception {
        // Resolve project + agent
        String projectName = PROJECT_NAME_PREFIX + "-org-" + orgId;
        Project project = projectService.findByName(projectName);
        if (project == null)
            throw new IllegalStateException("Setup not done — call POST /bootstrap first");

        String agentSuffix = "discovery".equals(phase) ? AGENT_DISCOVERY_SUFFIX : AGENT_MONITORING_SUFFIX;
        String agentName   = PROJECT_NAME_PREFIX + agentSuffix + "-org-" + orgId;
        Agent agent = agentService.findByName(agentName);
        if (agent == null)
            throw new IllegalStateException("Agent not found — call POST /bootstrap first");

        // Create Dataset (pointing to the MinIO CSV)
        Dataset dataset = new Dataset();
        dataset.setName("pc-" + phase + "-" + orgId + "-" + timestamp);
        dataset.setOrganizationId(orgId);
        dataset.setStoragePath(csvPath);
        dataset.setCreatedAt(new java.util.Date());
        dataset = datasetService.create(dataset);

        // Create Job
        Job job = new Job();
        job.setName("pc-" + phase + "-" + orgId + "-" + timestamp);
        job.setProject(project);
        job.setAgent(agent);
        job.setInputDataset(dataset);
        job.setOrganizationId(orgId);
        job.setExecutionStatus(ExecutionStatus.PENDING);
        job = jobService.create(job);

        // Build SparkJobConfig and submit
        org.webrobot.eu.kernel.common.domain.SparkJobConfig sparkConfig =
            new org.webrobot.eu.kernel.common.domain.SparkJobConfig();
        toUUID(project.getId()).ifPresent(sparkConfig::setProjectId);
        toUUID(job.getId()).ifPresent(sparkConfig::setJobId);
        sparkConfig.setJobType("Python");
        sparkConfig.setOrganizationId(orgId);

        // Credentials
        List<UUID> credUuids = new ArrayList<>();
        for (String cid : credentialIds) {
            try { credUuids.add(UUID.fromString(cid)); } catch (Exception ignored) {}
        }
        if (!credUuids.isEmpty()) sparkConfig.setCloudCredentialIds(credUuids);

        // Spark args
        List<String> sparkArgs = new ArrayList<>(Arrays.asList(
            "--project-id",  project.getId().toString(),
            "--job-id",      job.getId().toString(),
            "--agent-id",    agent.getId().toString(),
            "--organization-id", orgId
        ));
        sparkConfig.setArgs(sparkArgs);

        // Resolve ETL JAR from DB (same pattern as EAN plugin)
        String buildType = System.getenv().getOrDefault("BUILD_TYPE", "development");
        String jarUrl = findLatestJarFromDatabase(buildType);
        if (jarUrl != null) sparkConfig.setJarDependencies(Collections.singletonList(jarUrl));

        org.webrobot.eu.kernel.spark.kube.KubernetesJobCloud kube =
            new org.webrobot.eu.kernel.spark.kube.KubernetesJobCloud();
        org.webrobot.eu.kernel.common.domain.SparkJobResult result =
            kube.executeJob(sparkConfig, "default-cluster");

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("job_id",     job.getId());
        response.put("dataset_id", dataset.getId());
        response.put("phase",      phase);
        response.put("csv_path",   csvPath);
        response.put("status",     result != null ? result.getStatus() : "submitted");
        return response;
    }

    // ── Internal: MinIO upload ────────────────────────────────────────────────

    private void uploadCsvToMinIO(String s3aPath, String csvContent) throws Exception {
        // Convert s3a://bucket/key to HTTP PUT against MinIO endpoint
        String minioEndpoint  = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://minio.webrobot.svc.cluster.local:9000");
        String accessKey      = System.getenv().getOrDefault("MINIO_ACCESS_KEY", "");
        String secretKey      = System.getenv().getOrDefault("MINIO_SECRET_KEY", "");

        // Parse s3a://bucket/key
        String withoutScheme = s3aPath.replaceFirst("^s3a://", "");
        int slash = withoutScheme.indexOf('/');
        String bucket = withoutScheme.substring(0, slash);
        String key    = withoutScheme.substring(slash + 1);

        String url = minioEndpoint.replaceAll("/+$", "") + "/" + bucket + "/" + key;
        byte[] data = csvContent.getBytes(StandardCharsets.UTF_8);

        java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();
        java.net.http.HttpRequest req = java.net.http.HttpRequest.newBuilder()
            .uri(java.net.URI.create(url))
            .header("Content-Type", "text/csv")
            // Basic auth for MinIO; in production KubernetesJobCloud injects AWS_ACCESS_KEY_ID etc.
            .header("Authorization", "Basic " + java.util.Base64.getEncoder()
                .encodeToString((accessKey + ":" + secretKey).getBytes(StandardCharsets.UTF_8)))
            .PUT(java.net.http.HttpRequest.BodyPublishers.ofByteArray(data))
            .build();

        java.net.http.HttpResponse<String> resp = http.send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400)
            throw new IOException("MinIO upload failed: HTTP " + resp.statusCode() + " — " + resp.body());

        logger.info("Uploaded CSV ({} bytes) to {}", data.length, s3aPath);
    }

    /**
     * Delegates to EanImageSourcingService via a shared interface if available,
     * otherwise falls back to a direct DB query on etl_library_versions.
     * Returns null (job proceeds without JAR dep) if resolution fails — the
     * operator must ensure the ETL JAR is available on the cluster classpath.
     */
    private String findLatestJarFromDatabase(String buildType) {
        // Prefer the shared utility exposed via the kernel if available
        try {
            Class<?> utilClass = Class.forName(
                "org.webrobot.eu.apis.jersey.jersey.api.EtlLibraryResolver");
            java.lang.reflect.Method m = utilClass.getMethod(
                "findLatestJarPresignedUrl", String.class);
            return (String) m.invoke(null, buildType);
        } catch (ClassNotFoundException ignored) {
            // Shared utility not present — fall through to direct DB query
        } catch (Exception e) {
            logger.warn("EtlLibraryResolver call failed: {}", e.getMessage());
        }

        // Direct DB fallback: same table queried by EanImageSourcingService
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(
                "SELECT presigned_url FROM etl_library_versions " +
                "WHERE build_type = ? AND is_active = TRUE " +
                "ORDER BY created_at DESC LIMIT 1")) {
            ps.setString(1, buildType);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString("presigned_url");
            }
        } catch (Exception e) {
            logger.warn("Could not resolve ETL JAR from DB (build_type={}): {}", buildType, e.getMessage());
        }
        return null;
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private Connection getConnection() throws SQLException {
        String jdbcUrl  = System.getenv().getOrDefault("DB_JDBC_URL",
                          System.getenv().getOrDefault("WEBROBOT_JDBC_URL", ""));
        String user     = System.getenv().getOrDefault("DB_USER",
                          System.getenv().getOrDefault("WEBROBOT_JDBC_USER", ""));
        String password = System.getenv().getOrDefault("DB_PASSWORD",
                          System.getenv().getOrDefault("WEBROBOT_JDBC_PASSWORD", ""));
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("loginTimeout", "5");  // avoid hanging forever on network partition
        return DriverManager.getConnection(jdbcUrl, props);
    }

    private Optional<UUID> toUUID(Object id) {
        if (id == null) return Optional.empty();
        try {
            return Optional.of(UUID.fromString(id.toString()));
        } catch (IllegalArgumentException e) {
            logger.warn("Cannot convert id '{}' to UUID — SparkJobConfig projectId/jobId will not be set", id);
            return Optional.empty();
        }
    }

    private List<Map<String, Object>> resultSetToList(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= cols; i++)
                row.put(meta.getColumnLabel(i), rs.getObject(i));
            rows.add(row);
        }
        return rows;
    }

    private String str(Map<String, Object> map, String key) {
        Object v = map.get(key);
        return v == null ? "" : v.toString();
    }

    private String csvQuote(String v) {
        if (v == null) return "";
        if (v.contains(",") || v.contains("\"") || v.contains("\n") || v.contains("\r"))
            return "\"" + v.replace("\"", "\"\"").replace("\r", "").replace("\n", " ") + "\"";
        return v;
    }
}
