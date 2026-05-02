package org.webrobot.eu.apis.jersey.pricecomparison.plugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.webrobot.eu.apis.jersey.jetty.RequiresScopes;
import org.webrobot.eu.apis.jersey.jersey.api.services.OrganizationContextHelper;

import javax.annotation.PostConstruct;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

/**
 * Price Comparison Plugin — REST API.
 *
 * Endpoints:
 *   POST   /bootstrap                     — create project + agents for calling org
 *   GET    /products                       — list monitored EANs
 *   POST   /products                       — add EAN to monitor
 *   DELETE /products/{ean}                 — remove EAN
 *   GET    /competitors                    — list competitor domains
 *   POST   /competitors                    — add competitor domain
 *   DELETE /competitors/{id}               — remove competitor
 *   POST   /jobs/discovery                 — run phase-1 job (search → match → save)
 *   POST   /jobs/monitoring                — run phase-2 job (saved URLs → price)
 *   GET    /prices                         — current prices (optionally filtered by ?ean=)
 *   GET    /matches                        — confirmed matches
 */
@Singleton
@Path("/webrobot/api/price-comparison")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PriceComparisonPlugin {

    private static final Logger logger = LoggerFactory.getLogger(PriceComparisonPlugin.class);

    private final PriceComparisonService service;

    public PriceComparisonPlugin() {
        this.service = new PriceComparisonService();
        logger.info("PriceComparisonPlugin initialized");
    }

    @PostConstruct
    public void bootstrap() {
        logger.info("PriceComparisonPlugin loaded — use POST /webrobot/api/price-comparison/bootstrap to initialize for an organization");
    }

    // ── Bootstrap ─────────────────────────────────────────────────────────────

    @POST
    @Path("/bootstrap")
    @RequiresScopes("admin")
    public Response bootstrapOrg(
            @Context ContainerRequestContext req) {
        try {
            String orgId = OrganizationContextHelper.getOrganizationId(req);
            Map<String, Object> result = service.ensureSetup(orgId);
            return Response.ok(result).build();
        } catch (Exception e) {
            logger.error("Bootstrap failed", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        }
    }

    // ── Product catalog ───────────────────────────────────────────────────────

    @GET
    @Path("/products")
    @RequiresScopes("read")
    public Response listProducts(@Context ContainerRequestContext req) {
        try {
            String orgId = OrganizationContextHelper.getOrganizationId(req);
            List<Map<String, Object>> products = service.listProducts(orgId);
            return Response.ok(Map.of("products", products, "total", products.size())).build();
        } catch (Exception e) {
            logger.error("listProducts failed", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        }
    }

    /**
     * Body: { "ean": "1234567890123", "product_name": "...", "brand": "...", "image_url": "..." }
     */
    @POST
    @Path("/products")
    @RequiresScopes("write")
    public Response addProduct(
            @Context ContainerRequestContext req,
            Map<String, Object> body) {
        try {
            String orgId = OrganizationContextHelper.getOrganizationId(req);
            String ean         = require(body, "ean");
            String productName = (String) body.getOrDefault("product_name", "");
            String brand       = (String) body.getOrDefault("brand", "");
            String imageUrl    = (String) body.getOrDefault("image_url", "");
            Map<String, Object> created = service.upsertProduct(orgId, ean, productName, brand, imageUrl);
            return Response.ok(created).build();
        } catch (IllegalArgumentException e) {
            return Response.status(400).entity(Map.of("error", e.getMessage())).build();
        } catch (Exception e) {
            logger.error("addProduct failed", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        }
    }

    @DELETE
    @Path("/products/{ean}")
    @RequiresScopes("write")
    public Response deleteProduct(
            @Context ContainerRequestContext req,
            @PathParam("ean") String ean) {
        try {
            String orgId = OrganizationContextHelper.getOrganizationId(req);
            int deleted = service.deleteProduct(orgId, ean);
            return Response.ok(Map.of("deleted", deleted)).build();
        } catch (Exception e) {
            logger.error("deleteProduct failed", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        }
    }

    // ── Competitors ───────────────────────────────────────────────────────────

    @GET
    @Path("/competitors")
    @RequiresScopes("read")
    public Response listCompetitors(@Context ContainerRequestContext req) {
        try {
            String orgId = OrganizationContextHelper.getOrganizationId(req);
            List<Map<String, Object>> competitors = service.listCompetitors(orgId);
            return Response.ok(Map.of("competitors", competitors, "total", competitors.size())).build();
        } catch (Exception e) {
            logger.error("listCompetitors failed", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        }
    }

    /**
     * Body: { "site_domain": "amazon.it", "site_name": "Amazon Italy", "country_code": "IT" }
     */
    @POST
    @Path("/competitors")
    @RequiresScopes("write")
    public Response addCompetitor(
            @Context ContainerRequestContext req,
            Map<String, Object> body) {
        try {
            String orgId      = OrganizationContextHelper.getOrganizationId(req);
            String domain     = require(body, "site_domain");
            String siteName   = (String) body.getOrDefault("site_name", domain);
            String country    = (String) body.getOrDefault("country_code", "");
            Map<String, Object> created = service.upsertCompetitor(orgId, domain, siteName, country);
            return Response.ok(created).build();
        } catch (IllegalArgumentException e) {
            return Response.status(400).entity(Map.of("error", e.getMessage())).build();
        } catch (Exception e) {
            logger.error("addCompetitor failed", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        }
    }

    @DELETE
    @Path("/competitors/{id}")
    @RequiresScopes("write")
    public Response deleteCompetitor(
            @Context ContainerRequestContext req,
            @PathParam("id") long id) {
        try {
            String orgId = OrganizationContextHelper.getOrganizationId(req);
            int deleted = service.deleteCompetitor(orgId, id);
            return Response.ok(Map.of("deleted", deleted)).build();
        } catch (Exception e) {
            logger.error("deleteCompetitor failed", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        }
    }

    // ── Jobs ──────────────────────────────────────────────────────────────────

    /**
     * Phase 1 — discovery: for each EAN × competitor, search and find product URLs.
     * Body: { "cloudCredentialIds": ["uuid1", "uuid2"] }
     */
    @POST
    @Path("/jobs/discovery")
    @RequiresScopes("write")
    public Response runDiscovery(
            @Context ContainerRequestContext req,
            Map<String, Object> body) {
        try {
            String orgId = OrganizationContextHelper.getOrganizationId(req);
            List<String> credIds = extractCredentialIds(body);
            Map<String, Object> result = service.runDiscoveryJob(orgId, credIds);
            return Response.ok(result).build();
        } catch (IllegalStateException e) {
            return Response.status(409).entity(Map.of("error", e.getMessage())).build();
        } catch (Exception e) {
            logger.error("runDiscovery failed", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        }
    }

    /**
     * Phase 2 — monitoring: re-visit saved URLs to fetch updated prices.
     * Body: { "cloudCredentialIds": ["uuid1"] }
     */
    @POST
    @Path("/jobs/monitoring")
    @RequiresScopes("write")
    public Response runMonitoring(
            @Context ContainerRequestContext req,
            Map<String, Object> body) {
        try {
            String orgId = OrganizationContextHelper.getOrganizationId(req);
            List<String> credIds = extractCredentialIds(body);
            Map<String, Object> result = service.runMonitoringJob(orgId, credIds);
            return Response.ok(result).build();
        } catch (IllegalStateException e) {
            return Response.status(409).entity(Map.of("error", e.getMessage())).build();
        } catch (Exception e) {
            logger.error("runMonitoring failed", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        }
    }

    // ── Results ───────────────────────────────────────────────────────────────

    /**
     * Current prices per EAN. Optional query params: ?ean=xxx&competitor_site=yyy&limit=100
     */
    @GET
    @Path("/prices")
    @RequiresScopes("read")
    public Response getCurrentPrices(
            @Context ContainerRequestContext req,
            @QueryParam("ean") String ean,
            @QueryParam("competitor_site") String site,
            @QueryParam("limit") @DefaultValue("200") int limit) {
        try {
            String orgId = OrganizationContextHelper.getOrganizationId(req);
            List<Map<String, Object>> prices = service.getCurrentPrices(orgId, ean, site, limit);
            return Response.ok(Map.of("prices", prices, "total", prices.size())).build();
        } catch (Exception e) {
            logger.error("getCurrentPrices failed", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        }
    }

    /**
     * Confirmed matches (active). Optional: ?ean=xxx&competitor_site=yyy
     */
    @GET
    @Path("/matches")
    @RequiresScopes("read")
    public Response getMatches(
            @Context ContainerRequestContext req,
            @QueryParam("ean") String ean,
            @QueryParam("competitor_site") String site) {
        try {
            String orgId = OrganizationContextHelper.getOrganizationId(req);
            List<Map<String, Object>> matches = service.getMatches(orgId, ean, site);
            return Response.ok(Map.of("matches", matches, "total", matches.size())).build();
        } catch (Exception e) {
            logger.error("getMatches failed", e);
            return Response.serverError().entity(Map.of("error", e.getMessage())).build();
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String require(Map<String, Object> body, String key) {
        Object v = body == null ? null : body.get(key);
        if (v == null || v.toString().isBlank())
            throw new IllegalArgumentException("Required field missing: " + key);
        return v.toString().trim();
    }

    private List<String> extractCredentialIds(Map<String, Object> body) {
        if (body == null) return Collections.emptyList();
        Object raw = body.get("cloudCredentialIds");
        if (raw instanceof List) {
            return ((List<?>) raw).stream()
                .filter(java.util.Objects::nonNull)
                .map(Object::toString)
                .collect(java.util.stream.Collectors.toList());
        }
        return Collections.emptyList();
    }
}
