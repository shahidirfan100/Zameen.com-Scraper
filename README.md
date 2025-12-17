# Zameen.com Property Scraper — Apify Actor

A focused Apify actor for harvesting property listings from Zameen.com area, project or search pages. The actor prefers structured sources (JSON-LD or embedded API responses) and falls back to HTML when necessary. It paginates listing pages, optionally visits detail pages, and saves normalized records to the default dataset.

## Why use this actor

- Fast, JSON-first extraction using embedded `window.state` and the underlying Algolia JSON API.
- Reliable HTML fallback for pages without structured data.
- Built-in pagination, deduplication, and configurable result limits.
- Clean, export-ready dataset containing price, beds, baths, area, address, and more.

## Quick Start (Apify)

1. Open the actor on the Apify platform.
2. Provide `startUrls` for specific listing pages, or use `keyword` and `location` to search dynamically (see examples below).
3. Configure `results_wanted`, `max_pages`, and `scrapeDetails` as needed.
4. Run the actor and export results from the dataset.

## Input (configuration)

Provide JSON input to configure the run. All fields are optional — sensible defaults are used when omitted.

Example input:

```json
{
	"startUrls": [
		"https://www.zameen.com/Homes/Islamabad_DHA_Defence-3188-1.html"
	],
	"keyword": "house",
	"location": "Islamabad",
	"results_wanted": 20,
	"max_pages": 3,
	"scrapeDetails": true,
	"proxyConfiguration": { "useApifyProxy": true }
}
```

Key input fields

- `startUrls` (array): listing pages to start from (areas, projects, searches).
- `keyword` (string): search keyword for properties (e.g., 'house', 'apartment').
- `location` (string): location filter (e.g., 'Islamabad', 'Lahore').
- `results_wanted` (integer): maximum number of listings to collect.
- `max_pages` (integer): pagination cap per start URL.
- `scrapeDetails` (boolean): open each listing page to extract full details (recommended).
- `proxyConfiguration` (object): Apify proxy settings to reduce blocking.

## Extraction strategy

1. JSON-first bootstrap: parses embedded `window.state` to get listing hits and the exact search filters for the page.
2. JSON API pagination: uses Zameen's Algolia JSON API to paginate quickly and consistently.
3. HTML link discovery: falls back to collecting listing links from anchors/listing cards when JSON data is missing.
4. Detail parsing: visits each listing page (if enabled) and extracts full details from `window.state` (`state.property.data`), with HTML fallback as a last resort.

This layered approach maximizes accuracy and speed while being resilient to minor markup changes.

## Output schema (example)

Records saved to the dataset follow this normalized structure:

```json
{
	"title": "3 Marla House in DHA Defence",
	"price": 12500000,
	"currency": "PKR",
	"bedrooms": 3,
	"bathrooms": 3,
	"area": 3,
	"area_unit": "marla",
	"location": "DHA Phase 2, DHA Defence",
	"city": "Islamabad",
	"property_type": "house",
	"purpose": "sale",
	"Description": "...plain text description...",
	"external_id": "53463864",
	"url": "https://www.zameen.com/...,",
	"source": "zameen.com"
}
```

## Configuration tips

- Start with `results_wanted` small (10–50) to validate the setup, then scale up.
- Keep `scrapeDetails` enabled for full fields; disable if you only need listing URLs.
- Use `proxyConfiguration` to minimize blocks; Apify Proxy is recommended for production runs.
- Reduce concurrency if you see frequent transient errors or IP blocks.

## Common issues & troubleshooting

- Empty results: confirm `startUrls` point to valid Zameen listing pages (area/project/search). Some pages redirect to third-party CDNs or trackers — prefer canonical listing pages. If using keyword/location, ensure they match Zameen.com's search parameters.
- Redirects to CDNs or tag services: the actor prefers JSON sources and robust link discovery; if landing on trackers, try an alternate listing URL or enable proxy. Use residential proxies for better success.
- Rate limiting or blocking: enable `proxyConfiguration`, lower `MAX_CONCURRENCY` / `MAX_REQUESTS_PER_MINUTE` in `src/main.js` if needed, or add retries. Zameen.com may block without proxies.
- Missing fields: Zameen pages vary in structure — the actor uses JSON-LD first and best-effort HTML selectors as fallback; update `src/main.js` if you need project-specific selectors.
- Search not working: Zameen.com uses 'search' parameter for keywords in URLs; the actor builds this correctly from input.

## Security & respectful crawling

- Honor robots.txt and rate limits.
- Use proxies and backoff to avoid negative impact on the site.

## Support

If you encounter errors running the actor on Apify, provide:

1. The exact `startUrls` used.
2. The actor run ID or logs (from the Apify run page).
3. A short description of the problem and the symptoms.

With those, maintainers can diagnose selector issues, redirects, or blocking.

## Changelog

- v1.0.0 — Initial Zameen.com property scraper with JSON-first parsing and HTML fallback.

---

If you want, I can also add a short troubleshooting checklist of specific error messages you saw on Apify and update the README accordingly.
