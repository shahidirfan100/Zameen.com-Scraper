# Zameen.com Property Scraper

[![Apify Actor](https://img.shields.io/badge/Apify-Actor-blue)](https://apify.com)

Extract comprehensive property listings from Zameen.com, Pakistan's leading real estate platform. This powerful scraper collects detailed information about houses, apartments, plots, and commercial properties across all major cities including Islamabad, Lahore, Karachi, and more. Perfect for real estate analysis, market research, and data-driven investment decisions.

## Key Features

- **Comprehensive Data Extraction**: Captures essential property details including price, bedrooms, bathrooms, area, location, and descriptions
- **Flexible Input Options**: Scrape by canonical listing URLs (recommended) or by `location + keyword` resolver (no `/search/`)
- **Intelligent Pagination**: Automatically handles multiple pages to collect extensive datasets
- **Detail Page Scraping**: Optional deep scraping of individual property pages for complete information
- **Deduplication**: Built-in duplicate removal ensures clean, unique results
- **Proxy Support**: Integrated proxy configuration to handle rate limiting and blocking
- **Export-Ready Output**: Structured JSON data perfect for analysis and integration

## Input Configuration

Configure the scraper using the following input parameters. All fields are optional with sensible defaults.

### Basic Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `startUrls` | Array | Canonical Zameen listing URLs (recommended) | `["https://www.zameen.com/Homes/Lahore-1-1.html"]` |
| `category` | String | Used only with `location + keyword` resolver | `"Homes"` |
| `keyword` | String | Area/development keyword to resolve within city | `"Bahria Town"` |
| `location` | String | City name to resolve within | `"Lahore"` |

Note: if you provide `startUrls`, the actor uses those URLs directly and ignores the `location + keyword` resolver.

### Advanced Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `results_wanted` | Integer | Maximum number of properties to collect | `100` |
| `max_pages` | Integer | Maximum pages to scrape per URL | `20` |
| `scrapeDetails` | Boolean | Scrape individual property detail pages | `true` |
| `proxyConfiguration` | Object | Proxy settings for anti-blocking | `{"useApifyProxy": true}` |

### Example Input

```json
{
  "startUrls": [
    "https://www.zameen.com/Homes/Pakistan-1-1.html"
  ],
  "results_wanted": 50,
  "max_pages": 5,
  "scrapeDetails": true,
  "proxyConfiguration": {
    "useApifyProxy": true
  }
}
```

## Output Data Structure

The scraper produces clean, structured JSON records with the following schema:

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
  "Description": "Beautiful 3 marla house available for sale in DHA Defence Phase 2...",
  "external_id": "53463864",
  "url": "https://www.zameen.com/Property/dha_defence_dha_phase_2_3_marla_house_for_sale_53463864-12345-1.html",
  "source": "zameen.com"
}
```

## Usage Guide

### Quick Start

1. **Access the Actor**: Visit the actor page on Apify platform
2. **Configure Input**: Set your desired parameters using the input form or JSON editor
3. **Run the Actor**: Click "Start" to begin scraping
4. **Download Results**: Export data from the dataset in your preferred format (JSON, CSV, Excel)

### Scraping Strategies

#### Direct URL Scraping
Provide specific Zameen.com URLs for targeted scraping:

```json
{
  "startUrls": [
    "https://www.zameen.com/Homes/Islamabad-3-1.html",
    "https://www.zameen.com/Homes/Lahore-1-1.html"
  ],
  "results_wanted": 100
}
```

#### Keyword Search
Resolve a canonical listing page from `location + keyword` (avoids `/search/`, which often returns 503):

```json
{
  "category": "Homes",
  "keyword": "Bahria Town",
  "location": "Lahore",
  "results_wanted": 200,
  "scrapeDetails": true
}
```

#### Area-Specific Scraping
Target specific neighborhoods or developments:

```json
{
  "startUrls": [
    "https://www.zameen.com/Homes/Karachi_DHA-1234-1.html"
  ],
  "max_pages": 20
}
```

## Best Practices

- **Start Small**: Begin with `results_wanted: 10-50` to test your configuration
- **Enable Details**: Set `scrapeDetails: true` for complete property information
- **Use Proxies**: Always enable proxy configuration for reliable scraping
- **Monitor Limits**: Respect Zameen.com's terms of service and implement appropriate delays
- **Data Validation**: Review sample results before large-scale runs

## Troubleshooting

### Common Issues

<details>
<summary>Empty or No Results</summary>

**Possible Causes:**
- Invalid or redirected URLs
- Search parameters not matching Zameen.com format
- Anti-bot measures blocking requests

**Solutions:**
- Verify URLs point to actual listing pages
- Use residential proxies
- Try different search keywords
- Reduce scraping speed
</details>

<details>
<summary>Missing Property Details</summary>

**Possible Causes:**
- `scrapeDetails` disabled
- Page structure changes
- Incomplete property listings

**Solutions:**
- Enable `scrapeDetails: true`
- Check for page redirects
- Verify property URLs are accessible
</details>

<details>
<summary>Rate Limiting or Blocking</summary>

**Possible Causes:**
- Too many requests per minute
- IP address blocked
- Missing proxy configuration

**Solutions:**
- Enable Apify Proxy
- Reduce concurrency settings
- Add delays between requests
- Use rotating proxy pools
</details>

<details>
<summary>Incorrect Location Data</summary>

**Possible Causes:**
- Location parameter format issues
- Zameen.com location name changes

**Solutions:**
- Use exact location names from Zameen.com
- Test with direct URLs first
- Check location spelling and formatting
</details>

### Error Messages

| Error | Description | Solution |
|-------|-------------|----------|
| `No listings found` | Search returned no results | Adjust keywords or location |
| `Page not accessible` | URL blocked or invalid | Use proxies, check URL validity |
| `Timeout error` | Request took too long | Increase timeout, use faster proxies |
| `Parse error` | Unexpected page structure | Report to maintainer, try different URLs |

## Data Usage Examples

### Real Estate Market Analysis
```json
// Collect data for market research
{
  "location": "Islamabad",
  "results_wanted": 1000,
  "scrapeDetails": true
}
```

### Investment Property Search
```json
// Find rental properties
{
  "keyword": "apartment for rent",
  "location": "Lahore",
  "results_wanted": 500
}
```

### Commercial Property Listings
```json
// Scrape commercial spaces
{
  "category": "Commercial",
  "keyword": "DHA",
  "location": "Karachi"
}
```

## Changelog

### v1.0.0
- Initial release with comprehensive Zameen.com scraping capabilities
- JSON-first extraction with HTML fallback
- Support for all major Pakistani cities
- Built-in pagination and deduplication

## Support

For technical support or feature requests:

1. Check the troubleshooting section above
2. Review Apify actor logs for detailed error information
3. Provide the following when reporting issues:
   - Actor run ID
   - Input configuration used
   - Specific error messages
   - Expected vs actual behavior

## Legal and Ethical Use

- Respect Zameen.com's Terms of Service
- Use scraped data responsibly and in compliance with local laws
- Implement appropriate rate limiting to avoid impacting the target website
- Consider the privacy implications of collecting property data

---

*This actor is designed for legitimate real estate research and analysis purposes. Always ensure compliance with applicable laws and website terms of service.*
