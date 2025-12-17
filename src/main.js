import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

const DEFAULT_START_URL = 'https://www.zameen.com/Homes/Islamabad_DHA_Defence-3188-1.html';

await Actor.init();

async function main() {
    try {
        const input = (await Actor.getInput()) || {};
        const {
            startUrls,
            keyword = '',
            location = '',
            results_wanted: RESULTS_WANTED_RAW = 100,
            max_pages: MAX_PAGES_RAW = 20,
            scrapeDetails = true,
            proxyConfiguration,
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : Number.MAX_SAFE_INTEGER;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 20;

        const buildSearchUrl = (kw, loc) => {
            const u = new URL('https://www.zameen.com/search/');
            if (kw) u.searchParams.set('search', String(kw).trim());
            if (loc) u.searchParams.set('location', String(loc).trim());
            return u.href;
        };

        const initial = [];
        if (Array.isArray(startUrls) && startUrls.length) initial.push(...startUrls.map(String));
        if (!initial.length && (keyword || location)) initial.push(buildSearchUrl(keyword, location));
        if (!initial.length) initial.push(DEFAULT_START_URL);

        const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;

        let saved = 0;
        const seen = new Set();

        const normalizeUrl = (href, base) => {
            if (!href) return null;
            try {
                const normalized = new URL(href, base || 'https://www.zameen.com').href;
                return normalized.split('#')[0];
            } catch (err) {
                return null;
            }
        };

        const safeParseJson = (text) => {
            if (!text) return null;
            try { return JSON.parse(text); } catch (err) { return null; }
        };

        const cleanText = (html) => {
            if (!html) return '';
            const $ = cheerioLoad(html);
            $('script, style, noscript, iframe').remove();
            return $.root().text().replace(/\s+/g, ' ').trim();
        };

        const textFrom = ($ctx, selectors = []) => {
            for (const sel of selectors) {
                const value = $ctx(sel).first().text().trim();
                if (value) return value;
            }
            return '';
        };

        const htmlFrom = ($ctx, selectors = []) => {
            for (const sel of selectors) {
                const node = $ctx(sel).first();
                if (node && node.length) {
                    const html = node.html();
                    if (html) return String(html).trim();
                }
            }
            return '';
        };

        const numberFrom = (value) => {
            if (value === null || value === undefined) return null;
            const match = String(value).replace(/[,\s]/g, ' ').match(/(-?\d+(?:\.\d+)?)/);
            return match ? Number(match[1]) : null;
        };

        const parseArea = (text) => {
            if (!text) return { area: null, area_unit: null };
            const match = String(text).match(/(\d+[\d.,]*)\s*(sq\.?\s*ft|sq\.?\s*yd|sq\.?\s*m|sqm|sqft|sqyd|marla|kanal)/i);
            if (!match) return { area: null, area_unit: null };
            return { area: numberFrom(match[1]), area_unit: match[2].replace(/\s+/g, ' ').toLowerCase() };
        };

        const inferPurpose = (urlString, text) => {
            const candidate = `${urlString || ''} ${text || ''}`.toLowerCase();
            if (/(rent|rental)/i.test(candidate)) return 'rent';
            if (/(sale|buy|purchase)/i.test(candidate)) return 'sale';
            return null;
        };

        const inferPropertyType = (urlString, text) => {
            const candidate = `${urlString || ''} ${text || ''}`.toLowerCase();
            if (/apartment|flat/.test(candidate)) return 'apartment';
            if (/house|home|villa/.test(candidate)) return 'house';
            if (/plot|land/.test(candidate)) return 'plot';
            if (/office|commercial/.test(candidate)) return 'commercial';
            return null;
        };

        const extractFromJsonLd = ($ctx) => {
            const scripts = $ctx('script[type="application/ld+json"]');
            for (let i = 0; i < scripts.length; i++) {
                const raw = $ctx(scripts[i]).contents().text();
                const parsed = safeParseJson(raw);
                const candidates = Array.isArray(parsed) ? parsed : parsed ? [parsed] : [];
                for (const node of candidates) {
                    if (!node || typeof node !== 'object') continue;
                    const type = node['@type'] || node.type;
                    if (type === 'Product' || type === 'RealEstateListing' || (Array.isArray(type) && type.includes('Product'))) {
                        return node;
                    }
                    if ((type === 'ItemList' || type === 'CollectionPage') && Array.isArray(node.itemListElement)) {
                        // Skip list pages here; handled elsewhere.
                        continue;
                    }
                }
            }
            return null;
        };

        const extractListFromItemList = ($ctx, base) => {
            const urls = new Set();
            const scripts = $ctx('script[type="application/ld+json"]');
            scripts.each((_, script) => {
                const raw = $ctx(script).contents().text();
                const parsed = safeParseJson(raw);
                const nodes = Array.isArray(parsed) ? parsed : parsed ? [parsed] : [];
                nodes.forEach((node) => {
                    if (!node || typeof node !== 'object') return;
                    const type = node['@type'] || node.type;
                    if (type === 'ItemList' || type === 'CollectionPage') {
                        (node.itemListElement || []).forEach((item) => {
                            const urlCandidate = item?.url || item?.item?.url;
                            const abs = normalizeUrl(urlCandidate, base);
                            if (abs) urls.add(abs);
                        });
                    }
                });
            });
            return [...urls];
        };

        const collectDetailLinksFromHtml = ($ctx, base) => {
            const urls = new Set();
            $ctx('a[href]').each((_, a) => {
                const href = $ctx(a).attr('href');
                if (!href) return;
                const normalized = normalizeUrl(href, base);
                if (!normalized) return;
                if (!/zameen\.com/i.test(normalized)) return;
                if (/(blog|guide|news|about|contact)/i.test(normalized)) return;
                if (/\/Property\//i.test(normalized) && /-\d+\.html$/i.test(normalized)) {
                    urls.add(normalized);
                }
            });
            return [...urls];
        };

        const findNextPage = ($ctx, base) => {
            const rel = $ctx('a[rel="next"], link[rel="next"]').attr('href');
            if (rel) return normalizeUrl(rel, base);
            const candidate = $ctx('a').filter((_, el) => /(next|»|›)/i.test($ctx(el).text())).first().attr('href');
            if (candidate) return normalizeUrl(candidate, base);
            const iconNext = $ctx('a[title*="Next"], a[aria-label*="Next"], .pagination a').filter((_, el) => /(next|»|›)/i.test($ctx(el).text() || $ctx(el).attr('title') || '')).first().attr('href');
            if (iconNext) return normalizeUrl(iconNext, base);
            return null;
        };

        const buildDetailItem = ({ $, html, url: pageUrl }) => {
            const jsonLd = extractFromJsonLd($) || {};
            const priceRaw = jsonLd?.offers?.price ?? textFrom($, ['[itemprop="price"]', '[aria-label*="Price"]', '.price', '[class*="price"]', '[data-cy*="price"]']);
            const { area, area_unit } = parseArea(jsonLd?.floorSize?.value || jsonLd?.floorSize?.text || textFrom($, ['[class*="area"]', '[data-testid*="area"]', '.size', '[class*="size"]', '[data-cy*="area"]', '[class*="sq"]']));
            const bedroomsRaw = jsonLd?.numberOfRooms ?? jsonLd?.numberOfRooms?.value ?? textFrom($, ['[class*="bed" i]', '[data-testid*="bed" i]', '[class*="bedroom"]', '[data-cy*="bed"]']);
            const bathroomsRaw = jsonLd?.numberOfBathroomsTotal ?? jsonLd?.numberOfBathroomsTotal?.value ?? textFrom($, ['[class*="bath" i]', '[data-testid*="bath" i]', '[class*="bathroom"]', '[data-cy*="bath"]']);
            const title = jsonLd?.name || textFrom($, ['h1', 'title', '[class*="title"]']);
            const location = jsonLd?.address?.streetAddress || jsonLd?.address?.addressLocality || textFrom($, ['[class*="location" i]', '[data-testid*="location" i]', '[class*="address"]', '[data-cy*="location"]']);
            const city = jsonLd?.address?.addressLocality || jsonLd?.address?.addressRegion || textFrom($, ['[class*="city"]', '[data-cy*="city"]']) || null;
            const description_html = jsonLd?.description || htmlFrom($, ['[class*="description" i]', '[data-testid*="description" i]', '.listing-description', '.description', '[class*="detail"]', '[data-cy*="description"]']);
            const description_text = cleanText(description_html);

            const price = numberFrom(priceRaw);
            const currency = jsonLd?.offers?.priceCurrency || (/rs|pkr/i.test(String(priceRaw || '')) ? 'PKR' : null);
            const bedrooms = numberFrom(bedroomsRaw);
            const bathrooms = numberFrom(bathroomsRaw);
            const purpose = inferPurpose(pageUrl, html);
            const property_type = inferPropertyType(pageUrl, `${title} ${description_text}`);

            return {
                title: title || null,
                price: price ?? null,
                currency: currency || null,
                bedrooms: bedrooms ?? null,
                bathrooms: bathrooms ?? null,
                area: area ?? null,
                area_unit: area_unit || null,
                location: location || null,
                city: city || null,
                property_type: property_type || null,
                purpose: purpose || null,
                Description: description_text || null,
                url: pageUrl,
                source: 'zameen.com',
            };
        };

        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 5,
            useSessionPool: true,
            maxConcurrency: 5,
            requestHandlerTimeoutSecs: 120,
            async requestHandler({ request, body, $, enqueueLinks, log: crawlerLog }) {
                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;

                if (label === 'LIST') {
                    const apiLinks = extractListFromItemList($, request.url);
                    const htmlLinks = collectDetailLinksFromHtml($, request.url);
                    const candidates = [...apiLinks, ...htmlLinks];

                    const remaining = RESULTS_WANTED - saved;
                    const toProcess = candidates.filter((u) => {
                        if (!u) return false;
                        const isNew = !seen.has(u);
                        if (isNew) seen.add(u);
                        return isNew;
                    }).slice(0, Math.max(0, remaining));

                    crawlerLog.info(`LIST ${request.url} -> queued ${toProcess.length} detail URLs (page ${pageNo})`);

                    if (scrapeDetails) {
                        if (toProcess.length) await enqueueLinks({ urls: toProcess, userData: { label: 'DETAIL' } });
                    } else if (toProcess.length) {
                        await Dataset.pushData(toProcess.map((u) => ({ url: u, source: 'zameen.com' })));
                        saved += toProcess.length;
                    }

                    if (saved < RESULTS_WANTED && pageNo < MAX_PAGES) {
                        const next = findNextPage($, request.url);
                        if (next) {
                            await enqueueLinks({ urls: [next], userData: { label: 'LIST', pageNo: pageNo + 1 } });
                        } else {
                            crawlerLog.info(`No next page link detected after page ${pageNo}`);
                        }
                    }
                    return;
                }

                if (label === 'DETAIL') {
                    if (saved >= RESULTS_WANTED) return;
                    try {
                        const html = body?.toString?.() || '';
                        const item = buildDetailItem({ $, html, url: request.url });
                        await Dataset.pushData(item);
                        saved++;
                    } catch (err) {
                        crawlerLog.error(`DETAIL ${request.url} failed: ${err.message}`);
                    }
                }
            },
        });

        await crawler.run(initial.map((u) => ({ url: u, userData: { label: 'LIST', pageNo: 1 } })));
        log.info(`Finished. Saved ${saved} items`);
    } finally {
        await Actor.exit();
    }
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
