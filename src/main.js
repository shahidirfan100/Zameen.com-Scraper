import { Actor, log } from 'apify';
import { Dataset, HttpCrawler, log as crawleeLog } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

// Broad listing page that reliably contains `window.state` + Algolia config.
const DEFAULT_START_URL = 'https://www.zameen.com/Homes/Pakistan-1-1.html';
// Lightweight page used only to resolve city/location slugs (avoid /search which is often 503).
const BOOTSTRAP_URL = 'https://www.zameen.com/';
const USE_ALGOLIA_API = true;
const MAX_CONCURRENCY = 2;
const MAX_REQUESTS_PER_MINUTE = 120;
const REQUEST_TIMEOUT_MS = 60000;

// Reduce log noise (Apify platform will still print container/system logs).
crawleeLog.setLevel(crawleeLog.LEVELS.ERROR);
log.setLevel(log.LEVELS.WARNING);

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
];

const pickUserAgent = (session) => {
    const idx = session?.id ? Math.abs([...session.id].reduce((a, c) => a + c.charCodeAt(0), 0)) % USER_AGENTS.length : 0;
    return USER_AGENTS[idx];
};

const safeParseJson = (text) => {
    if (!text) return null;
    try { return JSON.parse(text); } catch { return null; }
};

const normalizeUrl = (href, base) => {
    if (!href) return null;
    try {
        const normalized = new URL(href, base || 'https://www.zameen.com').href;
        return normalized.split('#')[0];
    } catch {
        return null;
    }
};

const isZameenUrl = (urlString) => /^https?:\/\/(?:www\.)?zameen\.com\//i.test(String(urlString || ''));

const isPropertyDetailUrl = (urlString) => {
    if (!urlString) return false;
    const u = String(urlString);
    return /^https?:\/\/(?:www\.)?zameen\.com\/Property\/.+-\d+\.html(?:$|\?)/i.test(u);
};

const extractExternalIdFromPropertyUrl = (urlString) => {
    const u = String(urlString || '');
    const match = u.match(/-(\d+)(?:-\d+){1,2}\.html(?:$|\?)/i);
    return match?.[1] ? String(match[1]) : null;
};

const normalizeForMatch = (value) =>
    String(value || '')
        .toLowerCase()
        .replace(/[_-]+/g, ' ')
        .replace(/[^a-z0-9\s]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();

const normalizeCategory = (value) => {
    const v = String(value || '').trim().toLowerCase();
    if (v === 'plots' || v === 'plot') return 'Plots';
    if (v === 'commercial' || v === 'com') return 'Commercial';
    if (v === 'homes' || v === 'home') return 'Homes';
    return null;
};

const inferCategorySegment = (keyword) => {
    const k = normalizeForMatch(keyword);
    if (/\b(plot|plots|land)\b/i.test(k)) return 'Plots';
    if (/\b(commercial|shop|office|warehouse)\b/i.test(k)) return 'Commercial';
    return 'Homes';
};

const collectLocationsFromState = (state, maxNodes = 150000, maxLocations = 60000) => {
    const bySlug = new Map();
    const stack = [state];
    let visited = 0;

    const isLocationLike = (obj) =>
        obj &&
        typeof obj === 'object' &&
        typeof obj.name === 'string' &&
        typeof obj.slug === 'string' &&
        obj.slug.startsWith('/') &&
        (typeof obj.externalID === 'string' || typeof obj.externalID === 'number');

    while (stack.length && visited < maxNodes && bySlug.size < maxLocations) {
        const node = stack.pop();
        visited++;
        if (!node) continue;
        if (Array.isArray(node)) {
            for (const item of node) {
                if (isLocationLike(item)) {
                    const slug = String(item.slug);
                    if (!bySlug.has(slug)) bySlug.set(slug, item);
                } else if (item && typeof item === 'object') {
                    stack.push(item);
                }
                if (bySlug.size >= maxLocations) break;
            }
            continue;
        }
        if (typeof node !== 'object') continue;
        for (const v of Object.values(node)) {
            if (v && typeof v === 'object') stack.push(v);
        }
    }

    return [...bySlug.values()];
};

const resolveBestLocation = ({ locations, combinedQuery, cityHint }) => {
    const q = normalizeForMatch(combinedQuery);
    const tokens = q.split(' ').filter(Boolean);
    const cityTokens = normalizeForMatch(cityHint).split(' ').filter(Boolean);
    if (!tokens.length) return null;

    const scoreOne = (loc) => {
        const name = normalizeForMatch(loc?.name);
        const hierarchyPath = normalizeForMatch(loc?.hierarchyPath || '');
        const hierarchyNames = Array.isArray(loc?.hierarchy)
            ? normalizeForMatch(loc.hierarchy.map((h) => h?.name).filter(Boolean).join(' '))
            : '';

        const hay = `${name} ${hierarchyPath} ${hierarchyNames}`.trim();
        if (!hay) return 0;

        let score = 0;
        if (name === q) score += 200;
        if (hay === q) score += 150;
        if (name.startsWith(q)) score += 120;
        if (hay.includes(q)) score += 80;

        const allTokensMatch = tokens.every((t) => hay.includes(t));
        if (allTokensMatch) score += 80;

        const cityMatch = cityTokens.length && cityTokens.every((t) => hay.includes(t));
        if (cityMatch) score += 40;

        if (typeof loc?.level === 'number') score += Math.min(30, loc.level * 3);
        return score;
    };

    let best = null;
    let bestScore = 0;
    for (const loc of locations) {
        const s = scoreOne(loc);
        if (s > bestScore) {
            bestScore = s;
            best = loc;
        }
    }
    return bestScore >= 120 ? best : null;
};

const inferListPageNoFromUrl = (urlString) => {
    const u = String(urlString || '');
    const match = u.match(/-(\d+)\.html(?:$|\?)/i);
    return match ? Math.max(1, Number(match[1])) : 1;
};

const extractZameenPropertyUrlFromText = (text) => {
    const t = String(text || '');
    const m = t.match(/https?:\/\/(?:www\.)?zameen\.com\/Property\/[^\s"'<>]+?-\d+\.html/ig);
    return m?.[0] || null;
};

const extractJsonObjectAfterMarker = (text, marker) => {
    if (!text) return null;
    const idx = String(text).indexOf(marker);
    if (idx === -1) return null;
    const start = String(text).indexOf('{', idx);
    if (start === -1) return null;

    let depth = 0;
    let inString = false;
    let escape = false;
    for (let i = start; i < text.length; i++) {
        const ch = text[i];
        if (inString) {
            if (escape) escape = false;
            else if (ch === '\\') escape = true;
            else if (ch === '"') inString = false;
            continue;
        }
        if (ch === '"') {
            inString = true;
            continue;
        }
        if (ch === '{') depth++;
        if (ch === '}') {
            depth--;
            if (depth === 0) return text.slice(start, i + 1);
        }
    }
    return null;
};

const parseWindowState = (html) => {
    const raw = extractJsonObjectAfterMarker(html, 'window.state =');
    return raw ? safeParseJson(raw) : null;
};

const cleanText = (htmlOrText) => {
    if (!htmlOrText) return '';
    const s = String(htmlOrText);
    if (!/[<>]/.test(s)) return s.replace(/\s+/g, ' ').trim();
    const $ = cheerioLoad(s);
    $('script, style, noscript, iframe').remove();
    return $.root().text().replace(/\s+/g, ' ').trim();
};

const numberFrom = (value) => {
    if (value === null || value === undefined) return null;
    const match = String(value).replace(/[,\s]/g, ' ').match(/(-?\d+(?:\.\d+)?)/);
    return match ? Number(match[1]) : null;
};

const pickNumber = (...values) => {
    for (const v of values) {
        const n = numberFrom(v);
        if (Number.isFinite(n)) return n;
    }
    return null;
};

const toPurpose = (purpose) => {
    const p = String(purpose || '').toLowerCase();
    if (p === 'for-sale' || p === 'sale') return 'sale';
    if (p === 'for-rent' || p === 'rent') return 'rent';
    return null;
};

const toPropertyType = (categoryArray) => {
    const categories = Array.isArray(categoryArray) ? categoryArray : [];
    const leaf = categories.find((c) => c?.level === 1) || categories[1] || categories[categories.length - 1];
    const type = leaf?.nameSingular || leaf?.name || leaf?.slug || null;
    return type ? String(type).toLowerCase().replace(/_property$/i, '').replace(/\s+/g, ' ').trim() : null;
};

const locationFromHierarchy = (locationArray) => {
    const locs = Array.isArray(locationArray) ? locationArray : [];
    const city = locs.find((x) => x?.level === 2)?.name || null;
    const parts = locs
        .filter((x) => x?.level >= 3 && x?.name)
        .map((x) => String(x.name).trim())
        .filter(Boolean);
    const locationText = parts.length ? parts.join(', ') : null;
    return { city, location: locationText };
};

const normalizeArea = (areaSqm) => {
    const sqm = Number(areaSqm);
    if (!Number.isFinite(sqm) || sqm <= 0) return { area: null, area_unit: null };

    const sqft = sqm * 10.76391041671;
    const marla = sqft / 225;
    const kanal = marla / 20;

    const nearInt = (value, tolerance = 0.08) => Math.abs(value - Math.round(value)) <= tolerance;

    if (kanal >= 1 && nearInt(kanal)) return { area: Math.round(kanal), area_unit: 'kanal' };
    if (marla >= 1 && marla < 20 && nearInt(marla)) return { area: Math.round(marla), area_unit: 'marla' };

    if (sqft >= 1 && Math.abs(sqft - Math.round(sqft)) <= 2) return { area: Math.round(sqft), area_unit: 'sqft' };
    return { area: Math.round(sqm * 100) / 100, area_unit: 'sqm' };
};

const toDetailUrlFromHit = (hit) => {
    const urlCandidate = hit?.link || hit?.url;
    if (urlCandidate) {
        const normalized = normalizeUrl(urlCandidate, 'https://www.zameen.com/');
        if (normalized && isPropertyDetailUrl(normalized)) return normalized;
    }
    const slug = hit?.slug;
    if (!slug) return null;
    const s = String(slug).replace(/^\/+/, '');
    const full = `https://www.zameen.com/Property/${s}.html`;
    return isPropertyDetailUrl(full) ? full : null;
};

const buildAlgoliaFilters = (filtersObject) => {
    const filters = filtersObject && typeof filtersObject === 'object' ? filtersObject : {};

    const normalizeValue = (v) => {
        if (v === null || v === undefined) return null;
        if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return v;
        if (typeof v === 'object') return v.slug || v.externalID || v.name || null;
        return String(v);
    };

    const quote = (v) => {
        if (typeof v === 'number' || typeof v === 'boolean') return String(v);
        const s = String(v).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        return `"${s}"`;
    };

    const pieces = [];
    for (const [key, f] of Object.entries(filters)) {
        if (!f?.active) continue;
        if (key === 'page') continue;
        const attribute = f.attribute || key;

        const rawValue = f.value;
        const values = [];
        if (Array.isArray(rawValue)) {
            rawValue.forEach((x) => {
                const v = normalizeValue(x);
                if (v !== null && v !== undefined && String(v).trim()) values.push(v);
            });
        } else {
            const v = normalizeValue(rawValue);
            if (v !== null && v !== undefined && String(v).trim()) values.push(v);
        }

        if (!values.length) continue;
        const joiner = f.selectionType === 'union' ? 'OR' : 'AND';
        const expressions = values.map((v) => `${attribute}:${quote(v)}`);
        if (expressions.length === 1) pieces.push(expressions[0]);
        else pieces.push(`(${expressions.join(` ${joiner} `)})`);
    }

    return pieces.join(' AND ');
};

const buildAlgoliaRequest = ({ appId, apiKey, indexName, filtersStr, pageNo, hitsPerPage }) => {
    const url = `https://${appId}-dsn.algolia.net/1/indexes/${encodeURIComponent(indexName)}/query`;
    const pageIndex = Math.max(0, Number(pageNo) - 1);
    const params = new URLSearchParams({
        query: '',
        hitsPerPage: String(hitsPerPage),
        page: String(pageIndex),
    });
    if (filtersStr) params.set('filters', filtersStr);

    return {
        url,
        method: 'POST',
        headers: {
            'content-type': 'application/json',
            'x-algolia-api-key': String(apiKey),
            'x-algolia-application-id': String(appId),
        },
        payload: JSON.stringify({ params: params.toString() }),
    };
};

const looksBlocked = (html) => {
    const t = String(html || '').toLowerCase();
    return (
        t.includes('recaptcha') ||
        t.includes('g-recaptcha') ||
        t.includes('captcha') ||
        t.includes('access denied') ||
        t.includes('pardon our interruption')
    );
};

const parseJsonLdBlocks = ($) => {
    const raw = [];
    $('script[type="application/ld+json"]').each((_, el) => {
        const txt = $(el).text();
        const parsed = safeParseJson(txt);
        if (parsed) raw.push(parsed);
    });

    const flattened = [];
    const flattenInto = (node) => {
        if (!node) return;
        if (Array.isArray(node)) return node.forEach(flattenInto);
        if (node && typeof node === 'object') {
            flattened.push(node);
            if (Array.isArray(node['@graph'])) node['@graph'].forEach(flattenInto);
        }
    };
    raw.forEach(flattenInto);

    // De-dup by stable stringification (best-effort).
    const seen = new Set();
    const unique = [];
    for (const b of flattened) {
        let key;
        try { key = JSON.stringify(b).slice(0, 500); } catch { key = String(b?.['@type'] || 'obj'); }
        if (seen.has(key)) continue;
        seen.add(key);
        unique.push(b);
    }
    return unique;
};

const pickJsonLdListing = (jsonLdBlocks) => {
    const blocks = Array.isArray(jsonLdBlocks) ? jsonLdBlocks : [];
    const scoreOne = (obj) => {
        if (!obj || typeof obj !== 'object') return 0;
        const type = obj['@type'];
        const typeStr = Array.isArray(type) ? type.join(' ') : String(type || '');
        const hasOffers = !!obj.offers;
        const hasName = !!obj.name;
        const hasDesc = !!obj.description;
        const hasUrl = !!obj.url;
        let score = 0;
        if (/\b(product|offer|place|residence|house|apartment)\b/i.test(typeStr)) score += 30;
        if (hasOffers) score += 40;
        if (hasName) score += 20;
        if (hasDesc) score += 10;
        if (hasUrl) score += 10;
        return score;
    };

    let best = null;
    let bestScore = 0;
    for (const b of blocks) {
        const s = scoreOne(b);
        if (s > bestScore) {
            bestScore = s;
            best = b;
        }
    }
    return bestScore >= 40 ? best : null;
};

await Actor.init();

async function main() {
    try {
        const input = (await Actor.getInput()) || {};
        const {
            startUrls,
            keyword = '',
            location = '',
            category: categoryInput = '',
            results_wanted: RESULTS_WANTED_RAW = 100,
            max_pages: MAX_PAGES_RAW = 20,
            scrapeDetails = true,
            proxyConfiguration,
        } = input;

        const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW) ? Math.max(1, +RESULTS_WANTED_RAW) : Number.MAX_SAFE_INTEGER;
        const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW) ? Math.max(1, +MAX_PAGES_RAW) : 20;

        const initial = [];
        if (Array.isArray(startUrls) && startUrls.length) initial.push(...startUrls.map(String));
        if (!initial.length && !String(keyword).trim() && !String(location).trim()) {
            log.warning(`No start URLs or keyword/location provided; falling back to ${DEFAULT_START_URL}`);
            initial.push(DEFAULT_START_URL);
        }

        const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration({ ...proxyConfiguration }) : undefined;
        const requestQueue = await Actor.openRequestQueue();

        let saved = 0;
        let reserved = 0; // how many listings we decided to output (queued or already output)
        const MAX_RESERVATIONS = scrapeDetails ? RESULTS_WANTED + Math.min(50, Math.ceil(RESULTS_WANTED * 0.2)) : RESULTS_WANTED;
        const seenDetail = new Set(); // externalID or URL
        const seenPages = new Set(); // list/API pagination dedupe
        const runStats = {
            blockedList: 0,
            blockedDetail: 0,
            http403: 0,
            http429: 0,
            queuedDetails: 0,
            pushedFull: 0,
            pushedFallback: 0,
        };

        const maybeLogBlocked = (type, url) => {
            const key = type === 'LIST' ? 'blockedList' : 'blockedDetail';
            runStats[key] += 1;
            const count = runStats[key];

            // Log first few examples, then only occasional rollups.
            if (count <= 3) {
                log.warning(`Blocked/captcha-like response on ${type}: ${url}`);
                return;
            }
            if (count % 25 === 0) {
                log.warning(`Blocked/captcha-like responses so far: list=${runStats.blockedList}, detail=${runStats.blockedDetail}`);
            }
        };

        const enqueueDetail = async ({ url, externalId, partial }) => {
            if (!isPropertyDetailUrl(url)) return;
            const finalExternalId = externalId || extractExternalIdFromPropertyUrl(url);
            const uniqueKey = finalExternalId ? `detail:${finalExternalId}` : url;
            if (seenDetail.has(uniqueKey)) return;
            if (reserved >= MAX_RESERVATIONS) return;
            seenDetail.add(uniqueKey);
            reserved++;
            runStats.queuedDetails++;
            await requestQueue.addRequest({ url, userData: { label: 'DETAIL', partial }, uniqueKey });
        };

        const enqueueNextAlgoliaPageIfNeeded = async (algoliaConfig, nextPageNo) => {
            if (!USE_ALGOLIA_API) return false;
            if (!algoliaConfig?.appId || !algoliaConfig?.apiKey || !algoliaConfig?.indexName) return false;
            if (nextPageNo > MAX_PAGES) return false;
            if (reserved >= MAX_RESERVATIONS) return false;

            const signature = `${algoliaConfig.indexName}|${algoliaConfig.filtersStr}|${algoliaConfig.hitsPerPage}|${nextPageNo}`;
            if (seenPages.has(signature)) return false;
            seenPages.add(signature);

            const req = buildAlgoliaRequest({
                appId: algoliaConfig.appId,
                apiKey: algoliaConfig.apiKey,
                indexName: algoliaConfig.indexName,
                filtersStr: algoliaConfig.filtersStr,
                pageNo: nextPageNo,
                hitsPerPage: algoliaConfig.hitsPerPage,
            });

            await requestQueue.addRequest({
                url: req.url,
                method: req.method,
                headers: req.headers,
                payload: req.payload,
                userData: { label: 'ALGOLIA', pageNo: nextPageNo, algoliaConfig },
                uniqueKey: `algolia:${signature}`,
            });

            return true;
        };

        // If user provided keyword/location without startUrls, avoid /search (often 503) by resolving to a canonical listing URL.
        if ((!Array.isArray(startUrls) || !startUrls.length) && (String(keyword).trim() || String(location).trim())) {
            await requestQueue.addRequest({
                url: BOOTSTRAP_URL,
                userData: {
                    label: 'BOOTSTRAP',
                    keyword: String(keyword || ''),
                    location: String(location || ''),
                    category: normalizeCategory(categoryInput) || null,
                },
                uniqueKey: `bootstrap:${normalizeForMatch(location)}|${normalizeForMatch(keyword)}`,
            });
        } else {
            for (const uRaw of initial) {
                const u = String(uRaw);
                if (!isZameenUrl(u) && !extractZameenPropertyUrlFromText(u)) {
                    log.warning(`Skipping non-Zameen start URL: ${u}`);
                    continue;
                }
                const pageNo = isPropertyDetailUrl(u) ? 1 : inferListPageNoFromUrl(u);
                const normalizedUrl = isZameenUrl(u) ? u : extractZameenPropertyUrlFromText(u);
                await requestQueue.addRequest({
                    url: normalizedUrl,
                    userData: isPropertyDetailUrl(normalizedUrl) ? { label: 'DETAIL' } : { label: 'LIST', pageNo },
                    uniqueKey: normalizedUrl,
                });
            }
        }

        const crawler = new HttpCrawler({
            requestQueue,
            proxyConfiguration: proxyConf,
            useSessionPool: true,
            maxRequestRetries: 4,
            maxConcurrency: MAX_CONCURRENCY,
            maxRequestsPerMinute: MAX_REQUESTS_PER_MINUTE,
            requestHandlerTimeoutSecs: 240,
            preNavigationHooks: [
                async ({ request, session }, gotOptions) => {
                    gotOptions.headers ??= {};
                    gotOptions.headers['user-agent'] = pickUserAgent(session);
                    gotOptions.headers['accept-language'] = 'en-US,en;q=0.9';
                    const label = request.userData?.label || 'LIST';
                    if (label === 'ALGOLIA') {
                        gotOptions.headers.accept ??= 'application/json,text/plain,*/*';
                    } else {
                        gotOptions.headers.accept ??= 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8';
                    }
                    gotOptions.followRedirect = true;
                    // Avoid huge HTML snippets in error logs by handling status codes ourselves.
                    gotOptions.throwHttpErrors = false;
                    gotOptions.timeout = { request: REQUEST_TIMEOUT_MS };
                },
            ],
            async requestHandler({ request, body, response, session, log: crawlerLog }) {
                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;
                const bodyText = body?.toString?.() || '';
                const statusCode = response?.statusCode || 0;

                // Sanitize retries/errors (no HTML payloads in error messages).
                if (statusCode >= 500) {
                    if (session) session.retire();
                    throw new Error(`HTTP ${statusCode} on ${label}`);
                }
                if (statusCode === 403 || statusCode === 429) {
                    if (statusCode === 403) runStats.http403++;
                    if (statusCode === 429) runStats.http429++;
                    if (session) session.retire();
                    throw new Error(`HTTP ${statusCode} on ${label}`);
                }

                if (label === 'BOOTSTRAP') {
                    const state = parseWindowState(bodyText);
                    if (!state || typeof state !== 'object') {
                        log.warning('Failed to bootstrap (missing window.state). Provide `startUrls` instead.');
                        return;
                    }

                    const kw = String(request.userData?.keyword || '');
                    const loc = String(request.userData?.location || '');
                    const locations = collectLocationsFromState(state);

                    const category = request.userData?.category || normalizeCategory(categoryInput) || inferCategorySegment(kw);
                    const cityBest = resolveBestLocation({ locations, combinedQuery: loc, cityHint: loc });

                    // Step 1: resolve city page (e.g., /Homes/Lahore-1-1.html) then resolve area within that city.
                    if (cityBest?.slug) {
                        const citySlugNoSlash = String(cityBest.slug).replace(/^\//, '');
                        const cityUrl = `https://www.zameen.com/${category}/${citySlugNoSlash}-1.html`;

                        if (String(kw).trim()) {
                            await requestQueue.addRequest({
                                url: cityUrl,
                                userData: { label: 'BOOTSTRAP_CITY', keyword: kw, location: loc, category },
                                uniqueKey: `bootstrapCity:${category}:${citySlugNoSlash}:${normalizeForMatch(kw)}`,
                            });
                        } else {
                            await requestQueue.addRequest({
                                url: cityUrl,
                                userData: { label: 'LIST', pageNo: 1 },
                                uniqueKey: cityUrl,
                            });
                        }
                        return;
                    }

                    // Step 2 (fallback): resolve directly from this bootstrap page.
                    const combined = `${loc} ${kw}`.trim();
                    const bestDirect =
                        resolveBestLocation({ locations, combinedQuery: combined, cityHint: loc }) ||
                        resolveBestLocation({ locations, combinedQuery: loc, cityHint: loc }) ||
                        resolveBestLocation({ locations, combinedQuery: kw, cityHint: loc });

                    if (!bestDirect?.slug) {
                        log.warning(`Could not resolve keyword/location to a Zameen listing page. Provide \`startUrls\` instead (keyword="${kw}", location="${loc}").`);
                        return;
                    }

                    const slugNoSlash = String(bestDirect.slug).replace(/^\//, '');
                    const listingUrl = `https://www.zameen.com/${category}/${slugNoSlash}-1.html`;
                    await requestQueue.addRequest({ url: listingUrl, userData: { label: 'LIST', pageNo: 1 }, uniqueKey: listingUrl });
                    return;
                }

                if (label === 'BOOTSTRAP_CITY') {
                    const state = parseWindowState(bodyText);
                    if (!state || typeof state !== 'object') {
                        log.warning('Failed to bootstrap city page (missing window.state). Provide `startUrls` instead.');
                        return;
                    }

                    const kw = String(request.userData?.keyword || '');
                    const loc = String(request.userData?.location || '');
                    const category = String(request.userData?.category || inferCategorySegment(kw));

                    const locations = collectLocationsFromState(state);
                    const combined = `${loc} ${kw}`.trim();
                    const best =
                        resolveBestLocation({ locations, combinedQuery: combined, cityHint: loc }) ||
                        resolveBestLocation({ locations, combinedQuery: kw, cityHint: loc }) ||
                        resolveBestLocation({ locations, combinedQuery: loc, cityHint: loc });

                    if (!best?.slug) {
                        // Fallback: just treat this city page as the listing page.
                        await requestQueue.addRequest({ url: request.url, userData: { label: 'LIST', pageNo: 1 }, uniqueKey: `list:${request.url}` });
                        return;
                    }

                    const slugNoSlash = String(best.slug).replace(/^\//, '');
                    const listingUrl = `https://www.zameen.com/${category}/${slugNoSlash}-1.html`;
                    await requestQueue.addRequest({ url: listingUrl, userData: { label: 'LIST', pageNo: 1 }, uniqueKey: listingUrl });
                    return;
                }

                if (label === 'LIST') {
                    if (looksBlocked(bodyText)) {
                        maybeLogBlocked('LIST', request.url);
                        if (session) session.retire();
                        throw new Error('Blocked/captcha-like response');
                    }

                    const $ = cheerioLoad(bodyText);
                    const state = parseWindowState(bodyText);

                    const hits = state?.algolia?.content?.hits;
                    const algoliaConfig = state?.algolia
                        ? {
                            appId: state.algolia.appId,
                            apiKey: state.algolia.apiKey,
                            indexName: state.algolia.indexName,
                            hitsPerPage: state.algolia.settings?.hitsPerPage || state.algolia.content?.hitsPerPage || 25,
                            filtersStr: buildAlgoliaFilters(state.algolia.filters),
                        }
                        : null;

                    const candidates = [];

                    if (Array.isArray(hits) && hits.length) {
                        hits.forEach((hit) => {
                            const url = toDetailUrlFromHit(hit);
                            if (!url) return;
                            candidates.push({ url, hit });
                        });
                    }

                    $('a[href]').each((_, a) => {
                        const href = $(a).attr('href');
                        const normalized = normalizeUrl(href, request.url);
                        if (!normalized) return;

                        // Avoid social-share URLs; if they embed a Zameen property URL, extract and crawl the real one.
                        let candidateUrl = normalized;
                        if (!isZameenUrl(candidateUrl)) {
                            const decoded = (() => {
                                try { return decodeURIComponent(candidateUrl); } catch { return candidateUrl; }
                            })();
                            const embedded = extractZameenPropertyUrlFromText(decoded) || extractZameenPropertyUrlFromText(candidateUrl);
                            if (!embedded) return;
                            candidateUrl = embedded;
                        }

                        if (!isPropertyDetailUrl(candidateUrl)) return;
                        candidates.push({ url: candidateUrl, hit: null });
                    });

                    const remaining = MAX_RESERVATIONS - reserved;
                    const localSeen = new Set();
                    const uniqueCandidates = [];
                    for (const c of candidates) {
                        const externalId =
                            c?.hit?.externalID ? String(c.hit.externalID) : extractExternalIdFromPropertyUrl(c?.url);
                        const key = externalId ? `detail:${externalId}` : c?.url;
                        if (!key) continue;
                        if (localSeen.has(key)) continue;
                        if (seenDetail.has(key)) continue;
                        localSeen.add(key);
                        uniqueCandidates.push(c);
                    }
                    const toQueue = uniqueCandidates.slice(0, Math.max(0, remaining));

                    if (scrapeDetails) {
                        for (const { url, hit } of toQueue) {
                            const externalId = hit?.externalID ? String(hit.externalID) : extractExternalIdFromPropertyUrl(url);
                            const { city, location: locationText } = locationFromHierarchy(hit?.location);
                            const areaNormalized = normalizeArea(hit?.area);
                            const partial = hit
                                ? {
                                    title: hit.title || null,
                                    price: pickNumber(hit.price) ?? null,
                                    currency: 'PKR',
                                    bedrooms: pickNumber(hit.rooms) ?? null,
                                    bathrooms: pickNumber(hit.baths) ?? null,
                                    area: areaNormalized.area ?? null,
                                    area_unit: areaNormalized.area_unit || null,
                                    location: locationText || null,
                                    city: city || null,
                                    purpose: toPurpose(hit.purpose) || null,
                                    property_type: toPropertyType(hit.category) || null,
                                    url,
                                    external_id: externalId,
                                    source: 'zameen.com',
                                }
                                : null;
                            await enqueueDetail({ url, externalId, partial });
                        }
                    } else {
                        await Dataset.pushData(toQueue.map(({ url }) => ({ url, source: 'zameen.com' })));
                        saved += toQueue.length;
                        reserved += toQueue.length;
                    }

                    if (reserved < MAX_RESERVATIONS && pageNo < MAX_PAGES) {
                        const enqueued = await enqueueNextAlgoliaPageIfNeeded(algoliaConfig, pageNo + 1);
                        if (!enqueued) {
                            const nextHref = $('a[rel="next"], link[rel="next"]').attr('href');
                            const next = normalizeUrl(nextHref, request.url);
                            if (next) {
                                await requestQueue.addRequest({ url: next, userData: { label: 'LIST', pageNo: pageNo + 1 }, uniqueKey: next });
                            }
                        }
                    }
                    return;
                }

                if (label === 'ALGOLIA') {
                    if (saved >= RESULTS_WANTED) return;
                    if (reserved >= MAX_RESERVATIONS) return;

                    const parsed = safeParseJson(bodyText);
                    if (!parsed?.hits || !Array.isArray(parsed.hits)) {
                        crawlerLog.warning(`ALGOLIA page ${pageNo}: unexpected response, keys=${parsed ? Object.keys(parsed) : 'null'}`);
                        return;
                    }

                    const hits = parsed.hits;
                    const remaining = MAX_RESERVATIONS - reserved;
                    const uniqueHits = [];
                    const localSeen = new Set();
                    for (const hit of hits) {
                        const url = toDetailUrlFromHit(hit);
                        const externalId = hit?.externalID ? String(hit.externalID) : extractExternalIdFromPropertyUrl(url);
                        const key = externalId ? `detail:${externalId}` : url;
                        if (!key) continue;
                        if (localSeen.has(key)) continue;
                        if (seenDetail.has(key)) continue;
                        localSeen.add(key);
                        uniqueHits.push(hit);
                    }
                    const toQueue = uniqueHits.slice(0, Math.max(0, remaining));

                    if (scrapeDetails) {
                        for (const hit of toQueue) {
                            const url = toDetailUrlFromHit(hit);
                            if (!url) continue;

                            const externalId = hit?.externalID ? String(hit.externalID) : extractExternalIdFromPropertyUrl(url);
                            const { city, location: locationText } = locationFromHierarchy(hit?.location);
                            const areaNormalized = normalizeArea(hit?.area);
                            const partial = {
                                title: hit.title || null,
                                price: pickNumber(hit.price) ?? null,
                                currency: 'PKR',
                                bedrooms: pickNumber(hit.rooms) ?? null,
                                bathrooms: pickNumber(hit.baths) ?? null,
                                area: areaNormalized.area ?? null,
                                area_unit: areaNormalized.area_unit || null,
                                location: locationText || null,
                                city: city || null,
                                purpose: toPurpose(hit.purpose) || null,
                                property_type: toPropertyType(hit.category) || null,
                                url,
                                external_id: externalId,
                                source: 'zameen.com',
                            };

                            await enqueueDetail({ url, externalId, partial });
                        }
                    } else {
                        await Dataset.pushData(toQueue.map((hit) => ({ url: toDetailUrlFromHit(hit), source: 'zameen.com' })).filter((x) => x.url));
                        saved += toQueue.length;
                        reserved += toQueue.length;
                    }

                    const cfg = request.userData?.algoliaConfig;
                    if (reserved < MAX_RESERVATIONS && pageNo < MAX_PAGES) {
                        await enqueueNextAlgoliaPageIfNeeded(cfg, pageNo + 1);
                    }
                    return;
                }

                if (label === 'DETAIL') {
                    if (saved >= RESULTS_WANTED) return;
                    if (!isPropertyDetailUrl(request.url)) return;

                    if (looksBlocked(bodyText)) {
                        maybeLogBlocked('DETAIL', request.url);
                        if (session) session.retire();
                        throw new Error('Blocked/captcha-like response');
                    }

                    const state = parseWindowState(bodyText);
                    const data = state?.property?.data;
                    const partial = request.userData?.partial || null;

                    if (data && typeof data === 'object' && String(data.externalID || '').trim()) {
                        const { city, location: locationText } = locationFromHierarchy(data.location);
                        const areaNormalized = normalizeArea(data.area);

                        const item = {
                            title: data.title || partial?.title || null,
                            price: pickNumber(data.price, partial?.price) ?? null,
                            currency: 'PKR',
                            bedrooms: pickNumber(data.rooms, partial?.bedrooms) ?? null,
                            bathrooms: pickNumber(data.baths, partial?.bathrooms) ?? null,
                            area: areaNormalized.area ?? partial?.area ?? null,
                            area_unit: areaNormalized.area_unit || partial?.area_unit || null,
                            location: locationText || partial?.location || null,
                            city: city || partial?.city || null,
                            property_type: toPropertyType(data.category) || partial?.property_type || null,
                            purpose: toPurpose(data.purpose) || partial?.purpose || null,
                            Description: cleanText(data.description || data.shortDescription || ''),
                            url: data.link || request.url,
                            source: 'zameen.com',
                            external_id: String(data.externalID),
                        };

                        await Dataset.pushData(item);
                        saved++;
                        runStats.pushedFull++;
                        return;
                    }

                    // JSON-LD / HTML fallback (best-effort) if window.state is missing.
                    const $ = cheerioLoad(bodyText);
                    const jsonLd = pickJsonLdListing(parseJsonLdBlocks($));
                    const jsonLdOffers = jsonLd?.offers && typeof jsonLd.offers === 'object'
                        ? (Array.isArray(jsonLd.offers) ? jsonLd.offers[0] : jsonLd.offers)
                        : null;

                    const title = $('h1').first().text().trim() || $('title').text().trim() || partial?.title || null;
                    const description = cleanText(jsonLd?.description || partial?.Description || partial?.description || '');
                    const jsonLdCity = jsonLd?.address?.addressLocality || jsonLd?.address?.addressRegion || null;
                    const jsonLdLocation = jsonLd?.address?.streetAddress || jsonLd?.address?.addressLocality || null;
                    const jsonLdUrl = jsonLd?.url || null;
                    const jsonLdBedrooms = pickNumber(jsonLd?.numberOfRooms) ?? null;
                    const jsonLdBathrooms = pickNumber(jsonLd?.numberOfBathroomsTotal) ?? null;

                    const jsonLdFloorSizeValue = jsonLd?.floorSize?.value ?? null;
                    const jsonLdFloorSizeUnit = String(jsonLd?.floorSize?.unitCode || jsonLd?.floorSize?.unitText || '').toLowerCase();
                    const jsonLdArea =
                        jsonLdFloorSizeValue !== null && jsonLdFloorSizeValue !== undefined ? pickNumber(jsonLdFloorSizeValue) : null;
                    const jsonLdAreaUnit = jsonLdFloorSizeUnit
                        ? (jsonLdFloorSizeUnit.includes('mtk') || jsonLdFloorSizeUnit.includes('sqm') ? 'sqm'
                            : (jsonLdFloorSizeUnit.includes('sqf') || jsonLdFloorSizeUnit.includes('sqft') ? 'sqft' : null))
                        : null;
                    const item = {
                        title: jsonLd?.name || title || null,
                        price: pickNumber(jsonLdOffers?.price, partial?.price) ?? null,
                        currency: String(jsonLdOffers?.priceCurrency || partial?.currency || 'PKR'),
                        bedrooms: pickNumber(partial?.bedrooms, jsonLdBedrooms) ?? null,
                        bathrooms: pickNumber(partial?.bathrooms, jsonLdBathrooms) ?? null,
                        area: partial?.area ?? jsonLdArea ?? null,
                        area_unit: partial?.area_unit || jsonLdAreaUnit || null,
                        location: partial?.location || jsonLdLocation || null,
                        city: partial?.city || jsonLdCity || null,
                        property_type: partial?.property_type || null,
                        purpose: partial?.purpose || null,
                        Description: description || null,
                        url: jsonLdUrl || request.url,
                        source: 'zameen.com',
                        external_id: partial?.external_id || extractExternalIdFromPropertyUrl(request.url) || null,
                    };

                    await Dataset.pushData(item);
                    saved++;
                    runStats.pushedFallback++;
                }
            },
            failedRequestHandler: async ({ request, error }) => {
                const label = request.userData?.label || 'LIST';
                if (label === 'DETAIL') {
                    const partial = request.userData?.partial || null;
                    if (partial && typeof partial === 'object') {
                        await Dataset.pushData({ ...partial, Description: partial.Description || null });
                        saved++;
                        runStats.pushedFallback++;
                        return;
                    }
                }

                // Keep logs minimal and actionable.
                log.warning(`Request failed after retries: ${label} ${request.url} (${error?.message || 'unknown error'})`);
            },
        });

        await crawler.run();
        log.warning(
            `Finished. Saved ${saved} items (full=${runStats.pushedFull}, fallback=${runStats.pushedFallback}). ` +
            `Blocked(list=${runStats.blockedList}, detail=${runStats.blockedDetail}) HTTP(403=${runStats.http403}, 429=${runStats.http429}) queuedDetails=${runStats.queuedDetails}`
        );
    } finally {
        await Actor.exit();
    }
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
