import { Actor, log } from 'apify';
import { Dataset, HttpCrawler } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

const DEFAULT_START_URL = 'https://www.zameen.com/Homes/Islamabad_DHA_Defence-3188-1.html';
const USE_ALGOLIA_API = true;
const MAX_CONCURRENCY = 10;
const MAX_REQUESTS_PER_MINUTE = 300;

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

const isPropertyDetailUrl = (urlString) => {
    if (!urlString) return false;
    const u = String(urlString);
    return /https?:\/\/www\.zameen\.com\//i.test(u) && /\/Property\//i.test(u) && /-\d+\.html($|\?)/i.test(u);
};

const inferListPageNoFromUrl = (urlString) => {
    const u = String(urlString || '');
    const match = u.match(/-(\d+)\.html(?:$|\?)/i);
    return match ? Math.max(1, Number(match[1])) : 1;
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
        if (!initial.length) {
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

        const enqueueDetail = async ({ url, externalId, partial }) => {
            const uniqueKey = externalId ? `detail:${externalId}` : url;
            if (seenDetail.has(uniqueKey)) return;
            if (reserved >= MAX_RESERVATIONS) return;
            seenDetail.add(uniqueKey);
            reserved++;
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

        for (const uRaw of initial) {
            const u = String(uRaw);
            const pageNo = isPropertyDetailUrl(u) ? 1 : inferListPageNoFromUrl(u);
            await requestQueue.addRequest({
                url: u,
                userData: isPropertyDetailUrl(u) ? { label: 'DETAIL' } : { label: 'LIST', pageNo },
                uniqueKey: u,
            });
        }

        const crawler = new HttpCrawler({
            requestQueue,
            proxyConfiguration: proxyConf,
            useSessionPool: true,
            maxRequestRetries: 6,
            maxConcurrency: MAX_CONCURRENCY,
            maxRequestsPerMinute: MAX_REQUESTS_PER_MINUTE,
            requestHandlerTimeoutSecs: 180,
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
                    gotOptions.timeout = { request: 30000 };
                },
            ],
            async requestHandler({ request, body, response, session, log: crawlerLog }) {
                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;
                const bodyText = body?.toString?.() || '';

                if ((response?.statusCode === 403 || response?.statusCode === 429) && session) {
                    crawlerLog.warning(`HTTP ${response.statusCode} for ${request.url} (retiring session)`);
                    session.retire();
                }

                if (label === 'LIST') {
                    if (looksBlocked(bodyText) && session) {
                        crawlerLog.warning(`Blocked/captcha-like response on LIST ${request.url} (retiring session)`);
                        session.retire();
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
                        if (!/zameen\.com/i.test(normalized)) return;
                        if (/(blog|guide|news|about|contact)/i.test(normalized)) return;
                        if (!isPropertyDetailUrl(normalized)) return;
                        candidates.push({ url: normalized, hit: null });
                    });

                    const remaining = MAX_RESERVATIONS - reserved;
                    const localSeen = new Set();
                    const uniqueCandidates = [];
                    for (const c of candidates) {
                        const externalId = c?.hit?.externalID ? String(c.hit.externalID) : null;
                        const key = externalId ? `detail:${externalId}` : c?.url;
                        if (!key) continue;
                        if (localSeen.has(key)) continue;
                        if (seenDetail.has(key)) continue;
                        localSeen.add(key);
                        uniqueCandidates.push(c);
                    }
                    const toQueue = uniqueCandidates.slice(0, Math.max(0, remaining));

                    crawlerLog.info(`LIST page ${pageNo}: discovered=${uniqueCandidates.length}, remaining=${remaining}, willQueue=${toQueue.length}`);

                    if (scrapeDetails) {
                        for (const { url, hit } of toQueue) {
                            const externalId = hit?.externalID ? String(hit.externalID) : null;
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
                            } else {
                                crawlerLog.info(`LIST page ${pageNo}: no next page detected`);
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
                        const externalId = hit?.externalID ? String(hit.externalID) : null;
                        const url = toDetailUrlFromHit(hit);
                        const key = externalId ? `detail:${externalId}` : url;
                        if (!key) continue;
                        if (localSeen.has(key)) continue;
                        if (seenDetail.has(key)) continue;
                        localSeen.add(key);
                        uniqueHits.push(hit);
                    }
                    const toQueue = uniqueHits.slice(0, Math.max(0, remaining));
                    crawlerLog.info(`ALGOLIA page ${pageNo}: hits=${hits.length}, unique=${uniqueHits.length}, remaining=${remaining}, willQueue=${toQueue.length}`);

                    if (scrapeDetails) {
                        for (const hit of toQueue) {
                            const url = toDetailUrlFromHit(hit);
                            if (!url) continue;

                            const externalId = hit?.externalID ? String(hit.externalID) : null;
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

                    if (looksBlocked(bodyText) && session) {
                        crawlerLog.warning(`Blocked/captcha-like response on DETAIL ${request.url} (retiring session)`);
                        session.retire();
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
                        return;
                    }

                    // HTML fallback (best-effort) if window.state is missing.
                    const $ = cheerioLoad(bodyText);
                    const title = $('h1').first().text().trim() || $('title').text().trim() || partial?.title || null;
                    const description = cleanText(partial?.Description || partial?.description || '');
                    const item = {
                        title: title || null,
                        price: partial?.price ?? null,
                        currency: partial?.currency || 'PKR',
                        bedrooms: partial?.bedrooms ?? null,
                        bathrooms: partial?.bathrooms ?? null,
                        area: partial?.area ?? null,
                        area_unit: partial?.area_unit || null,
                        location: partial?.location || null,
                        city: partial?.city || null,
                        property_type: partial?.property_type || null,
                        purpose: partial?.purpose || null,
                        Description: description || null,
                        url: request.url,
                        source: 'zameen.com',
                        external_id: partial?.external_id || null,
                    };

                    await Dataset.pushData(item);
                    saved++;
                }
            },
        });

        await crawler.run();
        log.info(`Finished. Saved ${saved} items`);
    } finally {
        await Actor.exit();
    }
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
