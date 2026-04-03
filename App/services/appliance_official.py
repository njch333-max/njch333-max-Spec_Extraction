from __future__ import annotations

import html
import io
import re
from typing import Any, Callable
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import httpx
from pypdf import PdfReader

from App.services import cleaning_rules, parsing


ProgressCallback = Callable[[str, str], None] | None

SEARCH_ENDPOINTS = (
    "https://duckduckgo.com/html/",
    "https://www.bing.com/search",
)

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

SPEC_HINTS = ("spec", "specification", "product-sheet", "product sheet", "data sheet", "dimensions")
MANUAL_HINTS = ("manual", "user-manual", "user manual", "user guide", "instruction", "installation")
SIZE_HINTS = ("overall dimensions", "product dimensions", "dimensions", "size")

DIRECT_PRODUCT_PATHS = {
    "westinghouse": {
        "cooktop": ("/cooking/cooktops/{model}/",),
        "oven": ("/cooking/ovens/{model}/", "/cooking/freestanding-ovens/{model}/"),
        "under bench oven": ("/cooking/ovens/{model}/",),
        "rangehood": ("/cooking/rangehoods/{model}/",),
        "dishwasher": ("/dishwashing/dishwashers/{model}/",),
        "microwave": ("/cooking/microwaves/{model}/",),
    },
    "electrolux": {
        "cooktop": ("/cooking/cooktops/{model}/",),
        "oven": ("/cooking/ovens/{model}/", "/cooking/freestanding-ovens/{model}/"),
        "under bench oven": ("/cooking/ovens/{model}/",),
        "rangehood": ("/cooking/rangehoods/{model}/",),
        "dishwasher": ("/dishwashing/dishwashers/{model}/",),
        "microwave": ("/cooking/microwaves/{model}/",),
    },
    "aeg": {
        "cooktop": (
            "/cooking/cooktops/induction-cooktops/{model}/",
            "/cooking/cooktops/{model}/",
        ),
        "oven": (
            "/cooking/ovens/pyrolytic-ovens/{model}/",
            "/cooking/ovens/steam-ovens/{model}/",
            "/cooking/ovens/{model}/",
        ),
        "rangehood": (
            "/cooking/rangehoods/integrated-rangehoods/{model}/",
            "/cooking/rangehoods/{model}/",
        ),
        "dishwasher": (
            "/dishwashing/dishwashers/built-in-dishwashers/{model}/",
            "/dishwashing/dishwashers/{model}/",
        ),
    },
}


def enrich_appliance_rows(rows: list[dict[str, Any]], progress_callback: ProgressCallback = None, rule_flags: Any = None) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    cache: dict[tuple[str, str], dict[str, str]] = {}
    candidates = [row for row in rows if isinstance(row, dict)]
    target_rows = [row for row in candidates if _should_lookup(row)]
    total = len(target_rows)
    if not total:
        return [_normalized_appliance_row(row) for row in candidates]

    with httpx.Client(timeout=10.0, follow_redirects=True, headers=HTTP_HEADERS) as client:
        processed = 0
        for row in candidates:
            clean_row = _normalized_appliance_row(row)
            if not _should_lookup(clean_row):
                result.append(clean_row)
                continue
            processed += 1
            make = parsing.normalize_space(str(clean_row.get("make", "")))
            model_no = parsing.normalize_space(str(clean_row.get("model_no", "")))
            cache_key = (make.lower(), _normalized_model_token(model_no))
            if cache_key not in cache:
                _notify(progress_callback, "official_model_lookup", f"Official model lookup {processed}/{total}: {make} {model_no}")
                cache[cache_key] = lookup_official_appliance_resources(
                    make=make,
                    model_no=model_no,
                    appliance_type=str(clean_row.get("appliance_type", "")),
                    client=client,
                    progress_callback=progress_callback,
                    rule_flags=rule_flags,
                )
            resources = cache[cache_key]
            clean_row.update(resources)
            clean_row["website_url"] = clean_row.get("product_url", "") or ""
            result.append(clean_row)
    return result


def lookup_official_appliance_resources(
    make: str,
    model_no: str,
    appliance_type: str,
    client: httpx.Client,
    progress_callback: ProgressCallback = None,
    rule_flags: Any = None,
) -> dict[str, str]:
    empty = {"product_url": "", "spec_url": "", "manual_url": "", "website_url": "", "overall_size": ""}
    if not cleaning_rules.rule_enabled(rule_flags, "official_product_lookup"):
        return empty
    brand_home = _brand_home_url(make)
    if not brand_home or not model_no:
        return empty
    allowed_domains = _allowed_domains(make)
    if not allowed_domains:
        return empty

    product_html = ""
    fallback_pdfs = {"spec_url": "", "manual_url": ""}
    product_url, product_html = _probe_direct_product_page(client, make, model_no, appliance_type, brand_home)
    candidate_urls = [product_url] if product_url else []
    if not product_url:
        candidate_urls = _search_official_urls(client, make, model_no, appliance_type, allowed_domains)
        product_url, fallback_pdfs = _pick_product_and_pdf_candidates(candidate_urls, allowed_domains, model_no)
    page_links: list[str] = []
    if product_url and not product_html:
        _notify(progress_callback, "spec_manual_discovery", f"Inspecting official product page for {make} {model_no}")
        product_html = _safe_get_text(client, product_url)
    if product_url and product_html:
        page_links = _extract_links(product_html, product_url)

    all_links = _dedupe_urls([*candidate_urls, *page_links])
    spec_url = _pick_resource_url(all_links, allowed_domains, model_no, SPEC_HINTS)
    manual_url = _pick_resource_url(all_links, allowed_domains, model_no, MANUAL_HINTS)
    if not spec_url:
        spec_url = fallback_pdfs.get("spec_url", "")
    if not manual_url:
        manual_url = fallback_pdfs.get("manual_url", "")

    overall_size = ""
    if product_html:
        _notify(progress_callback, "official_size_extraction", f"Extracting official dimensions from product page for {make} {model_no}")
        overall_size = _extract_size_from_text(product_html) if cleaning_rules.rule_enabled(rule_flags, "official_overall_size_lookup") else ""
    if not overall_size and spec_url and cleaning_rules.rule_enabled(rule_flags, "official_overall_size_lookup"):
        _notify(progress_callback, "official_size_extraction", f"Extracting official dimensions from spec PDF for {make} {model_no}")
        overall_size = _extract_size_from_pdf(client, spec_url)

    return {
        "product_url": product_url,
        "spec_url": spec_url,
        "manual_url": manual_url,
        "website_url": product_url,
        "overall_size": overall_size,
    }


def _normalized_appliance_row(row: dict[str, Any]) -> dict[str, Any]:
    clean_row = dict(row)
    product_url = parsing.normalize_space(str(clean_row.get("product_url") or clean_row.get("website_url") or ""))
    clean_row["product_url"] = product_url if _looks_like_url(product_url) else ""
    clean_row["spec_url"] = parsing.normalize_space(str(clean_row.get("spec_url", ""))) if _looks_like_url(clean_row.get("spec_url", "")) else ""
    clean_row["manual_url"] = parsing.normalize_space(str(clean_row.get("manual_url", ""))) if _looks_like_url(clean_row.get("manual_url", "")) else ""
    clean_row["website_url"] = clean_row["product_url"]
    clean_row["overall_size"] = ""
    return clean_row


def _should_lookup(row: dict[str, Any]) -> bool:
    appliance_type = parsing.normalize_space(str(row.get("appliance_type", ""))).lower()
    if any(token in appliance_type for token in ("sink", "basin", "tap", "tub")):
        return False
    make = parsing.normalize_space(str(row.get("make", "")))
    model_no = parsing.normalize_space(str(row.get("model_no", "")))
    placeholder_tokens = (
        "as above",
        "by client",
        "provide space only",
        "leave standard space",
        "not included",
    )
    if model_no and any(token in model_no.lower() for token in placeholder_tokens):
        return False
    return bool(make and model_no)


def _brand_home_url(make: str) -> str:
    lowered = make.strip().lower()
    for brand, url in sorted(parsing.KNOWN_BRANDS.items(), key=lambda item: len(item[0]), reverse=True):
        if brand == lowered or brand in lowered or lowered in brand:
            return url
    return ""


def _allowed_domains(make: str) -> set[str]:
    domains: set[str] = set()
    brand_home = _brand_home_url(make)
    if not brand_home:
        return domains
    host = urlparse(brand_home).netloc.lower()
    if host:
        domains.add(host)
        if host.startswith("www."):
            domains.add(host[4:])
        else:
            domains.add(f"www.{host}")
    return domains


def _search_official_urls(
    client: httpx.Client,
    make: str,
    model_no: str,
    appliance_type: str,
    allowed_domains: set[str],
) -> list[str]:
    model_token = _primary_model_token(model_no) or model_no
    queries = []
    host = sorted(allowed_domains)[0]
    queries.append(f"site:{host} {make} {model_token}")
    if appliance_type:
        queries.append(f"site:{host} {make} {appliance_type} {model_token}")
    queries.append(f"{make} {model_token}")

    urls: list[str] = []
    for endpoint in SEARCH_ENDPOINTS:
        for query in queries:
            try:
                response = client.get(endpoint, params={"q": query} if "duckduckgo" in endpoint or "bing" in endpoint else None)
                if "duckduckgo" not in endpoint and "bing" not in endpoint:
                    response = client.get(f"{endpoint}?q={quote_plus(query)}")
                response.raise_for_status()
            except Exception:
                continue
            urls.extend(_extract_search_urls(response.text, allowed_domains))
            if urls:
                break
        if urls:
            break
    return _dedupe_urls(urls)


def _extract_search_urls(page_html: str, allowed_domains: set[str]) -> list[str]:
    urls: list[str] = []
    for raw_href in re.findall(r"""href=["']([^"']+)["']""", page_html, flags=re.IGNORECASE):
        decoded = _decode_search_href(raw_href)
        if not decoded or not decoded.startswith(("http://", "https://")):
            continue
        host = urlparse(decoded).netloc.lower()
        if not _host_allowed(host, allowed_domains):
            continue
        urls.append(decoded)
    return _dedupe_urls(urls)


def _decode_search_href(raw_href: str) -> str:
    href = html.unescape(raw_href.strip())
    if href.startswith("//"):
        href = f"https:{href}"
    if href.startswith("/l/?") or href.startswith("https://duckduckgo.com/l/?"):
        parsed = urlparse(href)
        target = parse_qs(parsed.query).get("uddg", [""])[0]
        if target:
            return unquote(target)
    if href.startswith("/url?") or href.startswith("https://www.google.com/url?"):
        parsed = urlparse(href)
        target = parse_qs(parsed.query).get("q", [""])[0]
        if target:
            return unquote(target)
    return href


def _pick_product_and_pdf_candidates(
    urls: list[str],
    allowed_domains: set[str],
    model_no: str,
) -> tuple[str, dict[str, str]]:
    best_product = ""
    best_score = -1
    fallback_pdfs = {"spec_url": "", "manual_url": ""}
    model_token = _normalized_model_token(_primary_model_token(model_no) or model_no)
    for url in urls:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        path = parsed.path.lower()
        if not _host_allowed(host, allowed_domains):
            continue
        lowered = url.lower()
        if lowered.endswith(".pdf"):
            if not fallback_pdfs["spec_url"] and any(hint in lowered for hint in SPEC_HINTS):
                fallback_pdfs["spec_url"] = url
            if not fallback_pdfs["manual_url"] and any(hint in lowered for hint in MANUAL_HINTS):
                fallback_pdfs["manual_url"] = url
            continue
        score = 0
        if model_token and model_token in _normalized_model_token(url):
            score += 8
        if lowered.endswith(".html"):
            score += 2
        if any(token in lowered for token in ("product", "appliance", "shop", "catalogue", "details")):
            score += 2
        if any(token in path for token in ("/cooling/", "/cooking/", "/dishwashing/", "/refrigeration/", "/integrated-refrigeration/")):
            score += 4
        if "support" in host or "/support/" in path or "/s/article/" in path:
            score -= 5
        if score > best_score:
            best_product = url
            best_score = score
    return best_product, fallback_pdfs


def _pick_resource_url(urls: list[str], allowed_domains: set[str], model_no: str, hints: tuple[str, ...]) -> str:
    best_url = ""
    best_score = -1
    model_token = _normalized_model_token(_primary_model_token(model_no) or model_no)
    for url in urls:
        host = urlparse(url).netloc.lower()
        if not _host_allowed(host, allowed_domains):
            continue
        lowered = url.lower()
        score = 0
        if not any(hint in lowered for hint in hints):
            continue
        if lowered.endswith(".pdf"):
            score += 4
        if model_token and model_token in _normalized_model_token(url):
            score += 6
        if score > best_score:
            best_url = url
            best_score = score
    return best_url


def _extract_links(page_html: str, base_url: str) -> list[str]:
    urls: list[str] = []
    for raw_href in re.findall(r"""href=["']([^"']+)["']""", page_html, flags=re.IGNORECASE):
        href = html.unescape(raw_href.strip())
        if href.startswith(("mailto:", "tel:", "#", "javascript:")):
            continue
        absolute = urljoin(base_url, href)
        if absolute.startswith(("http://", "https://")):
            urls.append(absolute)
    return _dedupe_urls(urls)


def _extract_size_from_pdf(client: httpx.Client, pdf_url: str) -> str:
    try:
        response = client.get(pdf_url)
        response.raise_for_status()
        reader = PdfReader(io.BytesIO(response.content))
    except Exception:
        return ""

    texts: list[str] = []
    for page in reader.pages[:3]:
        try:
            texts.append(page.extract_text() or "")
        except Exception:
            continue
    return _extract_size_from_text("\n".join(texts))


def _extract_size_from_text(text: str) -> str:
    plain_text = re.sub(r"<[^>]+>", " ", html.unescape(text or ""))
    normalized = parsing.normalize_space(plain_text)
    if not normalized:
        return ""
    structured_match = re.search(
        r'"height".*?"value":"?(\d{2,4}\s*mm)"?.*?"width".*?"value":"?(\d{2,4}\s*mm)"?.*?"depth".*?"value":"?(\d{2,4}\s*mm)"?',
        normalized,
        re.IGNORECASE | re.DOTALL,
    )
    if structured_match:
        return " x ".join(
            [
                f"{parsing.normalize_space(structured_match.group(1))} (H)",
                f"{parsing.normalize_space(structured_match.group(2))} (W)",
                f"{parsing.normalize_space(structured_match.group(3))} (D)",
            ]
        )
    labelled_patterns = (
        r"(\d{2,4}\s*mm\s*\(H\))\s*(\d{2,4}\s*mm\s*\(W\))\s*(\d{2,4}\s*mm\s*\(D\))",
        r"Height[^0-9]{0,30}(\d{2,4}\s*mm).{0,120}?Width[^0-9]{0,30}(\d{2,4}\s*mm).{0,120}?Depth[^0-9]{0,30}(\d{2,4}\s*mm)",
        r"(height[^0-9]{0,20}\d{2,4}\s*mm).{0,80}?(width[^0-9]{0,20}\d{2,4}\s*mm).{0,80}?(depth[^0-9]{0,20}\d{2,4}\s*mm)",
        r"(total product height[^0-9]{0,20}\d{2,4}\s*mm).{0,80}?(total product width[^0-9]{0,20}\d{2,4}\s*mm).{0,80}?(total product depth[^0-9]{0,20}\d{2,4}\s*mm)",
    )
    for pattern in labelled_patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match:
            parts = []
            for index, group in enumerate(match.groups()):
                if not group:
                    continue
                if index == 0 and re.fullmatch(r"\d{2,4}\s*mm", group, re.IGNORECASE):
                    parts.append(f"{parsing.normalize_space(group)} (H)")
                elif index == 1 and re.fullmatch(r"\d{2,4}\s*mm", group, re.IGNORECASE):
                    parts.append(f"{parsing.normalize_space(group)} (W)")
                elif index == 2 and re.fullmatch(r"\d{2,4}\s*mm", group, re.IGNORECASE):
                    parts.append(f"{parsing.normalize_space(group)} (D)")
                else:
                    parts.append(_normalize_dimension_label(group))
            if len(parts) == 3:
                return " x ".join(parts)
    for hint in SIZE_HINTS:
        match = re.search(
            rf"{re.escape(hint)}[^0-9]{{0,80}}(\d{{2,4}}\s*[xX×]\s*\d{{2,4}}\s*[xX×]\s*\d{{2,4}}\s*(?:mm|cm)?)",
            normalized,
            re.IGNORECASE,
        )
        if match:
            return _clean_size(match.group(1))
    match = re.search(r"(\d{2,4}\s*[xX×]\s*\d{2,4}\s*[xX×]\s*\d{2,4}\s*(?:mm|cm)?)", normalized, re.IGNORECASE)
    if match:
        return _clean_size(match.group(1))
    return ""


def _clean_size(value: str) -> str:
    text = parsing.normalize_space(value)
    text = text.replace("×", " x ")
    text = re.sub(r"\s*[xX]\s*", " x ", text)
    return re.sub(r"\s{2,}", " ", text).strip()


def _normalize_dimension_label(value: str) -> str:
    text = parsing.normalize_space(value)
    shorthand = re.match(r"(\d{2,4}\s*mm)\s*\(([HWD])\)", text, re.IGNORECASE)
    if shorthand:
        return f"{shorthand.group(1)} ({shorthand.group(2).upper()})"
    patterns = (
        (r"(?:total product )?height[^0-9]{0,20}(\d{2,4}\s*mm)", "H"),
        (r"(?:total product )?width[^0-9]{0,20}(\d{2,4}\s*mm)", "W"),
        (r"(?:total product )?depth[^0-9]{0,20}(\d{2,4}\s*mm)", "D"),
    )
    for pattern, label in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return f"{match.group(1)} ({label})"
    return text


def _safe_get_text(client: httpx.Client, url: str) -> str:
    try:
        response = client.get(url)
        response.raise_for_status()
    except Exception:
        return ""
    return response.text


def _probe_direct_product_page(
    client: httpx.Client,
    make: str,
    model_no: str,
    appliance_type: str,
    brand_home: str,
) -> tuple[str, str]:
    model_token = _primary_model_token(model_no)
    if not model_token:
        return "", ""
    for candidate_url in _build_direct_product_candidates(make, appliance_type, model_token, brand_home):
        text = _safe_get_text(client, candidate_url)
        if _looks_like_valid_product_page(text, candidate_url, model_token):
            return candidate_url, text
    return "", ""


def _build_direct_product_candidates(make: str, appliance_type: str, model_no: str, brand_home: str | None = None) -> list[str]:
    brand = _brand_key(make)
    home = (brand_home or _brand_home_url(make)).rstrip("/")
    model_token = (_primary_model_token(model_no) or model_no).lower()
    if not brand or not home or not model_token:
        return []
    candidates: list[str] = []
    for category in _product_categories(appliance_type):
        for template in DIRECT_PRODUCT_PATHS.get(brand, {}).get(category, ()):
            candidates.append(urljoin(f"{home}/", template.format(model=model_token).lstrip("/")))
    return _dedupe_urls(candidates)


def _product_categories(appliance_type: str) -> list[str]:
    lowered = parsing.normalize_space(appliance_type).lower()
    categories: list[str] = []
    if "under bench oven" in lowered:
        categories.append("under bench oven")
    if "oven" in lowered:
        categories.append("oven")
    if "cooktop" in lowered:
        categories.append("cooktop")
    if "rangehood" in lowered:
        categories.append("rangehood")
    if "dishwasher" in lowered:
        categories.append("dishwasher")
    if "microwave" in lowered:
        categories.append("microwave")
    return categories


def _looks_like_valid_product_page(text: str, url: str, model_token: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if "<title>404" in lowered or "url not found" in lowered or "page not found" in lowered:
        return False
    token = _normalized_model_token(model_token)
    return bool(token and token in _normalized_model_token(f"{url} {text[:8000]}"))


def _brand_key(make: str) -> str:
    lowered = parsing.normalize_space(make).lower()
    if "westinghouse" in lowered:
        return "westinghouse"
    if "electrolux" in lowered:
        return "electrolux"
    if lowered == "aeg" or " aeg " in f" {lowered} ":
        return "aeg"
    if "fisher" in lowered and "paykel" in lowered:
        return "fisher-paykel"
    return ""


def _primary_model_token(value: str) -> str:
    tokens = re.findall(r"[A-Za-z][A-Za-z0-9-]*\d[A-Za-z0-9-]*", str(value or ""))
    if not tokens:
        return ""
    return max(tokens, key=len)


def _normalized_model_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _host_allowed(host: str, allowed_domains: set[str]) -> bool:
    host = host.lower()
    return any(host == allowed or host.endswith(f".{allowed}") for allowed in allowed_domains)


def _looks_like_url(value: Any) -> bool:
    text = parsing.normalize_space(str(value or ""))
    return text.startswith(("http://", "https://"))


def _dedupe_urls(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for url in urls:
        clean = url.strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        result.append(clean)
    return result


def _notify(progress_callback: ProgressCallback, stage: str, message: str) -> None:
    if progress_callback:
        progress_callback(stage, message)
