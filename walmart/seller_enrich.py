from typing import Any, Dict, Optional, List

import json
import re
import time
from urllib.parse import urljoin, urlparse, quote_plus

import requests
from bs4 import BeautifulSoup


def fetch_walmart_seller_profile_url(listing_url: Optional[str], seller_name: Optional[str] = None, seller_id: Optional[str] = None) -> Optional[str]:
	if not listing_url and not seller_id:
		return None
	# Attempt via product page first
	if listing_url:
		try:
			resp = requests.get(listing_url, timeout=30)
			resp.raise_for_status()
			soup = BeautifulSoup(resp.text, "html.parser")
			anchor = soup.select_one('a[href*="/seller/"]') or soup.find('a', string=lambda s: s and 'seller' in s.lower())
			if anchor and anchor.get('href'):
				href = anchor['href']
				if href.startswith('http'):
					return href
				return f"https://www.walmart.com{href}"
			# JSON-LD seller url
			for script in soup.find_all('script', attrs={'type': 'application/ld+json'}):
				try:
					data = json.loads(script.string or '{}')
				except Exception:
					continue
				nodes: List[Dict[str, Any]] = data if isinstance(data, list) else [data]
				for node in nodes:
					if not isinstance(node, dict):
						continue
					seller = node.get('seller')
					if isinstance(seller, dict):
						url = seller.get('url')
						if isinstance(url, str) and url:
							return url
		except Exception:
			pass
	# Construct probable URL from name/id
	if seller_id and seller_name:
		slug = re.sub(r'[^a-z0-9]+', '-', seller_name.strip().lower())
		slug = slug.strip('-')
		return f"https://www.walmart.com/seller/{slug}/{seller_id}"
	return None


def scrape_walmart_seller_profile(profile_url: str) -> Dict[str, Optional[str]]:
	try:
		headers = {
			"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
		}
		resp = requests.get(profile_url, headers=headers, timeout=30)
		resp.raise_for_status()
		soup = BeautifulSoup(resp.text, "html.parser")
		# Guard against bot/challenge interstitials contaminating fields
		page_text_lower = resp.text.lower()
		challenge_markers = [
			"robot or human",
			"are you human",
			"captcha",
			"access denied",
			"blocked" 
		]
		if any(marker in page_text_lower for marker in challenge_markers):
			return {
				"seller_profile_picture": None,
				"seller_profile_url": profile_url,
				"business_legal_name": None,
				"country": None,
				"state_province": None,
				"zip_code": None,
				"phone_number": None,
				"address": None,
				"email_address": None,
			}
		# Best-effort extraction; Walmart page structures change often
		name = None
		img = None
		address_block = None
		phone = None
		email = None
		country = None
		state_province = None
		zip_code = None
		# Common selectors
		name_el = soup.select_one('h1, h2, .seller-name, [data-testid="seller-name"]')
		if name_el:
			candidate = name_el.get_text(strip=True)
			if candidate and candidate.strip().lower() not in ("robot or human?", "robot or human"):
				name = candidate
		img_el = soup.select_one('img[alt*="seller" i], .seller-avatar img')
		if img_el and img_el.get('src'):
			img = img_el['src']
		# Address and phone often appear in info blocks
		info_text = ' '.join(el.get_text(" ", strip=True) for el in soup.select('.seller-info, .about-seller, .seller-details, address, [data-testid="about-seller"], .lh-copy'))
		if info_text:
			address_block = info_text
			# naive phone/email pull
			m_phone = re.search(r"(\+?\d[\d\s\-()]{7,}\d)", info_text)
			if m_phone:
				phone = m_phone.group(1)
			m_email = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", info_text)
			if m_email:
				email = m_email.group(0)
		# mailto/tel links
		mailto = soup.select_one('a[href^="mailto:"]')
		if mailto and not email:
			email = (mailto.get('href') or '').replace('mailto:', '').strip()
		tel = soup.select_one('a[href^="tel:"]')
		if tel and not phone:
			phone = (tel.get('href') or '').replace('tel:', '').strip()
		# Parse JSON-LD for structured contact
		for script in soup.find_all('script', attrs={'type': 'application/ld+json'}):
			try:
				data = json.loads(script.string or '{}')
			except Exception:
				continue
			nodes: List[Dict[str, Any]] = data if isinstance(data, list) else [data]
			for node in nodes:
				if not isinstance(node, dict):
					continue
				if node.get('@type') in ('Organization', 'Store', 'LocalBusiness'):
					name = name or node.get('name')
					img = img or node.get('image')
					email = email or node.get('email')
					phone = phone or node.get('telephone')
					addr = node.get('address') if isinstance(node.get('address'), dict) else None
					if addr:
						parts = [addr.get('streetAddress'), addr.get('addressLocality'), addr.get('addressRegion'), addr.get('postalCode'), addr.get('addressCountry')]
						address_block = address_block or ' '.join([p for p in parts if p])
						state_province = state_province or addr.get('addressRegion')
						zip_code = zip_code or addr.get('postalCode')
						country = country or (addr.get('addressCountry') if isinstance(addr.get('addressCountry'), str) else None)
		# Parse embedded window state for more details
		state_scripts: List[str] = []
		for script in soup.find_all('script'):
			text = script.string or script.get_text() or ''
			if '__WML_REDUX_INITIAL_STATE__' in text or '__WML_STATE__' in text:
				state_scripts.append(text)
		for raw in state_scripts:
			try:
				m = re.search(r"(__WML_(?:REDUX_)?INITIAL_STATE__|__WML_STATE__)\s*=\s*(\{[\s\S]*?\})\s*;", raw)
				if not m:
					continue
				payload = m.group(2)
				state = json.loads(payload)
				# Heuristic paths where seller profile info may live
				candidates: List[Any] = []
				if isinstance(state, dict):
					for key in ("seller", "sellerProfile", "entities", "pageData", "global", "features"):
						val = state.get(key)
						if val is not None:
							candidates.append(val)
					# Deep search small dicts for known fields
					stack: List[Any] = candidates[:]
					visited: set = set()
					while stack:
						node = stack.pop()
						if id(node) in visited:
							continue
						visited.add(id(node))
						if isinstance(node, dict):
							# Pull common fields if present
							name = name or node.get('displayName') or node.get('name') or node.get('legalName')
							img = img or node.get('logo') or node.get('image')
							email = email or node.get('email')
							phone = phone or node.get('phone') or node.get('telephone')
							addr_obj = node.get('address') if isinstance(node.get('address'), dict) else None
							if addr_obj:
								parts = [addr_obj.get('street1') or addr_obj.get('streetAddress'), addr_obj.get('city') or addr_obj.get('addressLocality'), addr_obj.get('state') or addr_obj.get('addressRegion'), addr_obj.get('zip') or addr_obj.get('postalCode'), addr_obj.get('country') or addr_obj.get('addressCountry')]
								address_block = address_block or ' '.join([p for p in parts if p])
								state_province = state_province or (addr_obj.get('state') or addr_obj.get('addressRegion'))
								zip_code = zip_code or (addr_obj.get('zip') or addr_obj.get('postalCode'))
								country = country or (addr_obj.get('country') or addr_obj.get('addressCountry'))
							for v in node.values():
								if isinstance(v, (dict, list)):
									stack.append(v)
						elif isinstance(node, list):
							for v in node:
								if isinstance(v, (dict, list)):
									stack.append(v)
			except Exception:
				# Best-effort; ignore JSON errors
				pass
		# Follow policy/returns/contact links for additional contact details
		policy_links: List[str] = []
		for a in soup.find_all('a'):
			text = (a.get_text(strip=True) or '').lower()
			href = a.get('href') or ''
			if any(token in text for token in ["policy", "policies", "return", "shipping", "contact", "details", "about"]):
				if href and not href.startswith('#'):
					policy_links.append(urljoin(profile_url, href))
		# Deduplicate while keeping order
		seen = set()
		unique_policy_links = []
		for link in policy_links:
			if link not in seen:
				seen.add(link)
				unique_policy_links.append(link)
		for link in unique_policy_links[:5]:
			try:
				resp2 = requests.get(link, headers=headers, timeout=20)
				if resp2.status_code != 200:
					continue
				s2 = BeautifulSoup(resp2.text, 'html.parser')
				text2 = s2.get_text(" ", strip=True)
				if not email:
					m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text2)
					if m:
						email = m.group(0)
				if not phone:
					m = re.search(r"(\+?\d[\d\s\-()]{7,}\d)", text2)
					if m:
						phone = m.group(1)
				# JSON-LD on policy pages too
				for script in s2.find_all('script', attrs={'type': 'application/ld+json'}):
					try:
						data = json.loads(script.string or '{}')
						nodes: List[Dict[str, Any]] = data if isinstance(data, list) else [data]
						for node in nodes:
							if not isinstance(node, dict):
								continue
							if node.get('@type') in ('Organization', 'Store', 'LocalBusiness'):
								email = email or node.get('email')
								phone = phone or node.get('telephone')
								addr = node.get('address') if isinstance(node.get('address'), dict) else None
								if addr and not address_block:
									parts = [addr.get('streetAddress'), addr.get('addressLocality'), addr.get('addressRegion'), addr.get('postalCode'), addr.get('addressCountry')]
									address_block = ' '.join([p for p in parts if p])
					except Exception:
						continue
			except Exception:
				continue
			# be polite between requests
			time.sleep(0.3)
		return {
			"seller_profile_picture": img,
			"seller_profile_url": profile_url,
			"business_legal_name": name,
			"country": country,
			"state_province": state_province,
			"zip_code": zip_code,
			"phone_number": phone,
			"address": address_block,
			"email_address": email,
		}
	except Exception:
		return {
			"seller_profile_picture": None,
			"seller_profile_url": profile_url,
			"business_legal_name": None,
			"country": None,
			"state_province": None,
			"zip_code": None,
			"phone_number": None,
			"address": None,
			"email_address": None,
		}



# ----------------- External web enrichment (best-effort) -----------------

_GENERIC_HEADERS = {
	"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
}


def _normalize_base(url: str) -> str:
	try:
		o = urlparse(url)
		return f"{o.scheme}://{o.netloc}/"
	except Exception:
		return url


def _safe_get(url: str, timeout: int = 20) -> Optional[str]:
	try:
		resp = requests.get(url, headers=_GENERIC_HEADERS, timeout=timeout)
		if resp.status_code == 200 and resp.text:
			return resp.text
	except Exception:
		return None
	return None


def find_seller_official_site(seller_name: str) -> Optional[str]:
	"""Use DuckDuckGo HTML as a lightweight search to guess the seller's official site."""
	if not seller_name:
		return None
	query = quote_plus(f"{seller_name} official site")
	search_url = f"https://duckduckgo.com/html/?q={query}"
	html = _safe_get(search_url)
	if not html:
		return None
	soup = BeautifulSoup(html, "html.parser")
	# Prefer result links; skip large marketplaces and social sites
	blacklist = ["walmart.com", "amazon.", "ebay.", "facebook.com", "instagram.com", "twitter.com", "x.com", "linkedin.com", "pinterest.com"]
	for a in soup.find_all('a'):
		href = a.get('href') or ''
		text = (a.get_text(strip=True) or '').lower()
		if not href.startswith('http'):
			continue
		if any(b in href for b in blacklist):
			continue
		# Heuristic: link text includes seller name tokens
		name_tokens = [t for t in re.split(r"\W+", seller_name.lower()) if t]
		if any(t in text or t in href.lower() for t in name_tokens):
			return _normalize_base(href)
	return None


def scrape_contact_from_site(base_url: str) -> Dict[str, Optional[str]]:
	"""Fetch homepage and likely contact/support/about page to extract email/phone/address."""
	if not base_url:
		return {}
	home_html = _safe_get(base_url)
	if not home_html:
		return {}
	soup = BeautifulSoup(home_html, "html.parser")
	# Candidate links
	links: List[str] = []
	for a in soup.find_all('a'):
		text = (a.get_text(strip=True) or '').lower()
		href = a.get('href') or ''
		if any(k in text for k in ["contact", "support", "customer service", "help", "about", "privacy", "terms"]):
			if href and not href.startswith('#'):
				links.append(urljoin(base_url, href))
	# Always include homepage as last resort
	links.append(base_url)
	seen = set()
	info: Dict[str, Optional[str]] = {"email_address": None, "phone_number": None, "address": None, "country": None, "state_province": None, "zip_code": None}
	for link in links[:6]:
		if link in seen:
			continue
		seen.add(link)
		h = _safe_get(link)
		if not h:
			continue
		s = BeautifulSoup(h, "html.parser")
		text = s.get_text(" ", strip=True)
		if not info.get("email_address"):
			m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
			if m:
				info["email_address"] = m.group(0)
		if not info.get("phone_number"):
			m = re.search(r"(\+?\d[\d\s\-()]{7,}\d)", text)
			if m:
				info["phone_number"] = m.group(1)
		# JSON-LD for address
		for script in s.find_all('script', attrs={'type': 'application/ld+json'}):
			try:
				data = json.loads(script.string or '{}')
			except Exception:
				continue
			nodes: List[Dict[str, Any]] = data if isinstance(data, list) else [data]
			for node in nodes:
				if not isinstance(node, dict):
					continue
				if node.get('@type') in ('Organization', 'LocalBusiness', 'Store', 'Corporation'):
					addr = node.get('address') if isinstance(node.get('address'), dict) else None
					if addr and not info.get('address'):
						parts = [addr.get('streetAddress'), addr.get('addressLocality'), addr.get('addressRegion'), addr.get('postalCode'), addr.get('addressCountry')]
						info['address'] = ' '.join([p for p in parts if p])
						info['state_province'] = info['state_province'] or addr.get('addressRegion')
						info['zip_code'] = info['zip_code'] or addr.get('postalCode')
						info['country'] = info['country'] or (addr.get('addressCountry') if isinstance(addr.get('addressCountry'), str) else None)
		# Stop early if we have at least one of email/phone
		if info.get('email_address') or info.get('phone_number'):
			break
		# politeness
		time.sleep(0.2)
	return info


def web_enrich_seller(seller_name: Optional[str]) -> Dict[str, Optional[str]]:
	"""High-level: guess official site for seller_name and scrape contact details."""
	if not seller_name:
		return {}
	base = find_seller_official_site(seller_name)
	if not base:
		return {}
	return scrape_contact_from_site(base)


