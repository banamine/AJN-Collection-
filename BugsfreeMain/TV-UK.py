import requests
import json
import os
import re
from urllib.parse import urlparse
from collections import defaultdict
from datetime import datetime
import pytz
import concurrent.futures
import threading
import logging
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Entries we never publish: streams with a login embedded in the URL (resold panel feeds)
# and adult groups.
PANEL_RESTREAM = re.compile(r'^https?://[^/]+:\d+/[^/]+/[^/]+/\d+(\.\w+)?$', re.I)
CREDENTIAL_QUERY = re.compile(r'[?&](username|password)=', re.I)
ADULT_GROUP = re.compile(r'\b(xxx|adults?)\b', re.I)

class M3UCollector:
    def __init__(self, country="UK", base_dir="LiveTV", check_links=True):
        self.channels = defaultdict(list)
        self.default_logo = "https://bugsfreeweb.github.io/LiveTVCollector/BugsfreeLogo/default-logo.png"
        self.seen_urls = set()
        self.url_status_cache = {}
        self.output_dir = os.path.join(base_dir, country)
        self.lock = threading.Lock()
        self.check_links = check_links  # Toggle link checking
        os.makedirs(self.output_dir, exist_ok=True)

    def fetch_content(self, url):
        """Fetch content (M3U or HTML) with streaming."""
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        
        try:
            with requests.get(url, stream=True, headers=headers, timeout=10) as response:
                response.raise_for_status()
                lines = [line.decode('utf-8', errors='ignore') if isinstance(line, bytes) else line for line in response.iter_lines()]
                content = '\n'.join(lines)
                if not lines:
                    logging.warning(f"No content fetched from {url}")
                else:
                    logging.info(f"Fetched {len(lines)} lines from {url}")
                return content, lines
        except requests.RequestException as e:
            logging.error(f"Failed to fetch {url}: {str(e)}")
            return None, []

    def extract_stream_urls_from_html(self, html_content, base_url):
        """Extract streaming URLs from HTML."""
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'html.parser')
        stream_urls = set()
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            parsed_base = urlparse(base_url)
            parsed_href = urlparse(href)
            if not parsed_href.scheme:
                href = f"{parsed_base.scheme}://{parsed_base.netloc}{href}"
            
            if (href.endswith(('.m3u', '.m3u8')) or 
                re.match(r'^https?://.*\.(ts|mp4|avi|mkv|flv|wmv)$', href) or 
                'playlist' in href.lower() or 'stream' in href.lower()):
                if not any(exclude in href.lower() for exclude in ['telegram', '.html', '.php', 'github.com', 'login', 'signup']):
                    stream_urls.add(href)
        
        logging.info(f"Extracted {len(stream_urls)} streaming URLs from {base_url}")
        return list(stream_urls)

    def _probe_link(self, url, headers, read_timeout):
        """True if the stream looks alive. Geo/auth-blocked (403/451) and rate-limited (429) streams
        are kept; dead (404/410), unreachable, error-page and non-manifest responses are dropped."""
        for attempt in range(2):  # one retry for timeouts, connection errors and 5xx
            try:
                with requests.get(url, stream=True, headers=headers, timeout=(5, read_timeout), allow_redirects=True) as r:
                    code = r.status_code
                    if code in (403, 429, 451):
                        return True
                    if code in (404, 410):
                        return False
                    if 200 <= code < 300:
                        ctype = r.headers.get('Content-Type', '').lower()
                        path = url.lower().split('?')[0]
                        if '.m3u' in path or 'mpegurl' in ctype:
                            chunk = next(r.iter_content(2048), b'')
                            return chunk.lstrip(b'\xef\xbb\xbf \r\n\t').startswith(b'#EXTM3U')
                        return 'text/html' not in ctype
                    if code >= 500 or code == 408:
                        continue
                    return False
            except requests.RequestException:
                continue
        return False

    def check_link_active(self, url, timeout=8):
        """Return (is_active, url), cached per URL."""
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        with self.lock:
            if url in self.url_status_cache:
                return self.url_status_cache[url]
        result = (self._probe_link(url, headers, timeout), url)
        logging.info(f"Checked {url}: {'Active' if result[0] else 'Dead'}")
        with self.lock:
            self.url_status_cache[url] = result
        return result

    def is_excluded(self, url, group):
        """True for credentialed panel streams and adult groups."""
        return bool(PANEL_RESTREAM.match(url) or CREDENTIAL_QUERY.search(url) or ADULT_GROUP.search(group or ''))

    def parse_and_store(self, lines, source_url):
        """Parse M3U lines and store channels."""
        current_channel = {}
        channel_count = 0
        for line in lines:
            line = line.strip()
            if line.startswith('#EXTINF:'):
                match = re.search(r'tvg-logo="([^"]*)"', line)
                logo = match.group(1) if match and match.group(1) else self.default_logo
                
                match = re.search(r'group-title="([^"]*)"', line)
                group = match.group(1) if match else "Uncategorized"
                
                match = re.search(r',(.+)$', line)
                name = match.group(1).strip() if match else "Unnamed Channel"
                
                current_channel = {
                    'name': name,
                    'logo': logo,
                    'group': group,
                    'source': source_url
                }
            elif line.startswith('http') and current_channel:
                if self.is_excluded(line, current_channel.get('group', '')):
                    current_channel = {}
                    continue
                with self.lock:
                    if line not in self.seen_urls:
                        self.seen_urls.add(line)
                        current_channel['url'] = line
                        self.channels[current_channel['group']].append(current_channel)
                        channel_count += 1
                current_channel = {}
        logging.info(f"Parsed {channel_count} channels from {source_url}")

    def filter_active_channels(self):
        """Filter out inactive channels, skippable for speed."""
        if not self.check_links:
            logging.info("Skipping link activity check for speed")
            return
        
        active_channels = defaultdict(list)
        all_channels = [(group, ch) for group, chans in self.channels.items() for ch in chans]
        url_set = set()
        
        logging.info(f"Total channels to check: {len(all_channels)}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            future_to_channel = {
                executor.submit(self.check_link_active, ch['url']): (group, ch)
                for group, ch in all_channels if ch['url'] not in url_set and not url_set.add(ch['url'])
            }
            for future in concurrent.futures.as_completed(future_to_channel):
                group, channel = future_to_channel[future]
                try:
                    is_active, updated_url = future.result()
                    if is_active:
                        channel['url'] = updated_url
                        active_channels[group].append(channel)
                except Exception as e:
                    logging.error(f"Error checking {channel['url']}: {e}")

        self.channels = active_channels
        logging.info(f"Active channels after filtering: {sum(len(ch) for ch in active_channels.values())}")

    def process_sources(self, source_urls):
        """Process sources sequentially for better control."""
        self.channels.clear()
        self.seen_urls.clear()
        self.url_status_cache.clear()
        
        all_m3u_urls = set()
        for url in source_urls:
            html_content, lines = self.fetch_content(url)
            if url.endswith('.html'):
                m3u_urls = self.extract_stream_urls_from_html(html_content, url)
                all_m3u_urls.update(m3u_urls)
            else:
                self.parse_and_store(lines, url)
        
        for m3u_url in all_m3u_urls:
            _, lines = self.fetch_content(m3u_url)
            self.parse_and_store(lines, m3u_url)
        
        if self.channels:
            self.filter_active_channels()
        else:
            logging.warning("No channels parsed from sources")

    def export_m3u(self, filename="LiveTV.m3u"):
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('#EXTM3U\n')
            for group, channels in self.channels.items():
                for channel in channels:
                    f.write(f'#EXTINF:-1 tvg-logo="{channel["logo"]}" group-title="{group}",{channel["name"]}\n')
                    f.write(f'{channel["url"]}\n')
        logging.info(f"Exported M3U to {filepath}")
        return filepath

    def export_txt(self, filename="LiveTV.txt"):
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            for group, channels in sorted(self.channels.items()):
                f.write(f"Group: {group}\n")
                for channel in channels:
                    f.write(f"Name: {channel['name']}\n")
                    f.write(f"URL: {channel['url']}\n")
                    f.write(f"Logo: {channel['logo']}\n")
                    f.write(f"Source: {channel['source']}\n")
                    f.write("-" * 50 + "\n")
                f.write("\n")
        logging.info(f"Exported TXT to {filepath}")
        return filepath

    def export_json(self, filename="LiveTV.json"):
        filepath = os.path.join(self.output_dir, filename)
        mumbai_tz = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(mumbai_tz).strftime('%Y-%m-%d %H:%M:%S')
        
        json_data = {
            "date": current_time,
            "channels": dict(self.channels)
        }
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        logging.info(f"Exported JSON to {filepath}")
        return filepath

    def export_custom(self, filename="LiveTV"):
        """Export to custom format without extension."""
        filepath = os.path.join(self.output_dir, filename)
        custom_data = []
        
        for group, channels in self.channels.items():
            for channel in channels:
                custom_data.append({
                    "name": channel['name'],
                    "type": group,
                    "url": channel['url'],
                    "img": channel['logo']
                })
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(custom_data, f, ensure_ascii=False, indent=2)
        logging.info(f"Exported custom format to {filepath}")
        return filepath

def main():
    # Specific M3U sources (12 sources)
    source_urls = [   
        "https://raw.githubusercontent.com/HelmerLuzo/PlutoTV_HL/refs/heads/main/tv/m3u/PlutoTV_tv_GB.m3u",
        "https://iptv-org.github.io/iptv/countries/uk.m3u",
    ]

    # Link checking is ON: dead streams (404/410, unreachable, not a manifest) are dropped; geo-blocked (403/451) are kept
    collector = M3UCollector(country="UK", check_links=True)
    collector.process_sources(source_urls)
    if not any(collector.channels.values()):
        logging.error("No active channels collected (sources down or all filtered); keeping existing files unchanged")
        return
    
    # Export files
    collector.export_m3u("LiveTV.m3u")
    collector.export_txt("LiveTV.txt")
    collector.export_json("LiveTV.json")
    collector.export_custom("LiveTV")
    
    total_channels = sum(len(ch) for ch in collector.channels.values())
    mumbai_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    logging.info(f"[{mumbai_time}] Collected {total_channels} unique channel for UK")
    logging.info(f"Groups found: {len(collector.channels)}")

if __name__ == "__main__":
    main()
