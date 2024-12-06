import asyncio
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse
import re
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


class EcommerceCrawler:
    def __init__(self, domains, max_depth=3, concurrency=5):
        self.domains = domains
        self.max_depth = max_depth
        self.concurrency = concurrency
        self.visited_urls = set()
        self.product_urls = {domain: [] for domain in domains}
        self.url_patterns = re.compile(r"(/products/|/product/|/item/|/p/|/t/|/dp/|/gp/product/)", re.IGNORECASE)
        self.pagination_pattern = re.compile(r"(\?page=\d+|&page=\d+|/page/\d+)", re.IGNORECASE)
        self.platform_start_points = {
            "amazon": ["/s?k=", "/gp/bestsellers/", "/deals"],
            "default": ["/collections", "/products", "/categories", "/shop"]
        }
        self.pages = {}
        self.queues = {domain: asyncio.Queue() for domain in domains}
        self.current_depth = {}

    def sanitize_url(self, url):
        """Sanitize the URL by removing query parameters and fragments."""
        parsed_url = urlparse(url)
        sanitized_url = parsed_url._replace(query="", fragment="").geturl()
        return sanitized_url

    def is_valid_url(self, url, domain):
        """Validate if the URL is within the same domain and is not already visited."""
        parsed_url = urlparse(url)
        sanitized_url = self.sanitize_url(url)
        is_valid = (
            parsed_url.netloc.endswith(domain)
            and parsed_url.scheme in {"http", "https"}
            and sanitized_url not in self.visited_urls
        )
        if is_valid:
            self.visited_urls.add(sanitized_url)
        return is_valid

    async def extract_pagination_urls(self, page, base_url):
        """Extract pagination URLs from the current page."""
        try:
            links = await page.eval_on_selector_all(
                "a[href]", "elements => elements.map(el => el.href)"
            )
            pagination_urls = [
                self.sanitize_url(urljoin(base_url, link))
                for link in links
                if self.pagination_pattern.search(link) and self.is_valid_url(link, urlparse(base_url).netloc)
            ]
            return pagination_urls
        except Exception as e:
            logging.error(f"Error extracting pagination URLs: {e}")
            return []

    async def handle_infinite_scroll(self, page, max_scrolls=20, scroll_pause=2):
        """Handle infinite scrolling."""
        last_height = await page.evaluate("() => document.body.scrollHeight")
        scrolls = 0

        while scrolls < max_scrolls:
            scrolls += 1
            logging.info(f"Scrolling {scrolls}/{max_scrolls}...")
            await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(scroll_pause)

            new_height = await page.evaluate("() => document.body.scrollHeight")
            if new_height == last_height:
                logging.info("No more content to load.")
                break
            last_height = new_height

        try:
            links = await page.eval_on_selector_all(
                "a[href]", "elements => elements.map(el => el.href)"
            )
            return links
        except Exception as e:
            logging.error(f"Error extracting links after scrolling: {e}")
            return []

    async def crawl_collection(self, domain, start_url):
        """Crawl a collection, including paginated pages."""
        page_number = 1
        base_url = f"https://{domain}"

        while True:
            paginated_url = f"{start_url}?page={page_number}"
            logging.info(f"Visiting paginated URL: {paginated_url}")
            
            try:
                await self.page.goto(paginated_url, timeout=30000)
                await asyncio.sleep(1)  # Allow time for the page to load
            except Exception as e:
                logging.error(f"Failed to load {paginated_url}: {e}")
                break

            # Extract product links from the current page
            product_links = await self.handle_infinite_scroll(self.page)
            product_links = [
                self.sanitize_url(urljoin(base_url, link))
                for link in product_links
                if self.is_valid_url(link, domain) and re.search(self.url_patterns, link)
            ]

            # If no product links are found, break the pagination loop
            if not product_links:
                logging.info(f"No products found on page {page_number}. Stopping pagination.")
                break

            for product in product_links:
                if product not in self.product_urls[domain]:
                    self.product_urls[domain].append(product)
                    logging.info(f"Added product URL: {product}")

            # Increment page number for the next iteration
            page_number += 1

    async def extract_collection_urls(self, domain):
        """Extract all collection URLs from multiple starting points."""
        start_paths = []
        
        # Determine which starting points to use
        if any(platform in domain for platform in ["amazon.com", "amazon.in"]):
            start_paths = self.platform_start_points["amazon"]
        else:
            start_paths = self.platform_start_points["default"]
        
        collection_urls = set()
        base_url = f"https://{domain}"
        
        for path in start_paths:
            try:
                url = urljoin(base_url, path)
                logging.info(f"Trying URL: {url}")
                await self.page.goto(url, timeout=30000, wait_until="domcontentloaded")
                await asyncio.sleep(2)  # Increased wait time

                links = await self.page.eval_on_selector_all(
                    "a[href]", "elements => elements.map(el => el.href)"
                )
                
                # Filter and add valid URLs
                for link in links:
                    if self.is_valid_url(link, domain):
                        if any(platform in domain for platform in ["amazon.com", "amazon.in"]):
                            if "/dp/" in link or "/gp/product/" in link:
                                collection_urls.add(self.sanitize_url(link))
                        else:
                            if "/collections/" in link or "/products/" in link or "/categories/" in link:
                                collection_urls.add(self.sanitize_url(link))
                
            except Exception as e:
                logging.error(f"Error accessing {url}: {e}")
                continue
        
        logging.info(f"Found {len(collection_urls)} collection URLs for {domain}")
        return list(collection_urls)

    async def bfs_crawl(self, domain):
        """Perform breadth-first crawl of the domain."""
        base_url = f"https://{domain}"
        
        # Initialize with starting points
        start_paths = (self.platform_start_points["amazon"] 
                      if any(platform in domain for platform in ["amazon.com", "amazon.in"]) 
                      else self.platform_start_points["default"])
        
        # Add starting URLs to queue with depth 0
        for path in start_paths:
            url = urljoin(base_url, path)
            await self.queues[domain].put((url, 0))
            self.current_depth[url] = 0

        while not self.queues[domain].empty():
            current_url, depth = await self.queues[domain].get()
            
            if depth >= self.max_depth:
                continue

            try:
                logging.info(f"BFS visiting: {current_url} at depth {depth}")
                await self.pages[domain].goto(current_url, timeout=30000, wait_until="domcontentloaded")
                await asyncio.sleep(2)

                # Extract all links from current page
                links = await self.pages[domain].eval_on_selector_all(
                    "a[href]", "elements => elements.map(el => el.href)"
                )

                # Process discovered links
                for link in links:
                    if not self.is_valid_url(link, domain):
                        continue

                    sanitized_link = self.sanitize_url(link)
                    
                    # Check if it's a product URL
                    if self.url_patterns.search(link):
                        if sanitized_link not in self.product_urls[domain]:
                            self.product_urls[domain].append(sanitized_link)
                            logging.info(f"Added product URL: {sanitized_link}")
                    
                    # Add non-product URLs to queue for further exploration
                    elif sanitized_link not in self.current_depth:
                        self.current_depth[sanitized_link] = depth + 1
                        await self.queues[domain].put((sanitized_link, depth + 1))

            except Exception as e:
                logging.error(f"Error processing {current_url}: {e}")

    async def crawl_domain(self, domain):
        """Replace existing crawl with BFS approach."""
        await self.bfs_crawl(domain)

    async def start_crawl(self):
        """Start crawling for all domains."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--disable-http2"])
            
            # Create separate pages for each domain
            for domain in self.domains:
                self.pages[domain] = await browser.new_page()

            tasks = [self.crawl_domain(domain) for domain in self.domains]
            await asyncio.gather(*tasks)
            
            # Close all pages
            for page in self.pages.values():
                await page.close()
            await browser.close()

    def save_results(self, output_file="outputee.json"):
        """Save the results to a JSON file."""
        for domain in self.product_urls:
            self.product_urls[domain] = sorted(set(self.product_urls[domain]))

        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.product_urls, f, indent=4, ensure_ascii=False)
            logging.info(f"Results saved to {output_file}")
        except Exception as e:
            logging.error(f"Error saving results to {output_file}: {e}")


if __name__ == "__main__":
    input_domains = ["offduty.in", "bluorng.com", "lashkaraa.in",]  

    crawler = EcommerceCrawler(input_domains, max_depth=3, concurrency=5)
    asyncio.run(crawler.start_crawl())
    crawler.save_results()