# import asyncio
# from playwright.async_api import async_playwright
# from urllib.parse import urljoin, urlparse
# import json
# import logging
# import openai

# # Set your OpenAI API Key
# openai.api_key = ""  # Replace with your OpenAI API key


# import asyncio
# from playwright.async_api import async_playwright
# from urllib.parse import urljoin, urlparse
# import json
# import logging
# import openai

# # Set your OpenAI API Key
# openai.api_key = "your_openai_api_key"  # Replace with your OpenAI API key

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


# class ScalableEcommerceCrawler:
#     def __init__(self, domains, max_depth=3, concurrency=5):
#         self.domains = domains
#         self.max_depth = max_depth
#         self.concurrency = concurrency
#         self.visited_urls = set()
#         self.product_urls = {domain: [] for domain in domains}
#         self.collection_urls = {domain: [] for domain in domains}
#         self.page = None  # Placeholder for the page object

#     def sanitize_url(self, url):
#         """Sanitize the URL by removing query parameters and fragments."""
#         parsed_url = urlparse(url)
#         sanitized_url = parsed_url._replace(query="", fragment="").geturl()
#         return sanitized_url

#     def is_valid_url(self, url, domain):
#         """Validate if the URL is within the same domain and is not already visited."""
#         parsed_url = urlparse(url)
#         sanitized_url = self.sanitize_url(url)
#         is_valid = (
#             parsed_url.netloc.endswith(domain)
#             and parsed_url.scheme in {"http", "https"}
#             and sanitized_url not in self.visited_urls
#         )
#         if is_valid:
#             self.visited_urls.add(sanitized_url)
#         return is_valid

#     async def classify_urls_with_openai(self, urls, base_url):
#         """Classify URLs into categories using OpenAI API."""
#         prompt = (
#             "The following are URLs extracted from a webpage. "
#             "Categorize them as 'product', 'collection', 'pagination', or 'other'. "
#             "Output as a JSON list of objects in the format: [{'url': 'category'}]:\n"
#             + "\n".join(urls)
#         )
#         try:
#             response = openai.ChatCompletion.create(
#                 model="gpt-4o-mini",
#                 messages=[
#                     {
#                         "role": "system",
#                         "content": (
#                             "You are an assistant tasked with categorizing URLs. "
#                             "Ensure the output is well-formatted JSON. Do not add explanations."
#                         ),
#                     },
#                     {"role": "user", "content": prompt},
#                 ],
#                 max_tokens=300,
#             )

#             # Get the content of the response
#             raw_response = response["choices"][0]["message"]["content"].strip()

#             # Debugging: Log the raw response
#             logging.debug(f"Raw OpenAI Response: {raw_response}")

#             # Attempt to parse the JSON
#             try:
#                 classification = json.loads(raw_response)
#             except json.JSONDecodeError as json_err:
#                 logging.error(
#                     f"JSON parsing error: {json_err}. Response content: {raw_response}"
#                 )
#                 return {}

#             return classification
#         except Exception as e:
#             logging.error(f"Error classifying URLs with OpenAI: {e}")
#             return {}

#     async def extract_urls(self, page, base_url, domain):
#         """Extract and classify URLs from a page."""
#         try:
#             links = await page.eval_on_selector_all(
#                 "a[href]", "elements => elements.map(el => el.href)"
#             )
#             sanitized_links = [
#                 self.sanitize_url(urljoin(base_url, link))
#                 for link in links
#                 if self.is_valid_url(link, domain)
#             ]
#             classifications = await self.classify_urls_with_openai(
#                 sanitized_links, base_url
#             )
#             return classifications
#         except Exception as e:
#             logging.error(f"Error extracting URLs: {e}")
#             return {}

#     async def crawl_page(self, domain, start_url):
#         """Crawl a single page for products and collections."""
#         base_url = f"https://{domain}"
#         try:
#             await self.page.goto(start_url, timeout=30000)
#             await asyncio.sleep(2)
#             url_classifications = await self.extract_urls(self.page, base_url, domain)

#             for url, category in url_classifications.items():
#                 if category == "product":
#                     self.product_urls[domain].append(url)
#                 elif category == "collection":
#                     self.collection_urls[domain].append(url)
#         except Exception as e:
#             logging.error(f"Failed to crawl {start_url}: {e}")

#     async def crawl_domain(self, domain):
#         """Crawl the domain for collections and products."""
#         start_url = f"https://{domain}"
#         await self.crawl_page(domain, start_url)

#         # Crawl discovered collection URLs
#         for collection_url in self.collection_urls[domain]:
#             await self.crawl_page(domain, collection_url)

#     async def start_crawl(self):
#         """Start crawling for all domains."""
#         async with async_playwright() as p:
#             browser = await p.chromium.launch(headless=True)
#             self.page = await browser.new_page()  # Initialize the page object

#             tasks = [self.crawl_domain(domain) for domain in self.domains]
#             await asyncio.gather(*tasks)

#             await self.page.close()  # Close the page object
#             await browser.close()

#     def save_results(self, output_file="output.json"):
#         """Save the results to a JSON file."""
#         for domain in self.product_urls:
#             self.product_urls[domain] = sorted(set(self.product_urls[domain]))
#             self.collection_urls[domain] = sorted(set(self.collection_urls[domain]))

#         results = {
#             "products": self.product_urls,
#             "collections": self.collection_urls,
#         }
#         try:
#             with open(output_file, "w", encoding="utf-8") as f:
#                 json.dump(results, f, indent=4, ensure_ascii=False)
#             logging.info(f"Results saved to {output_file}")
#         except Exception as e:
#             logging.error(f"Error saving results: {e}")


# if __name__ == "__main__":
#     # Add target domains here
#     input_domains = ["offduty.in"]  # Replace with actual domains
#     crawler = ScalableEcommerceCrawler(input_domains)
#     asyncio.run(crawler.start_crawl())
#     crawler.save_results()


# import asyncio
# from playwright.async_api import async_playwright
# from urllib.parse import urljoin, urlparse
# import re
# import json
# import logging

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


# class EcommerceCrawler():
#     def __init__(self, domains, max_depth=3, concurrency=5):
#         self.domains = domains
#         self.max_depth = max_depth
#         self.concurrency = concurrency
#         self.visited_urls = set()
#         self.product_urls = {domain: [] for domain in domains}
#         self.url_patterns = re.compile(r"(/products/|/product/|/item/|/p/|/t/|/dp/)", re.IGNORECASE)
#         self.pagination_pattern = re.compile(r"\?page=\d+", re.IGNORECASE)

#     def sanitize_url(self, url):
#         """Sanitize the URL by removing query parameters and fragments."""
#         parsed_url = urlparse(url)
#         sanitized_url = parsed_url._replace(query="", fragment="").geturl()
#         return sanitized_url

#     def is_valid_url(self, url, domain):
#         """Validate if the URL is within the same domain and is not already visited."""
#         parsed_url = urlparse(url)
#         sanitized_url = self.sanitize_url(url)
#         is_valid = (
#             parsed_url.netloc.endswith(domain)
#             and parsed_url.scheme in {"http", "https"}
#             and sanitized_url not in self.visited_urls
#         )
#         if is_valid:
#             self.visited_urls.add(sanitized_url)
#         return is_valid

#     async def extract_pagination_urls(self, page, base_url):
#         """Extract pagination URLs from the current page."""
#         try:
#             links = await page.eval_on_selector_all(
#                 "a[href]", "elements => elements.map(el => el.href)"
#             )
#             pagination_urls = [
#                 self.sanitize_url(urljoin(base_url, link))
#                 for link in links
#                 if self.pagination_pattern.search(link) and self.is_valid_url(link, urlparse(base_url).netloc)
#             ]
#             return pagination_urls
#         except Exception as e:
#             logging.error(f"Error extracting pagination URLs: {e}")
#             return []

#     async def handle_infinite_scroll(self, page, max_scrolls=20, scroll_pause=2):
#         """Handle infinite scrolling."""
#         last_height = await page.evaluate("() => document.body.scrollHeight")
#         scrolls = 0

#         while scrolls < max_scrolls:
#             scrolls += 1
#             logging.info(f"Scrolling {scrolls}/{max_scrolls}...")
#             await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
#             await asyncio.sleep(scroll_pause)

#             new_height = await page.evaluate("() => document.body.scrollHeight")
#             if new_height == last_height:
#                 logging.info("No more content to load.")
#                 break
#             last_height = new_height

#         try:
#             links = await page.eval_on_selector_all(
#                 "a[href]", "elements => elements.map(el => el.href)"
#             )
#             return links
#         except Exception as e:
#             logging.error(f"Error extracting links after scrolling: {e}")
#             return []

#     async def crawl_collection(self, domain, start_url):
#         """Crawl a collection, including paginated pages."""
#         page_number = 1
#         base_url = f"https://{domain}"

#         while True:
#             paginated_url = f"{start_url}?page={page_number}"
#             logging.info(f"Visiting paginated URL: {paginated_url}")
            
#             try:
#                 await self.page.goto(paginated_url, timeout=30000)
#                 await asyncio.sleep(1)  # Allow time for the page to load
#             except Exception as e:
#                 logging.error(f"Failed to load {paginated_url}: {e}")
#                 break

#             # Extract product links from the current page
#             product_links = await self.handle_infinite_scroll(self.page)
#             product_links = [
#                 self.sanitize_url(urljoin(base_url, link))
#                 for link in product_links
#                 if self.is_valid_url(link, domain) and re.search(self.url_patterns, link)
#             ]

#             # If no product links are found, break the pagination loop
#             if not product_links:
#                 logging.info(f"No products found on page {page_number}. Stopping pagination.")
#                 break

#             for product in product_links:
#                 if product not in self.product_urls[domain]:
#                     self.product_urls[domain].append(product)
#                     logging.info(f"Added product URL: {product}")

#             # Increment page number for the next iteration
#             page_number += 1

#     async def extract_collection_urls(self, domain):
#         """Extract all collection URLs from the main collections page."""
#         base_url = f"https://{domain}/collections"  # Collections overview page
#         try:
#             await self.page.goto(base_url, timeout=30000)
#             await asyncio.sleep(1)  # Allow time for the page to load

#             links = await self.page.eval_on_selector_all(
#                 "a[href]", "elements => elements.map(el => el.href)"
#             )
#             collection_urls = [
#                 self.sanitize_url(urljoin(base_url, link))
#                 for link in links
#                 if self.is_valid_url(link, domain) and "/collections/" in link
#             ]
#             logging.info(f"Found {len(collection_urls)} collection URLs.")
#             return list(set(collection_urls))  # Remove duplicates
#         except Exception as e:
#             logging.error(f"Error extracting collection URLs from {base_url}: {e}")
#             return []

#     async def crawl_domain(self, domain):
#         """Crawl the domain for collections and their paginated product links."""
#         collection_urls = await self.extract_collection_urls(domain)
#         logging.info(f"Crawling {len(collection_urls)} collections for domain {domain}.")

#         for collection_url in collection_urls:
#             logging.info(f"Starting crawl for collection: {collection_url}")
#             await self.crawl_collection(domain, collection_url)

   
    

#     async def start_crawl(self):
#         """Start crawling for all domains."""
#         async with async_playwright() as p:
#             browser = await p.chromium.launch(headless=True, args=["--disable-http2"])
#             self.page = await browser.new_page()

#             tasks = [self.crawl_domain(domain) for domain in self.domains]
#             await asyncio.gather(*tasks)
#             await browser.close()

#     def save_results(self, output_file="outputee.json"):
#         """Save the results to a JSON file."""
#         for domain in self.product_urls:
#             self.product_urls[domain] = sorted(set(self.product_urls[domain]))

#         try:
#             with open(output_file, "w", encoding="utf-8") as f:
#                 json.dump(self.product_urls, f, indent=4, ensure_ascii=False)
#             logging.info(f"Results saved to {output_file}")
#         except Exception as e:
#             logging.error(f"Error saving results to {output_file}: {e}")


# if __name__ == "__main__":
#     input_domains = ["bluorng.com", "offduty.in", "lashkaraa.in"]  
#     crawler = EcommerceCrawler(input_domains, max_depth=3, concurrency=5)
#     asyncio.run(crawler.start_crawl())
#     crawler.save_results()







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
        self.url_patterns = re.compile(r"(/products/|/product/|/item/|/p/|/t/|/dp/)", re.IGNORECASE)

    def sanitize_url(self, url):
        """Sanitize the URL by removing query parameters and fragments."""
        parsed_url = urlparse(url)
        return parsed_url._replace(query="", fragment="").geturl()

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

    async def safe_goto(self, page, url, retries=3, delay=2):
        """Retry navigation with exponential backoff."""
        for attempt in range(retries):
            try:
                await page.goto(url, timeout=30000)
                return True
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                await asyncio.sleep(delay * (2 ** attempt))
        logging.error(f"Failed to load {url} after {retries} attempts.")
        return False

    async def handle_infinite_scroll(self, page, max_scrolls=20, scroll_pause=2):
        """Handle infinite scrolling."""
        last_height = await page.evaluate("() => document.body.scrollHeight")
        for _ in range(max_scrolls):
            await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(scroll_pause)
            new_height = await page.evaluate("() => document.body.scrollHeight")
            if new_height == last_height:
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

    async def bfs_crawl_domain(self, domain):
        """Crawl the domain using BFS for collections and their paginated product links."""
        queue = asyncio.Queue()
        base_url = f"https://{domain}/collections"
        await queue.put((base_url, 0))  # Add initial URL with depth 0

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--disable-http2"])
            page = await browser.new_page()
            await page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
            })

            try:
                while not queue.empty():
                    current_url, depth = await queue.get()

                    if depth > self.max_depth:
                        continue

                    logging.info(f"BFS visiting: {current_url} at depth {depth}")

                    # Navigate to the URL
                    if not await self.safe_goto(page, current_url):
                        continue

                    # Extract links from the current page
                    links = await self.handle_infinite_scroll(page)
                    links = [
                        self.sanitize_url(urljoin(base_url, link))
                        for link in links
                        if self.is_valid_url(link, domain)
                    ]

                    # Process product links
                    for link in links:
                        if re.search(self.url_patterns, link) and link not in self.product_urls[domain]:
                            self.product_urls[domain].append(link)
                            logging.info(f"Added product URL: {link}")

                    # Add new links to the queue with incremented depth
                    for link in links:
                        if self.is_valid_url(link, domain):
                            await queue.put((link, depth + 1))
            finally:
                await page.close()
                await browser.close()

    async def start_crawl(self):
        """Start BFS crawling for all domains."""
        tasks = [self.bfs_crawl_domain(domain) for domain in self.domains]
        await asyncio.gather(*tasks)

    def save_results(self, output_file="output.json"):
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
    input_domains = ["bluorng.com", "offduty.in", "lashkaraa.in"]
    crawler = EcommerceCrawler(input_domains, max_depth=3, concurrency=5)
    asyncio.run(crawler.start_crawl())
    crawler.save_results()
