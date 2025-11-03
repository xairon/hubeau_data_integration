"""
Parallel CSV fetching for Hub'Eau API with rate limiting.
Optimized for bulk data loading with concurrent requests.
"""

import time
import logging
from typing import Iterator, List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Lock

from hubeau_pipeline.sources.hubeau_csv_source import (
    HubeauAPIClient,
    fetch_csv_page,
    get_total_pages_from_json
)

logger = logging.getLogger(__name__)


class ParallelCSVFetcher:
    """
    Fetches CSV pages in parallel with rate limiting.
    """

    def __init__(
        self,
        client: HubeauAPIClient,
        endpoint: str,
        params: Dict,
        max_workers: int = 5,
        rate_limit: float = 0.3
    ):
        self.client = client
        self.endpoint = endpoint
        self.params = params
        self.max_workers = max_workers
        self.rate_limit = rate_limit
        self.last_request_time = time.time()
        self.request_lock = Lock()

    def _rate_limited_fetch(self, page: int) -> Optional[List[Dict]]:
        """
        Fetch a page with rate limiting.
        """
        # Enforce rate limit
        with self.request_lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.rate_limit:
                time.sleep(self.rate_limit - elapsed)
            self.last_request_time = time.time()

        try:
            # Fetch the page (returns DataFrame)
            df = fetch_csv_page(
                self.client,
                self.endpoint,
                page,
                self.params
            )

            # Convert DataFrame to list of dicts
            if df.empty:
                return None

            return df.to_dict('records')

        except Exception as e:
            logger.error(f"ERROR: Failed to fetch page {page}: {e}")
            return None

    def fetch_pages_parallel(
        self,
        start_page: int = 1,
        end_page: Optional[int] = None
    ) -> Iterator[List[Dict]]:
        """
        Fetch pages in parallel, yielding them as they complete.

        Args:
            start_page: First page to fetch
            end_page: Last page to fetch (None = auto-detect)

        Yields:
            List of records for each page
        """
        # Determine total pages if not specified
        if end_page is None:
            try:
                total_pages, _ = get_total_pages_from_json(
                    self.client,
                    self.endpoint,
                    self.params
                )
                end_page = total_pages
                logger.info(f"INFO: Total pages to fetch: {total_pages}")
            except Exception as e:
                logger.error(f"ERROR: Could not determine total pages: {e}")
                # Fallback to sequential fetching
                page = start_page
                while True:
                    records = self._rate_limited_fetch(page)
                    if not records:
                        break
                    yield records
                    page += 1
                return

        # Use ThreadPoolExecutor for parallel fetching
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit initial batch of tasks
            futures = {}
            page_queue = list(range(start_page, min(start_page + self.max_workers * 2, end_page + 1)))
            next_page = start_page + len(page_queue)

            for page in page_queue:
                future = executor.submit(self._rate_limited_fetch, page)
                futures[future] = page

            # Process completed futures and submit new ones
            while futures:
                # Wait for any future to complete
                for future in as_completed(futures):
                    page = futures.pop(future)

                    try:
                        records = future.result()
                        if records:
                            yield records

                    except Exception as e:
                        logger.error(f"ERROR: Failed to process page {page}: {e}")

                    # Submit next page if available
                    if next_page <= end_page:
                        new_future = executor.submit(self._rate_limited_fetch, next_page)
                        futures[new_future] = next_page
                        next_page += 1


def get_parallel_data_iterator(config_dict: Dict, max_workers: int = 5) -> Iterator[List[Dict]]:
    """
    Get parallel data iterator for Hub'Eau CSV endpoints.

    Args:
        config_dict: Configuration dictionary with resource, extraction, performance settings
        max_workers: Maximum number of parallel workers

    Yields:
        Lists of records (pages) from the API
    """
    resource_config = config_dict['resource']
    extraction_config = config_dict['extraction']
    performance_config = config_dict.get('performance', {})

    # Extract settings
    endpoint = resource_config['endpoint']
    base_url = resource_config['base_url']
    default_params = extraction_config.get('default_params', {})
    rate_limit = performance_config.get('rate_limit', 0.3)

    # Override max_workers if specified in config
    if 'parallelism' in performance_config:
        max_workers = min(max_workers, performance_config['parallelism'])

    logger.info(f"INFO: Parallel fetching - {max_workers} workers, {rate_limit}s rate limit")

    # Initialize client
    client = HubeauAPIClient(base_url=base_url)

    # Prepare parameters
    params = default_params.copy()

    # Create parallel fetcher
    fetcher = ParallelCSVFetcher(
        client=client,
        endpoint=endpoint,
        params=params,
        max_workers=max_workers,
        rate_limit=rate_limit
    )

    # Yield pages as they complete
    yield from fetcher.fetch_pages_parallel()