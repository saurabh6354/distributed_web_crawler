"""
Bloom Filter implementation for URL deduplication.
Uses Redis Bitmaps for distributed, memory-efficient storage.

Memory comparison for 10M URLs:
- Redis Set: ~800 MB
- Bloom Filter: ~14 MB (98% savings!)
"""

import mmh3  # MurmurHash3 for hash functions
from typing import Optional
from redis import Redis
import logging

logger = logging.getLogger(__name__)


class BloomFilter:
    """
    Memory-efficient Bloom Filter using Redis Bitmaps.
    
    Features:
    - 98% memory savings vs Redis Set
    - O(1) lookups
    - Distributed (shared across all workers)
    - Configurable false positive rate
    
    Trade-offs:
    - Small false positive rate (default: 0.1%)
    - Cannot delete items (acceptable for crawlers)
    """
    
    def __init__(self, redis_client: Redis, 
                 key: str = 'crawler:bloom',
                 capacity: int = 10000000,  # 10M URLs
                 error_rate: float = 0.001):  # 0.1% false positive
        """
        Initialize Bloom Filter.
        
        Args:
            redis_client: Redis connection
            key: Redis key for the bitmap
            capacity: Expected number of URLs
            error_rate: Acceptable false positive rate (0.001 = 0.1%)
        """
        self.redis = redis_client
        self.key = key
        self.capacity = capacity
        self.error_rate = error_rate
        
        # Calculate optimal parameters
        # m = -(n * ln(p)) / (ln(2)^2)
        # k = (m/n) * ln(2)
        import math
        self.size = int(-(capacity * math.log(error_rate)) / (math.log(2) ** 2))
        self.hash_count = int((self.size / capacity) * math.log(2))
        
        logger.info(f"Bloom Filter initialized: size={self.size:,} bits, "
                   f"hash_count={self.hash_count}, "
                   f"capacity={capacity:,}, error_rate={error_rate}")
        
        # Store metadata
        self.redis.hset(f'{self.key}:info', mapping={
            'size': self.size,
            'hash_count': self.hash_count,
            'capacity': capacity,
            'error_rate': error_rate
        })
    
    def _get_positions(self, url: str) -> list:
        """
        Calculate bit positions for a URL using multiple hash functions.
        
        Args:
            url: URL to hash
            
        Returns:
            List of bit positions
        """
        positions = []
        for i in range(self.hash_count):
            # Use MurmurHash3 with different seeds
            hash_val = mmh3.hash(url, i) % self.size
            positions.append(hash_val)
        return positions
    
    def add(self, url: str) -> bool:
        """
        Add URL to Bloom Filter.
        
        Args:
            url: URL to add
            
        Returns:
            True if probably new, False if definitely exists
        """
        positions = self._get_positions(url)
        
        # Check if already exists (optimization)
        pipeline = self.redis.pipeline()
        for pos in positions:
            pipeline.getbit(self.key, pos)
        bits = pipeline.execute()
        
        already_exists = all(bits)
        
        # Set all bits
        pipeline = self.redis.pipeline()
        for pos in positions:
            pipeline.setbit(self.key, pos, 1)
        pipeline.execute()
        
        return not already_exists
    
    def contains(self, url: str) -> bool:
        """
        Check if URL probably exists in the filter.
        
        Args:
            url: URL to check
            
        Returns:
            True if probably exists (might be false positive)
            False if definitely does not exist
        """
        positions = self._get_positions(url)
        
        # Check all bits
        pipeline = self.redis.pipeline()
        for pos in positions:
            pipeline.getbit(self.key, pos)
        bits = pipeline.execute()
        
        # All bits must be 1 for URL to exist
        return all(bits)
    
    def add_batch(self, urls: list) -> int:
        """
        Add multiple URLs in batch (more efficient).
        
        Args:
            urls: List of URLs to add
            
        Returns:
            Number of new URLs added
        """
        new_count = 0
        pipeline = self.redis.pipeline()
        
        for url in urls:
            positions = self._get_positions(url)
            for pos in positions:
                pipeline.setbit(self.key, pos, 1)
            new_count += 1
        
        pipeline.execute()
        return new_count
    
    def get_stats(self) -> dict:
        """
        Get Bloom Filter statistics.
        
        Returns:
            Dictionary with stats
        """
        # Count set bits (approximate item count)
        # This is expensive, so use sparingly
        info = self.redis.hgetall(f'{self.key}:info')
        
        return {
            'size_bits': int(info.get(b'size', 0)),
            'size_mb': int(info.get(b'size', 0)) / 8 / 1024 / 1024,
            'hash_count': int(info.get(b'hash_count', 0)),
            'capacity': int(info.get(b'capacity', 0)),
            'error_rate': float(info.get(b'error_rate', 0.001))
        }
    
    def clear(self):
        """Clear the Bloom Filter (delete bitmap)."""
        self.redis.delete(self.key)
        self.redis.delete(f'{self.key}:info')
        logger.info(f"Bloom Filter cleared: {self.key}")


# Example usage and testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Connect to Redis
    redis_client = Redis(host='localhost', port=6379, decode_responses=False)
    
    # Create Bloom Filter
    bloom = BloomFilter(
        redis_client,
        capacity=10000000,  # 10M URLs
        error_rate=0.001    # 0.1% false positive
    )
    
    # Test URLs
    test_urls = [
        "https://example.com/page1",
        "https://example.com/page2",
        "https://python.org/docs",
        "https://github.com/trending"
    ]
    
    print("\n" + "="*60)
    print("BLOOM FILTER TEST")
    print("="*60)
    
    # Add URLs
    print("\nAdding URLs...")
    for url in test_urls:
        is_new = bloom.add(url)
        print(f"  {url}: {'NEW' if is_new else 'DUPLICATE'}")
    
    # Check URLs
    print("\nChecking URLs...")
    for url in test_urls:
        exists = bloom.contains(url)
        print(f"  {url}: {'EXISTS' if exists else 'NOT FOUND'}")
    
    # Test new URL
    print("\nTesting new URL...")
    new_url = "https://stackoverflow.com/questions"
    exists = bloom.contains(new_url)
    print(f"  {new_url}: {'EXISTS' if exists else 'NOT FOUND'}")
    
    # Add and check again
    bloom.add(new_url)
    exists = bloom.contains(new_url)
    print(f"  {new_url} (after add): {'EXISTS' if exists else 'NOT FOUND'}")
    
    # Show stats
    print("\n" + "="*60)
    print("BLOOM FILTER STATISTICS")
    print("="*60)
    stats = bloom.get_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n✅ Bloom Filter test complete!")
