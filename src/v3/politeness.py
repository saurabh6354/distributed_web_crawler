"""
Distributed Politeness Manager using Redis locks.
Implements the "Bathroom Key" rule for per-domain crawl delays.

Each domain gets a lock that expires after crawl_delay seconds.
Workers self-regulate without needing a central coordinator.
"""

from redis import Redis
from urllib.parse import urlparse
from typing import Optional
import logging
import time

logger = logging.getLogger(__name__)


class PolitenessManager:
    """
    Distributed per-domain politeness enforcement using Redis locks.
    
    Features:
    - Self-regulating workers (no Master needed)
    - Automatic crawl-delay enforcement
    - Fair domain access across workers
    - Lock auto-expiry (fault tolerant)
    
    How it works:
    1. Worker wants to crawl example.com/page1
    2. Worker tries: SET lock:example.com "1" NX EX 1
    3. If successful → crawl
    4. If failed → re-queue with lower priority (snooze)
    """
    
    def __init__(self, redis_client: Redis, default_delay: float = 0.1):
        """
        Initialize Politeness Manager.
        
        Args:
            redis_client: Redis connection
            default_delay: Default crawl delay in seconds (if not specified)
        """
        self.redis = redis_client
        self.default_delay = default_delay
        self.stats = {
            'locks_acquired': 0,
            'locks_failed': 0,
            're_queued': 0
        }
        
        logger.info(f"PolitenessManager initialized (default_delay={default_delay}s)")
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            parsed = urlparse(url)
            return f"{parsed.scheme}://{parsed.netloc}"
        except:
            return "unknown"
    
    def _get_lock_key(self, domain: str) -> str:
        """Get Redis key for domain lock."""
        return f"lock:{domain}"
    
    def can_crawl(self, url: str, crawl_delay: Optional[float] = None) -> bool:
        """
        Check if URL's domain is available to crawl (acquire lock).
        
        Args:
            url: URL to check
            crawl_delay: Crawl delay in seconds (None = use default)
            
        Returns:
            True if lock acquired (can crawl)
            False if domain is locked (must wait/re-queue)
        """
        domain = self._extract_domain(url)
        lock_key = self._get_lock_key(domain)
        delay = crawl_delay or self.default_delay
        
        # Try to acquire lock with auto-expiry
        # SET key value NX EX seconds
        acquired = self.redis.set(
            lock_key,
            '1',
            nx=True,  # Only set if key does not exist
            ex=int(delay)  # Auto-expire after delay seconds
        )
        
        if acquired:
            self.stats['locks_acquired'] += 1
            logger.debug(f"Lock acquired for {domain} (delay={delay}s)")
            return True
        else:
            self.stats['locks_failed'] += 1
            logger.debug(f"Lock failed for {domain} (domain busy)")
            return False
    
    def get_lock_ttl(self, url: str) -> Optional[int]:
        """
        Get remaining TTL for domain lock.
        
        Args:
            url: URL to check
            
        Returns:
            Remaining seconds until lock expires, or None if not locked
        """
        domain = self._extract_domain(url)
        lock_key = self._get_lock_key(domain)
        ttl = self.redis.ttl(lock_key)
        
        if ttl > 0:
            return ttl
        return None
    
    def force_release_lock(self, url: str):
        """
        Force release lock for a domain (use with caution).
        
        Args:
            url: URL whose domain lock to release
        """
        domain = self._extract_domain(url)
        lock_key = self._get_lock_key(domain)
        self.redis.delete(lock_key)
        logger.warning(f"Force released lock for {domain}")
    
    def get_crawl_delay(self, url: str) -> float:
        """
        Get crawl delay for domain (from robots.txt cache or default).
        
        Args:
            url: URL to check
            
        Returns:
            Crawl delay in seconds
        """
        domain = self._extract_domain(url)
        
        # Try to get from robots.txt cache
        delay_key = f"crawler:robots:delay:{domain}"
        cached_delay = self.redis.get(delay_key)
        
        if cached_delay:
            try:
                return float(cached_delay)
            except:
                pass
        
        # Try to get from domain state
        state_key = f"crawler:domain_state:{domain}"
        state = self.redis.hgetall(state_key)
        if state and b'crawl_delay' in state:
            try:
                return float(state[b'crawl_delay'])
            except:
                pass
        
        # Use default
        return self.default_delay
    
    def set_crawl_delay(self, url: str, delay: float):
        """
        Set crawl delay for a domain.
        
        Args:
            url: URL of domain
            delay: Crawl delay in seconds
        """
        domain = self._extract_domain(url)
        
        # Store in domain state
        state_key = f"crawler:domain_state:{domain}"
        self.redis.hset(state_key, 'crawl_delay', delay)
        
        # Also store in robots cache format
        delay_key = f"crawler:robots:delay:{domain}"
        self.redis.setex(delay_key, 86400, delay)  # Cache for 24 hours
        
        logger.info(f"Set crawl delay for {domain}: {delay}s")
    
    def get_stats(self) -> dict:
        """Get politeness manager statistics."""
        total_attempts = self.stats['locks_acquired'] + self.stats['locks_failed']
        success_rate = (self.stats['locks_acquired'] / total_attempts * 100) if total_attempts > 0 else 0
        
        return {
            **self.stats,
            'total_attempts': total_attempts,
            'success_rate': f"{success_rate:.1f}%"
        }
    
    def clear_all_locks(self):
        """
        Clear all domain locks (emergency use only).
        Use for system shutdown or reset.
        """
        pattern = "lock:*"
        keys = self.redis.keys(pattern)
        if keys:
            self.redis.delete(*keys)
            logger.warning(f"Cleared {len(keys)} domain locks")


class ReQueueManager:
    """
    Manager for re-queuing URLs that failed politeness check.
    Implements the "snooze" functionality.
    """
    
    def __init__(self, redis_client: Redis, priority_penalty: float = 5.0):
        """
        Initialize Re-Queue Manager.
        
        Args:
            redis_client: Redis connection
            priority_penalty: How much to decrease priority when re-queuing
        """
        self.redis = redis_client
        self.priority_penalty = priority_penalty
        self.stats = {'re_queued': 0}
    
    def re_queue_url(self, url_data: dict, current_priority: float):
        """
        Re-queue URL with lower priority (snooze).
        
        Args:
            url_data: URL data dictionary
            current_priority: Current priority score
        """
        import json
        
        # Calculate new priority (lower = less important)
        new_priority = max(current_priority - self.priority_penalty, 1.0)
        
        # Push back to frontier
        url_json = json.dumps(url_data)
        self.redis.zadd('crawler:frontier', {url_json: new_priority})
        
        self.stats['re_queued'] += 1
        logger.debug(f"Re-queued URL: {url_data.get('url')} "
                    f"(priority: {current_priority:.1f} → {new_priority:.1f})")


# Example usage and testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Connect to Redis
    redis_client = Redis(host='localhost', port=6379, decode_responses=False)
    
    # Create managers
    politeness = PolitenessManager(redis_client, default_delay=2.0)
    requeue = ReQueueManager(redis_client, priority_penalty=5.0)
    
    print("\n" + "="*60)
    print("POLITENESS MANAGER TEST")
    print("="*60)
    
    # Test URLs from same domain
    url1 = "https://example.com/page1"
    url2 = "https://example.com/page2"
    url3 = "https://python.org/docs"
    
    # Test 1: Acquire lock for first URL
    print("\n[Test 1] Acquiring lock for example.com...")
    can_crawl = politeness.can_crawl(url1, crawl_delay=2.0)
    print(f"  {url1}: {'✅ CAN CRAWL' if can_crawl else '❌ LOCKED'}")
    
    # Test 2: Try to acquire lock for same domain (should fail)
    print("\n[Test 2] Trying same domain immediately...")
    can_crawl = politeness.can_crawl(url2, crawl_delay=2.0)
    print(f"  {url2}: {'✅ CAN CRAWL' if can_crawl else '❌ LOCKED'}")
    
    if not can_crawl:
        print("  → Domain is locked, would re-queue with lower priority")
        requeue.re_queue_url({'url': url2}, current_priority=80.0)
    
    # Test 3: Different domain (should succeed)
    print("\n[Test 3] Trying different domain...")
    can_crawl = politeness.can_crawl(url3, crawl_delay=1.0)
    print(f"  {url3}: {'✅ CAN CRAWL' if can_crawl else '❌ LOCKED'}")
    
    # Test 4: Check TTL
    print("\n[Test 4] Checking lock TTL...")
    ttl = politeness.get_lock_ttl(url1)
    print(f"  Lock expires in: {ttl} seconds" if ttl else "  No lock")
    
    # Test 5: Wait for lock to expire
    print("\n[Test 5] Waiting for lock to expire...")
    print("  Waiting 2 seconds...")
    time.sleep(2)
    
    can_crawl = politeness.can_crawl(url2, crawl_delay=2.0)
    print(f"  {url2}: {'✅ CAN CRAWL' if can_crawl else '❌ LOCKED'}")
    
    # Show stats
    print("\n" + "="*60)
    print("STATISTICS")
    print("="*60)
    
    pol_stats = politeness.get_stats()
    print("\nPoliteness Manager:")
    for key, value in pol_stats.items():
        print(f"  {key}: {value}")
    
    print("\nRe-Queue Manager:")
    for key, value in requeue.stats.items():
        print(f"  {key}: {value}")
    
    print("\n✅ Politeness test complete!")
