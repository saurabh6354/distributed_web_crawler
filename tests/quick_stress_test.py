#!/usr/bin/env python3
"""
QUICK 5-Minute Stress Test - For Rapid Performance Analysis

Tests (total ~5 minutes):
1. Worker Scaling: 1, 5, 10 workers (1 min each)
2. Memory Test: Quick snapshot (30 sec)
3. Throughput: 2 minutes sustained
"""

import sys
import os
import time
import json
import subprocess
import psutil
import signal
from datetime import datetime
from typing import Dict, List

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'v3'))

from redis import Redis
from pymongo import MongoClient

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')


class QuickStressTester:
    """5-minute stress test for quick performance analysis."""
    
    def __init__(self):
        self.redis = Redis(host='localhost', port=6379, decode_responses=False)
        self.mongo = MongoClient('mongodb://localhost:27017/')
        self.db = self.mongo['web_crawler']
        
        self.results = {
            'test_date': datetime.now().isoformat(),
            'system_info': self._get_system_info(),
            'tests': []
        }
        
        self.results_dir = os.path.join(PROJECT_ROOT, 'results')
        os.makedirs(self.results_dir, exist_ok=True)
        os.makedirs(os.path.join(self.results_dir, 'graphs'), exist_ok=True)
    
    def _get_system_info(self) -> Dict:
        """Get system info."""
        return {
            'cpu_count': psutil.cpu_count(),
            'memory_total_gb': psutil.virtual_memory().total / (1024**3),
            'platform': sys.platform
        }
    
    def _clean(self):
        """Quick clean."""
        print("🧹 Cleaning...")
        subprocess.run(['pkill', '-9', '-f', 'worker_v3.py'], stderr=subprocess.DEVNULL)
        time.sleep(1)
        
        self.redis.delete('crawler:frontier')
        self.redis.delete('crawler:bloom')
        for key in self.redis.scan_iter('lock:*'):
            self.redis.delete(key)
        
        self.db.pages_metadata.delete_many({})
        self.db.pages_content.delete_many({})
        print("✅ Cleaned")
    
    def _seed(self, count: int = 50):
        """Seed URLs."""
        print(f"🌱 Seeding {count} URLs...")
        
        # Use real seed URLs from seed_urls.txt
        seed_file = os.path.join(PROJECT_ROOT, 'seed_urls.txt')
        
        if os.path.exists(seed_file):
            with open(seed_file, 'r') as f:
                urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            
            # Repeat URLs if needed to reach count
            while len(urls) < count:
                urls.extend(urls[:count - len(urls)])
            urls = urls[:count]
        else:
            # Fallback to test URLs
            domains = ['python.org', 'github.com', 'stackoverflow.com', 'reddit.com']
            urls = [f"https://{domains[i % len(domains)]}" for i in range(count)]
        
        cmd = ['python3', os.path.join(PROJECT_ROOT, 'src/v3/master_v3.py'), 'seed'] + urls
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"⚠️  Seeding stderr: {result.stderr}")
        
        # Verify seeding
        frontier_size = self.redis.zcard('crawler:frontier')
        print(f"✅ Seeded {frontier_size} URLs to frontier")
    
    def _start_workers(self, num: int, pages: int = 50) -> List[subprocess.Popen]:
        """Start workers."""
        print(f"🚀 Starting {num} workers...")
        
        workers = []
        log_dir = os.path.join(PROJECT_ROOT, 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        for i in range(1, num + 1):
            worker_id = f"quick-test-{i}"
            log_file = os.path.join(log_dir, f'{worker_id}.log')
            
            cmd = [
                'python3', os.path.join(PROJECT_ROOT, 'src/v3/worker_v3.py'),
                '--worker-id', worker_id,
                '--max-pages', str(pages),
                '--idle-timeout', '30',
                '--batch-size', '5'
            ]
            
            with open(log_file, 'w') as log:
                proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT,
                                       preexec_fn=os.setsid)
                workers.append(proc)
            time.sleep(0.05)
        
        print(f"✅ Started {num} workers")
        return workers
    
    def _stop_workers(self, workers: List[subprocess.Popen]):
        """Stop workers."""
        for proc in workers:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except:
                pass
        time.sleep(2)
        for proc in workers:
            try:
                if proc.poll() is None:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except:
                pass
    
    def _collect_metrics(self, duration: int) -> Dict:
        """Collect metrics."""
        metrics = {
            'timestamps': [],
            'pages_crawled': [],
            'cpu_percent': [],
            'memory_mb': []
        }
        
        start = time.time()
        interval = 2  # Sample every 2 seconds
        
        while time.time() - start < duration:
            metrics['timestamps'].append(time.time() - start)
            
            try:
                pages = self.db.pages_metadata.count_documents({})
                metrics['pages_crawled'].append(pages)
            except:
                metrics['pages_crawled'].append(0)
            
            metrics['cpu_percent'].append(psutil.cpu_percent(interval=0.5))
            metrics['memory_mb'].append(psutil.virtual_memory().used / (1024**2))
            
            time.sleep(interval)
        
        return metrics
    
    def test_worker_scaling(self) -> Dict:
        """Test worker scaling (more data points for better graphs)."""
        print("\n" + "="*60)
        print("TEST 1: WORKER SCALING")
        print("="*60)
        
        worker_counts = [1, 2, 3, 5, 7, 10, 15, 20]  # 8 data points
        results = []
        
        for num_workers in worker_counts:
            print(f"\n--- {num_workers} workers (30 sec) ---")  # Shorter duration per test
            
            self._clean()
            self._seed(count=150)  # Plenty of URLs for all workers
            
            start = time.time()
            workers = self._start_workers(num_workers, pages=50)
            
            # Give workers time to start
            time.sleep(3)
            
            metrics = self._collect_metrics(duration=30)  # 30 seconds instead of 60
            
            # Give workers time to finish
            for proc in workers:
                try:
                    proc.wait(timeout=5)
                except:
                    pass
            
            elapsed = time.time() - start
            pages = self.db.pages_metadata.count_documents({})
            throughput = pages / elapsed if elapsed > 0 else 0
            
            result = {
                'num_workers': num_workers,
                'pages_crawled': pages,
                'elapsed_seconds': elapsed,
                'throughput_pages_per_sec': throughput,
                'avg_cpu': sum(metrics['cpu_percent']) / len(metrics['cpu_percent']) if metrics['cpu_percent'] else 0,
                'metrics': metrics
            }
            results.append(result)
            
            print(f"  Pages: {pages}, Throughput: {throughput:.2f}/sec")
            
            self._stop_workers(workers)
            time.sleep(2)
        
        return {'test_name': 'Worker Scaling', 'results': results}
    
    def test_memory_efficiency(self) -> Dict:
        """Test memory (30 sec)."""
        print("\n" + "="*60)
        print("TEST 2: MEMORY EFFICIENCY (30 seconds)")
        print("="*60)
        
        from bloom_filter import BloomFilter
        
        url_counts = [10000, 50000]
        results = []
        
        for url_count in url_counts:
            print(f"\n--- Testing {url_count:,} URLs ---")
            
            # Bloom Filter
            self.redis.delete('crawler:bloom')
            bloom_start = self.redis.info('memory')['used_memory']
            
            bloom = BloomFilter(self.redis, capacity=url_count, error_rate=0.001)
            for i in range(url_count):
                bloom.add(f"https://example.com/page{i}")
            
            bloom_end = self.redis.info('memory')['used_memory']
            bloom_mb = (bloom_end - bloom_start) / (1024**2)
            
            # Redis Set
            self.redis.delete('test:set')
            set_start = self.redis.info('memory')['used_memory']
            
            for i in range(url_count):
                self.redis.sadd('test:set', f"https://example.com/page{i}")
            
            set_end = self.redis.info('memory')['used_memory']
            set_mb = (set_end - set_start) / (1024**2)
            
            savings = ((set_mb - bloom_mb) / set_mb * 100) if set_mb > 0 else 0
            
            result = {
                'url_count': url_count,
                'bloom_filter_mb': bloom_mb,
                'redis_set_mb': set_mb,
                'savings_percent': savings
            }
            results.append(result)
            
            print(f"  Bloom: {bloom_mb:.2f}MB, Set: {set_mb:.2f}MB, Savings: {savings:.1f}%")
            
            self.redis.delete('crawler:bloom')
            self.redis.delete('test:set')
        
        return {'test_name': 'Memory Efficiency', 'results': results}
    
    def test_throughput(self) -> Dict:
        """Test sustained throughput (90 sec)."""
        print("\n" + "="*60)
        print("TEST 3: SUSTAINED THROUGHPUT (90 seconds)")
        print("="*60)
        
        self._clean()
        self._seed(count=200)
        
        workers = self._start_workers(10, pages=20)
        
        start = time.time()
        metrics = self._collect_metrics(duration=90)
        elapsed = time.time() - start
        
        pages = self.db.pages_metadata.count_documents({})
        avg_throughput = pages / elapsed if elapsed > 0 else 0
        
        self._stop_workers(workers)
        
        result = {
            'duration_seconds': elapsed,
            'pages_crawled': pages,
            'avg_throughput_pages_per_sec': avg_throughput,
            'metrics': metrics
        }
        
        print(f"\n  Pages: {pages}, Avg Throughput: {avg_throughput:.2f}/sec")
        
        return {'test_name': 'Sustained Throughput', 'results': result}
    
    def run_all(self):
        """Run all tests (~5 minutes)."""
        print("\n" + "="*60)
        print("🧪 QUICK 5-MINUTE STRESS TEST")
        print("="*60)
        print(f"Start: {datetime.now().strftime('%H:%M:%S')}")
        print(f"System: {self.results['system_info']['cpu_count']} CPUs, "
              f"{self.results['system_info']['memory_total_gb']:.1f}GB RAM")
        
        overall_start = time.time()
        
        # Run tests
        test1 = self.test_worker_scaling()  # 3 min
        self.results['tests'].append(test1)
        
        test2 = self.test_memory_efficiency()  # 30 sec
        self.results['tests'].append(test2)
        
        test3 = self.test_throughput()  # 90 sec
        self.results['tests'].append(test3)
        
        overall_elapsed = time.time() - overall_start
        
        # Save
        json_file = os.path.join(self.results_dir, 'test_results.json')
        with open(json_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        print("\n" + "="*60)
        print(f"✅ COMPLETED in {overall_elapsed/60:.1f} minutes")
        print("="*60)
        print(f"\nResults: {json_file}")
        print("\nGenerate graphs:")
        print("  python3 tests/generate_report.py")


def main():
    print("🚀 Quick 5-Minute Stress Test")
    print("⚠️  This will clean Redis & MongoDB!")
    print("    Ctrl+C to cancel, or wait 3 seconds...")
    
    try:
        time.sleep(3)
    except KeyboardInterrupt:
        print("\n❌ Cancelled")
        return
    
    tester = QuickStressTester()
    tester.run_all()


if __name__ == '__main__':
    main()
