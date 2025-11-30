#!/usr/bin/env python3
"""
Quick Graph Generator - Simple, Fast Visualizations
"""

import json
import os
import matplotlib.pyplot as plt
import numpy as np

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
RESULTS_DIR = os.path.join(PROJECT_ROOT, 'results')
GRAPHS_DIR = os.path.join(RESULTS_DIR, 'graphs')


def generate_all_graphs():
    """Generate all graphs quickly."""
    
    # Load results
    with open(os.path.join(RESULTS_DIR, 'test_results.json'), 'r') as f:
        results = json.load(f)
    
    os.makedirs(GRAPHS_DIR, exist_ok=True)
    
    print("\n📊 Generating Graphs...")
    
    # Graph 1: Worker Scaling
    print("  1/3 Worker Scaling...")
    test1 = [t for t in results['tests'] if t['test_name'] == 'Worker Scaling'][0]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
    
    workers = [r['num_workers'] for r in test1['results']]
    throughput = [r['throughput_pages_per_sec'] for r in test1['results']]
    pages = [r['pages_crawled'] for r in test1['results']]
    
    # Throughput
    ax1.plot(workers, throughput, marker='o', linewidth=2, markersize=10, color='#3498db')
    ax1.set_xlabel('Workers', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Throughput (pages/sec)', fontsize=12, fontweight='bold')
    ax1.set_title('Worker Scaling: Throughput', fontsize=13, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    for w, t in zip(workers, throughput):
        ax1.text(w, t, f'{t:.2f}', ha='center', va='bottom')
    
    # Pages
    ax2.bar(workers, pages, color='#e74c3c', alpha=0.7, width=0.6)
    ax2.set_xlabel('Workers', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Pages Crawled', fontsize=12, fontweight='bold')
    ax2.set_title('Worker Scaling: Total Pages', fontsize=13, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    for w, p in zip(workers, pages):
        ax2.text(w, p, str(p), ha='center', va='bottom')
    
    plt.tight_layout()
    plt.savefig(os.path.join(GRAPHS_DIR, 'worker_scaling.png'), dpi=300)
    plt.close()
    
    # Graph 2: Memory Efficiency
    print("  2/3 Memory Efficiency...")
    test2 = [t for t in results['tests'] if t['test_name'] == 'Memory Efficiency'][0]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
    
    urls = [r['url_count'] for r in test2['results']]
    bloom = [r['bloom_filter_mb'] for r in test2['results']]
    redis_set = [r['redis_set_mb'] for r in test2['results']]
    savings = [r['savings_percent'] for r in test2['results']]
    
    # Memory comparison
    x = np.arange(len(urls))
    width = 0.35
    ax1.bar(x - width/2, bloom, width, label='Bloom Filter', color='#2ecc71', alpha=0.8)
    ax1.bar(x + width/2, redis_set, width, label='Redis Set', color='#f39c12', alpha=0.8)
    ax1.set_xlabel('URLs', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Memory (MB)', fontsize=12, fontweight='bold')
    ax1.set_title('Memory Comparison', fontsize=13, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels([f'{u:,}' for u in urls])
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Savings
    ax2.plot(urls, savings, marker='o', linewidth=2, markersize=10, color='#9b59b6')
    ax2.set_xlabel('URLs', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Savings (%)', fontsize=12, fontweight='bold')
    ax2.set_title('Bloom Filter Savings', fontsize=13, fontweight='bold')
    ax2.set_xticks(urls)
    ax2.set_xticklabels([f'{u:,}' for u in urls])
    ax2.grid(True, alpha=0.3)
    for u, s in zip(urls, savings):
        ax2.text(u, s, f'{s:.1f}%', ha='center', va='bottom')
    
    plt.tight_layout()
    plt.savefig(os.path.join(GRAPHS_DIR, 'memory_efficiency.png'), dpi=300)
    plt.close()
    
    # Graph 3: Throughput Over Time
    print("  3/3 Throughput Over Time...")
    test3 = [t for t in results['tests'] if t['test_name'] == 'Sustained Throughput'][0]
    metrics = test3['results']['metrics']
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
    
    times = [t/60 for t in metrics['timestamps']]
    
    # Pages over time
    ax1.plot(times, metrics['pages_crawled'], linewidth=2, color='#3498db')
    ax1.fill_between(times, metrics['pages_crawled'], alpha=0.3, color='#3498db')
    ax1.set_xlabel('Time (minutes)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Pages Crawled', fontsize=12, fontweight='bold')
    ax1.set_title('Crawl Progress', fontsize=13, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    
    # CPU usage
    ax2.plot(times, metrics['cpu_percent'], linewidth=2, color='#e74c3c')
    ax2.fill_between(times, metrics['cpu_percent'], alpha=0.3, color='#e74c3c')
    ax2.set_xlabel('Time (minutes)', fontsize=12, fontweight='bold')
    ax2.set_ylabel('CPU Usage (%)', fontsize=12, fontweight='bold')
    ax2.set_title('CPU Usage Over Time', fontsize=13, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(GRAPHS_DIR, 'throughput_over_time.png'), dpi=300)
    plt.close()
    
    print("\n✅ Graphs Generated!")
    print(f"\n📂 Location: {GRAPHS_DIR}/")
    print("  - worker_scaling.png")
    print("  - memory_efficiency.png")
    print("  - throughput_over_time.png")


def main():
    if not os.path.exists(os.path.join(RESULTS_DIR, 'test_results.json')):
        print("❌ No results found! Run: python3 tests/quick_stress_test.py")
        return
    
    generate_all_graphs()
    
    print("\n🎉 Done! Open graphs:")
    print(f"  xdg-open {GRAPHS_DIR}/worker_scaling.png")


if __name__ == '__main__':
    main()
