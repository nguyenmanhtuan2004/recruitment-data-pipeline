import argparse
import asyncio
import json
import random
import sys
import time
import math
from typing import List

# Kiểm tra xem aiohttp có sẵn hay không, nếu không sẽ dùng urllib + ThreadPoolExecutor
try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False
    import urllib.request
    from concurrent.futures import ThreadPoolExecutor


def generate_click_payload() -> dict:
    """Tạo payload sự kiện click phù hợp với API /api/track"""
    return {
        "custom_track": "click",
        "bid": random.choice([1, 2, 3, 5]),
        "job_id": random.randint(1, 500),
        "publisher_id": random.randint(1, 50),
        "campaign_id": random.randint(1, 20),
        "group_id": random.randint(1, 10)
    }


def calculate_percentile(sorted_data: List[float], percentile: float) -> float:
    """Tính percentile (ms)"""
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (percentile / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_data[int(k)]
    d0 = sorted_data[int(f)] * (c - k)
    d1 = sorted_data[int(c)] * (k - f)
    return d0 + d1


# =========================================================
# ASYNCIO IMPLEMENTATION (Khi có aiohttp)
# =========================================================
async def async_worker(session: aiohttp.ClientSession, url: str, end_time: float, latencies: List[float], results: dict):
    headers = {"Content-Type": "application/json"}
    while time.perf_counter() < end_time:
        payload = generate_click_payload()
        start = time.perf_counter()
        try:
            async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                if resp.status in (200, 202):
                    results["success"] += 1
                    latencies.append(elapsed_ms)
                else:
                    results["failed"] += 1
        except Exception:
            results["failed"] += 1
            results["errors"] += 1


async def run_async_benchmark(url: str, concurrency: int, duration: int):
    results = {"success": 0, "failed": 0, "errors": 0}
    latencies: List[float] = []
    
    conn = aiohttp.TCPConnector(limit=concurrency * 2, keepalive_timeout=60)
    async with aiohttp.ClientSession(connector=conn) as session:
        end_time = time.perf_counter() + duration
        tasks = [
            asyncio.create_task(async_worker(session, url, end_time, latencies, results))
            for _ in range(concurrency)
        ]
        await asyncio.gather(*tasks)
        
    return results, latencies


# =========================================================
# THREADPOOL FALLBACK IMPLEMENTATION (Khi không có aiohttp)
# =========================================================
def sync_worker_task(url: str, end_time: float):
    headers = {"Content-Type": "application/json"}
    success = 0
    failed = 0
    errors = 0
    latencies = []
    
    while time.perf_counter() < end_time:
        payload_bytes = json.dumps(generate_click_payload()).encode('utf-8')
        req = urllib.request.Request(url, data=payload_bytes, headers=headers, method='POST')
        start = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                if response.status in (200, 202):
                    success += 1
                    latencies.append(elapsed_ms)
                else:
                    failed += 1
        except Exception:
            failed += 1
            errors += 1
            
    return success, failed, errors, latencies


def run_sync_benchmark(url: str, concurrency: int, duration: int):
    end_time = time.perf_counter() + duration
    total_success = 0
    total_failed = 0
    total_errors = 0
    all_latencies = []
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(sync_worker_task, url, end_time) for _ in range(concurrency)]
        for f in futures:
            succ, fail, err, lats = f.result()
            total_success += succ
            total_failed += fail
            total_errors += err
            all_latencies.extend(lats)
            
    results = {"success": total_success, "failed": total_failed, "errors": total_errors}
    return results, all_latencies


# =========================================================
# MAIN REPORTING & CLI
# =========================================================
def print_report(url: str, concurrency: int, duration: float, results: dict, latencies: List[float], engine_name: str):
    total_reqs = results["success"] + results["failed"]
    rps = results["success"] / duration if duration > 0 else 0
    success_rate = (results["success"] / total_reqs * 100) if total_reqs > 0 else 0.0
    
    latencies.sort()
    min_lat = latencies[0] if latencies else 0.0
    avg_lat = (sum(latencies) / len(latencies)) if latencies else 0.0
    p50_lat = calculate_percentile(latencies, 50)
    p95_lat = calculate_percentile(latencies, 95)
    p99_lat = calculate_percentile(latencies, 99)
    max_lat = latencies[-1] if latencies else 0.0

    print("\n" + "="*65)
    print(f"RECRUITMENT PIPELINE LOAD TEST REPORT (Engine: {engine_name})")
    print("="*65)
    print(f" Target API URL   : {url}")
    print(f" Concurrency      : {concurrency} workers")
    print(f" Actual Duration  : {duration:.2f} seconds")
    print("-" * 65)
    print(f" Total Requests   : {total_reqs:,}")
    print(f" Successful Reqs  : {results['success']:,} ({success_rate:.2f}%)")
    print(f" Failed Reqs      : {results['failed']:,}")
    print(f" THROUGHPUT       : {rps:,.2f} Clicks/sec (RPS)")
    print("-" * 65)
    print(" LATENCY DISTRIBUTION (ms):")
    print(f"    Min Latency   : {min_lat:.2f} ms")
    print(f"    Avg Latency   : {avg_lat:.2f} ms")
    print(f"    P50 (Median)  : {p50_lat:.2f} ms")
    print(f"    P95           : {p95_lat:.2f} ms  <-- Indicative for SLA")
    print(f"    P99           : {p99_lat:.2f} ms")
    print(f"    Max Latency   : {max_lat:.2f} ms")
    print("="*65 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Benchmark load tester for Recruitment Data Pipeline")
    parser.add_argument("--url", type=str, default="http://127.0.0.1:8082/api/track", help="Target API URL")
    parser.add_argument("--concurrency", type=int, default=50, help="Number of concurrent workers")
    parser.add_argument("--duration", type=int, default=15, help="Test duration in seconds")
    args = parser.parse_args()

    print(f"Starting Load Test against {args.url} ...")
    print(f"Config: Concurrency={args.concurrency}, Target Duration={args.duration}s")
    
    start_wall = time.perf_counter()
    if HAS_AIOHTTP:
        engine = "asyncio + aiohttp"
        results, latencies = asyncio.run(run_async_benchmark(args.url, args.concurrency, args.duration))
    else:
        engine = "ThreadPoolExecutor + urllib (Fallback)"
        results, latencies = run_sync_benchmark(args.url, args.concurrency, args.duration)
        
    actual_duration = time.perf_counter() - start_wall
    print_report(args.url, args.concurrency, actual_duration, results, latencies, engine)


if __name__ == "__main__":
    main()
