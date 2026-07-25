#!/usr/bin/env python3
"""
rpbot Endpoints Health Check Utility
Performs parallel latency, connectivity, and status checks on all configured .env endpoints.
"""

import asyncio
import json
import os
import sys
import time
import urllib.request
import urllib.error
import psycopg2
import websockets

def parse_env(filepath):
    env_vars = {}
    if not os.path.exists(filepath):
        return env_vars
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, val = line.split('=', 1)
                env_vars[key.strip()] = val.strip()
    return env_vars

async def check_http_rpc(name, url, custom_headers=None):
    start = time.perf_counter()
    payload = json.dumps({
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    }).encode('utf-8')
    
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (compatible; RpbotHealthCheck/1.0)'
    }
    if custom_headers:
        headers.update(custom_headers)
        
    req = urllib.request.Request(url, data=payload, headers=headers)
        
    try:
        loop = asyncio.get_event_loop()
        def _fetch():
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = resp.read()
                return resp.status, body
        status, body = await loop.run_in_executor(None, _fetch)
        elapsed_ms = (time.perf_counter() - start) * 1000
        
        data = json.loads(body.decode('utf-8'))
        if 'result' in data:
            block_num = int(data['result'], 16)
            return {
                "name": name,
                "url": url,
                "status": "HEALTHY",
                "http_code": status,
                "latency_ms": round(elapsed_ms, 2),
                "block_number": block_num,
                "error": None
            }
        elif 'error' in data:
            return {
                "name": name,
                "url": url,
                "status": "RPC_ERROR",
                "http_code": status,
                "latency_ms": round(elapsed_ms, 2),
                "block_number": None,
                "error": str(data['error'])[:100]
            }
        else:
            return {
                "name": name,
                "url": url,
                "status": "UNEXPECTED_RESPONSE",
                "http_code": status,
                "latency_ms": round(elapsed_ms, 2),
                "block_number": None,
                "error": str(data)[:100]
            }
    except urllib.error.HTTPError as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        err_body = e.read().decode('utf-8', errors='ignore') if e.fp else ""
        return {
            "name": name,
            "url": url,
            "status": "HTTP_ERROR",
            "http_code": e.code,
            "latency_ms": round(elapsed_ms, 2),
            "block_number": None,
            "error": f"HTTP {e.code}: {e.reason} | {err_body[:100]}"
        }
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "name": name,
            "url": url,
            "status": "FAILED",
            "http_code": None,
            "latency_ms": round(elapsed_ms, 2),
            "block_number": None,
            "error": str(e)
        }

async def check_wss_rpc(name, url):
    start = time.perf_counter()
    try:
        async with websockets.connect(url, close_timeout=5, open_timeout=10) as ws:
            payload = json.dumps({
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1
            })
            await ws.send(payload)
            res = await asyncio.wait_for(ws.recv(), timeout=10)
            elapsed_ms = (time.perf_counter() - start) * 1000
            data = json.loads(res)
            if 'result' in data:
                block_num = int(data['result'], 16)
                return {
                    "name": name,
                    "url": url,
                    "status": "HEALTHY",
                    "latency_ms": round(elapsed_ms, 2),
                    "block_number": block_num,
                    "error": None
                }
            else:
                return {
                    "name": name,
                    "url": url,
                    "status": "RPC_ERROR",
                    "latency_ms": round(elapsed_ms, 2),
                    "block_number": None,
                    "error": str(data)[:100]
                }
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "name": name,
            "url": url,
            "status": "FAILED",
            "latency_ms": round(elapsed_ms, 2),
            "block_number": None,
            "error": str(e)
        }

def check_postgres(url):
    start = time.perf_counter()
    try:
        conn = psycopg2.connect(url, connect_timeout=5)
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        cursor.fetchone()
        
        tables_count = 0
        try:
            cursor.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='public';")
            tables_count = cursor.fetchone()[0]
        except Exception:
            pass
            
        cursor.close()
        conn.close()
        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "name": "PG_URL",
            "url": url,
            "status": "HEALTHY",
            "latency_ms": round(elapsed_ms, 2),
            "tables_count": tables_count,
            "error": None
        }
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "name": "PG_URL",
            "url": url,
            "status": "FAILED",
            "latency_ms": round(elapsed_ms, 2),
            "tables_count": 0,
            "error": str(e)
        }

async def probe_bloxroute_auth(url, auth_header):
    start = time.perf_counter()
    payload = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "polygon_private_tx",
        "params": {"transaction": "00"}
    }).encode('utf-8')
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': auth_header,
        'User-Agent': 'Mozilla/5.0'
    }
    
    req = urllib.request.Request(url, data=payload, headers=headers)
    try:
        loop = asyncio.get_event_loop()
        def _fetch():
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return resp.status, resp.read()
            except urllib.error.HTTPError as e:
                body = e.fp.read() if e.fp else b""
                return e.code, body
        status, body_bytes = await loop.run_in_executor(None, _fetch)
        elapsed_ms = (time.perf_counter() - start) * 1000
        
        body = body_bytes.decode('utf-8', errors='ignore')
        if status in [200, 400] and ("invalid" in body.lower() or "transaction" in body.lower() or "result" in body.lower() or "error" in body.lower()):
            auth_valid = "unauthorized" not in body.lower() and "forbidden" not in body.lower() and status != 401 and status != 403
            return {
                "name": "PRIVATE_RPC_URL (bloXroute)",
                "url": url,
                "status": "HEALTHY (AUTH ACCEPTED)" if auth_valid else "AUTH_FAILED",
                "http_code": status,
                "latency_ms": round(elapsed_ms, 2),
                "error": None if auth_valid else "Unauthorized/Forbidden"
            }
        else:
            return {
                "name": "PRIVATE_RPC_URL (bloXroute)",
                "url": url,
                "status": "HTTP_ERROR",
                "http_code": status,
                "latency_ms": round(elapsed_ms, 2),
                "error": f"HTTP {status}"
            }
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "name": "PRIVATE_RPC_URL (bloXroute)",
            "url": url,
            "status": "FAILED",
            "http_code": None,
            "latency_ms": round(elapsed_ms, 2),
            "error": str(e)
        }

async def check_general_http(name, url, headers=None):
    start = time.perf_counter()
    req_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers)
    try:
        loop = asyncio.get_event_loop()
        def _fetch():
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, resp.read()
        status, _ = await loop.run_in_executor(None, _fetch)
        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "name": name,
            "url": url,
            "status": "HEALTHY",
            "http_code": status,
            "latency_ms": round(elapsed_ms, 2),
            "error": None
        }
    except urllib.error.HTTPError as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "name": name,
            "url": url,
            "status": "HTTP_ERROR",
            "http_code": e.code,
            "latency_ms": round(elapsed_ms, 2),
            "error": f"HTTP {e.code}: {e.reason}"
        }
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "name": name,
            "url": url,
            "status": "FAILED",
            "http_code": None,
            "latency_ms": round(elapsed_ms, 2),
            "error": str(e)
        }

async def main():
    dotenv_path = os.environ.get('DOTENV_PATH', '.env')
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    env_file = os.path.join(project_root, dotenv_path) if not os.path.isabs(dotenv_path) else dotenv_path
    
    env_vars = parse_env(env_file)
    print(f"Loaded {len(env_vars)} environment variables from {env_file}\n")
    
    results = {}
    tasks = []
    
    # 1. Postgres DB
    pg_url = env_vars.get('PG_URL')
    if pg_url:
        results['PG_URL'] = check_postgres(pg_url)
        
    # 2. Execution RPC
    exec_rpc = env_vars.get('EXECUTION_RPC')
    if exec_rpc:
        tasks.append(('EXECUTION_RPC', check_http_rpc('EXECUTION_RPC', exec_rpc)))
        
    # 3. State RPC
    state_rpc = env_vars.get('STATE_RPC_URL')
    if state_rpc:
        tasks.append(('STATE_RPC_URL', check_http_rpc('STATE_RPC_URL', state_rpc)))
        
    # 4. POLYGON_RPC_URLS
    rpc_urls = [u.strip() for u in env_vars.get('POLYGON_RPC_URLS', '').split(',') if u.strip()]
    for idx, url in enumerate(rpc_urls):
        tasks.append((f'POLYGON_RPC_URLS[{idx}]', check_http_rpc(f'POLYGON_RPC_URLS[{idx}]', url)))
        
    # 5. POLYGON_WSS_URLS
    wss_urls = [u.strip() for u in env_vars.get('POLYGON_WSS_URLS', '').split(',') if u.strip()]
    for idx, url in enumerate(wss_urls):
        tasks.append((f'POLYGON_WSS_URLS[{idx}]', check_wss_rpc(f'POLYGON_WSS_URLS[{idx}]', url)))

    # 6. PRIVATE_RPC_URL / Bloxroute
    priv_rpc = env_vars.get('PRIVATE_RPC_URL')
    auth_hdr = env_vars.get('BLOXROUTE_AUTH_HEADER')
    if priv_rpc and auth_hdr:
        tasks.append(('PRIVATE_RPC_URL', probe_bloxroute_auth(priv_rpc, auth_hdr)))
    elif priv_rpc:
        tasks.append(('PRIVATE_RPC_URL', check_http_rpc('PRIVATE_RPC_URL', priv_rpc)))

    # 7. Pyth Hermes & Balancer
    pyth_url = env_vars.get('ORACLE_PYTH_HERMES_URL', 'https://hermes.pyth.network')
    pyth_matic_id = "ffd11c5a1cfd42f80afb2df4d9f264c15f956d68153335374ec10722edd70472"
    tasks.append(('PYTH_HERMES', check_general_http('PYTH_HERMES', f"{pyth_url.rstrip('/')}/v2/updates/price/latest?encoding=hex&parsed=true&ids[]={pyth_matic_id}")))
    
    balancer_url = env_vars.get('BALANCER_BACKEND_URL', 'https://api-v3.balancer.fi/')
    tasks.append(('BALANCER_BACKEND', check_general_http('BALANCER_BACKEND', balancer_url)))

    # Run all async checks concurrently
    task_keys = [t[0] for t in tasks]
    coros = [t[1] for t in tasks]
    check_results = await asyncio.gather(*coros)
    
    for key, res in zip(task_keys, check_results):
        if key.startswith('POLYGON_RPC_URLS'):
            results.setdefault('POLYGON_RPC_URLS', []).append(res)
        elif key.startswith('POLYGON_WSS_URLS'):
            results.setdefault('POLYGON_WSS_URLS', []).append(res)
        else:
            results[key] = res

    # Output formatted JSON report
    print(json.dumps(results, indent=2))

if __name__ == '__main__':
    asyncio.run(main())
