import asyncio
import base64
import json
import os
import re
import socket
import subprocess
import time
import urllib.request
from urllib.parse import urlparse, parse_qs

# --- پیکربندی‌های اصلی سیستم ---
SUBSCRIPTION_URLS = [
    "https://example.com/sub1",  # لینک‌های سابسکریپشن یا منابع خود را اینجا قرار دهید
    "https://example.com/sub2"
]
MAX_PROCESS_LIMIT = 30000  # سقف کانفیگ‌ها برای ورود به مرحله تست اولیه
FINAL_OUTPUT_COUNT = 500   # تعداد کانفیگ‌های نهایی برای ذخیره در خروجی
TIMEOUT_TCP = 2.5          # تایم‌اوت تست اولیه TCP (ثانیه)
TIMEOUT_XRAY = 5.0         # تایم‌اوت تست عمیق پینگ Xray (ثانیه)
CONCURRENT_TESTS = 30      # تعداد تست‌های همزمان Xray برای جلوگیری از اتلاف وقت
TEST_URL = "http://cp.cloudflare.com"

def fetch_configs(urls):
    """جمع‌آوری و رمزگشایی کانفیگ‌ها از لینک‌های سابسکریپشن"""
    raw_configs = []
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=15) as response:
                content = response.read().decode('utf-8').strip()
                # بررسی و رمزگشایی در صورت Base64 بودن سابسکریپشن
                if not content.startswith("vless://") and not content.startswith("vmess://"):
                    try:
                        content = base64.b64decode(content).decode('utf-8')
                    except:
                        pass
                raw_configs.extend(content.splitlines())
        except Exception as e:
            print(f"[-] Error fetching from {url}: {e}")
    return raw_configs

def parse_and_filter_vless(configs):
    """حذف تکراری‌ها و فیلتر دقیق کانفیگ‌های VLESS دارای TLS یا Reality"""
    unique_configs = list(set(configs))
    valid_configs = []
    
    for conf in unique_configs:
        conf = conf.strip()
        if not conf.startswith("vless://"):
            continue
        try:
            # جداسازی اجزای اصلی کانفیگ برای بررسی شرایط امنیتی
            content = conf[8:]
            if "#" in content:
                content, _ = content.split("#", 1)
            if "@" not in content:
                continue
            _, rest = content.split("@", 1)
            
            query_str = rest.split("?", 1)[1] if "?" in rest else ""
            query = parse_qs(query_str)
            security = query.get('security', [''])[0].lower()
            
            if security in ['tls', 'reality']:
                # استخراج آی‌پی و پورت جهت تست TCP
                host_port = rest.split("?", 1)[0]
                host = host_port.split(":")[0]
                port = int(host_port.split(":")[1]) if ":" in host_port else 443
                
                # پارس کردن پارامترها برای ساخت فایل کانفیگ Xray
                params = {k: v[0] for k, v in query.items()}
                uuid = conf[8:].split("@")[0]
                
                valid_configs.append({
                    'raw': conf,
                    'host': host,
                    'port': port,
                    'uuid': uuid,
                    'params': params
                })
        except:
            continue
            
    return valid_configs[:MAX_PROCESS_LIMIT]

async def tcp_ping(host, port):
    """تست سریع زنده بودن آی‌پی و پورت اَست قبل از تست سنگین Xray"""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), 
            timeout=TIMEOUT_TCP
        )
        writer.close()
        await writer.wait_closed()
        return True
    except:
        return False

async def filter_live_hosts(parsed_configs):
    """اجرای دسته‌ای تست TCP روی تمامی کانفیگ‌های فیلتر شده"""
    live_configs = []
    
    async def check(item):
        if await tcp_ping(item['host'], item['port']):
            live_configs.append(item)
            
    chunk_size = 2000
    for i in range(0, len(parsed_configs), chunk_size):
        chunk = parsed_configs[i:i+chunk_size]
        await asyncio.gather(*(check(item) for item in chunk))
        
    return live_configs

def generate_xray_outbound(item):
    """ساخت دیکشنری استاندارد Outbound ساختار Xray بر اساس پارامترهای کانفیگ"""
    p = item['params']
    security = p.get('security', 'none')
    network = p.get('type', 'tcp')
    
    outbound = {
        "protocol": "vless",
        "settings": {
            "vnext": [{
                "address": item['host'],
                "port": item['port'],
                "users": [{
                    "id": item['uuid'],
                    "encryption": "none",
                    "flow": p.get('flow', '')
                }]
            }]
        },
        "streamSettings": {
            "network": network,
            "security": security
        }
    }
    
    if network == "ws":
        outbound["streamSettings"]["wsSettings"] = {
            "path": p.get('path', '/'),
            "headers": {"Host": p.get('host', item['host'])}
        }
    elif network == "grpc":
        outbound["streamSettings"]["grpcSettings"] = {
            "serviceName": p.get('serviceName', '')
        }
        
    if security == "reality":
        outbound["streamSettings"]["realitySettings"] = {
            "show": False,
            "fingerprint": p.get('fp', 'chrome'),
            "serverName": p.get('sni', ''),
            "publicKey": p.get('pbk', ''),
            "shortId": p.get('sid', '')
        }
    elif security == "tls":
        outbound["streamSettings"]["tlsSettings"] = {
            "serverName": p.get('sni', ''),
            "fingerprint": p.get('fp', 'chrome')
        }
        
    return outbound

async def test_xray_latency(item, port_index, semaphore):
    """اجرای مستقیم هسته Xray روی پورت اختصاصی محلی و سنجش سرعت واقعی لیتنسی با Curl"""
    async with semaphore:
        local_socks_port = 10800 + port_index
        config_filename = f"config_temp_{local_socks_port}.json"
        
        # ساختاربندی کل سیستم لوکال برای این کانفیگ خاص
        xray_main_config = {
            "log": {"loglevel": "none"},
            "inbounds": [{
                "port": local_socks_port,
                "listen": "127.0.0.1",
                "protocol": "socks",
                "settings": {"udp": True}
            }],
            "outbounds": [generate_xray_outbound(item)]
        }
        
        with open(config_filename, "w") as f:
            json.dump(xray_main_config, f)
            
        proc = None
        latency = float('inf')
        try:
            # اجرای غیرمسدودکننده فرآیند Xray
            proc = await asyncio.create_subprocess_exec(
                "./xray", "-c", config_filename,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            await asyncio.sleep(0.4) # زمان کوتاه برای باز شدن پورت توسط هسته
            
            start_time = time.time()
            curl_proc = await asyncio.create_subprocess_exec(
                "curl", "-x", f"socks5h://127.0.0.1:{local_socks_port}",
                "-s", "-o", "/dev/null", "-w", "%{http_code}",
                "--max-time", str(TIMEOUT_XRAY), TEST_URL,
                stdout=asyncio.subprocess.PIPE, stderr=subprocess.DEVNULL
            )
            stdout, _ = await asyncio.wait_for(curl_proc.communicate(), timeout=TIMEOUT_XRAY + 1)
            http_code = stdout.decode().strip()
            
            if http_code in ["200", "204", "301", "302"]:
                latency = (time.time() - start_time) * 1000
        except:
            pass
        finally:
            if proc:
                try:
                    proc.terminate()
                    await proc.wait()
                except:
                    pass
            try:
                os.remove(config_filename)
            except:
                pass
                
        if latency != float('inf'):
            return {'config': item['raw'], 'ping': latency}
        return None

async def main_pipeline():
    print("[+] Step 1: Fetching raw configs from subscriptions...")
    raw_list = fetch_configs(SUBSCRIPTION_URLS)
    print(f"[+] Total raw configs crawled: {len(raw_list)}")
    
    print("[+] Step 2: Filtering VLESS (Reality/TLS) & removing duplicates...")
    parsed_configs = parse_and_filter_vless(raw_list)
    print(f"[+] Cleaned & targeted configurations to scan: {len(parsed_configs)}")
    
    print("[+] Step 3: Running fast TCP health check...")
    alive_targets = await filter_live_hosts(parsed_configs)
    print(f"[+] Passed TCP stage (live nodes): {len(alive_targets)}")
    
    print("[+] Step 4: Running high-concurrency Xray speed test...")
    semaphore = asyncio.Semaphore(CONCURRENT_TESTS)
    
    # مدیریت پورت‌های لوکال به وسیله صف جهت جلوگیری از تداخل فرآیندها
    port_pool = asyncio.Queue()
    for idx in range(CONCURRENT_TESTS):
        await port_pool.put(idx)
        
    async def worker(item):
        port_idx = await port_pool.get()
        res = await test_xray_latency(item, port_idx, semaphore)
        await port_pool.put(port_idx)
        return res

    tasks = [worker(item) for item in alive_targets]
    test_results = await asyncio.gather(*tasks)
    
    # فیلتر موارد موفق و مرتب‌سازی بر اساس پینگ کمتر
    successful_tests = [r for r in test_results if r is not None]
    successful_tests.sort(key=lambda x: x['ping'])
    
    print(f"[+] Sorting completed. Total validated premium nodes: {len(successful_tests)}")
    
    # انتخاب ۵00 تای برتر و ذخیره نهایی
    top_nodes = successful_tests[:FINAL_OUTPUT_COUNT]
    
    print(f"[+] Saving top {len(top_nodes)} speed-tested configs to output file...")
    with open("best_configs.txt", "w") as f:
        for node in top_nodes:
            f.write(node['config'] + "\n")
            
    print("[+] Pipeline processed successfully. Output file 'best_configs.txt' updated.")

if __name__ == "__main__":
    asyncio.run(main_pipeline())
