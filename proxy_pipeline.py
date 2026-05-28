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

# --- لیست منابع اختصاصی شما ---
SUBSCRIPTION_URLS = [
    "https://raw.githubusercontent.com/itsyebekhe/PSG/main/subscriptions/xray/base64/mix",
    "https://raw.githubusercontent.com/shaoyouvip/free/refs/heads/main/base64.txt",
    "https://raw.githubusercontent.com/telegeam/freenode/refs/heads/master/v2ray.txt",
    "https://raw.githubusercontent.com/DukeMehdi/FreeList-V2ray-Configs/refs/heads/main/Configs/VLESS-V2Ray-Configs-By-DukeMehdi.txt",
    "https://raw.githubusercontent.com/Flikify/Free-Node/refs/heads/main/v2ray.txt",
    "https://raw.githubusercontent.com/RaitonRed/ConfigsHub/refs/heads/main/Splitted-By-Protocol/vless.txt",
    "https://raw.githubusercontent.com/shuaidaoya/FreeNodes/refs/heads/main/nodes/base64.txt",
    "https://raw.githubusercontent.com/penhandev/AutoAiVPN/refs/heads/main/allConfigs.txt",
    "https://raw.githubusercontent.com/Firmfox/Proxify/refs/heads/main/v2ray_configs/seperated_by_protocol/vless.txt",
    "https://raw.githubusercontent.com/crackbest/V2ray-Config/refs/heads/main/config.txt",
    "https://raw.githubusercontent.com/kismetpro/NodeSuber/refs/heads/main/Splitted-By-Protocol/vless.txt",
    "https://raw.githubusercontent.com/jagger235711/V2rayCollector/refs/heads/main/results/vless.txt",
    "https://raw.githubusercontent.com/mohamadfg-dev/telegram-v2ray-configs-collector/refs/heads/main/category/vless.txt",
    "https://raw.githubusercontent.com/SoroushImanian/BlackKnight/refs/heads/main/sub/vless",
    "https://raw.githubusercontent.com/Matin-RK0/ConfigCollector/refs/heads/main/subscription.txt",
    "https://raw.githubusercontent.com/Argh73/VpnConfigCollector/refs/heads/main/All_Configs_Sub.txt",
    "https://raw.githubusercontent.com/3yed-61/configs-collector/refs/heads/main/classified_output/vless.txt",
    "https://raw.githubusercontent.com/Leon406/SubCrawler/refs/heads/main/sub/share/vless",
    "https://raw.githubusercontent.com/ircfspace/XraySubRefiner/refs/heads/main/export/soliSpirit/normal",
    "https://raw.githubusercontent.com/ircfspace/XraySubRefiner/refs/heads/main/export/psgV6/normal",
    "https://raw.githubusercontent.com/ircfspace/XraySubRefiner/refs/heads/main/export/psgMix/normal",
    "https://raw.githubusercontent.com/MhdiTaheri/V2rayCollector_Py/refs/heads/main/sub/Mix/mix.txt",
    "https://raw.githubusercontent.com/T3stAcc/V2Ray/refs/heads/main/Splitted-By-Protocol/vless.txt",
    "https://raw.githubusercontent.com/F0rc3Run/F0rc3Run/refs/heads/main/splitted-by-protocol/vless.txt",
    "https://raw.githubusercontent.com/V2RayRoot/V2RayConfig/refs/heads/main/Config/vless.txt",
    "https://raw.githubusercontent.com/LalatinaHub/Mineral/refs/heads/master/result/nodes",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/All_Configs_Sub.txt",
    "https://raw.githubusercontent.com/hamedcode/port-based-v2ray-configs/refs/heads/main/sub/vless.txt",
    "https://raw.githubusercontent.com/iboxz/free-v2ray-collector/refs/heads/main/main/vless",
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/refs/heads/main/Splitted-By-Protocol/vless.txt",
    "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/refs/heads/main/vless_configs.txt",
    "https://raw.githubusercontent.com/Pasimand/v2ray-config-agg/refs/heads/main/config.txt",
    "https://raw.githubusercontent.com/arshiacomplus/v2rayExtractor/refs/heads/main/vless.html",
    "https://raw.githubusercontent.com/xyfqzy/free-nodes/refs/heads/main/nodes/vless.txt",
    "https://raw.githubusercontent.com/AvenCores/goida-vpn-configs/refs/heads/main/githubmirror/14.txt",
    "https://raw.githubusercontent.com/Awmiroosen/awmirx-v2ray/refs/heads/main/blob/main/v2-sub.txt",
    "https://raw.githubusercontent.com/SoliSpirit/v2ray-configs/refs/heads/main/Protocols/vless.txt",
    "https://raw.githubusercontent.com/gfpcom/free-proxy-list/refs/heads/main/list/vless.txt"
]

# --- تنظیمات پایپ‌لاین ---
MAX_PROCESS_LIMIT = 30000  
FINAL_OUTPUT_COUNT = 500   
TIMEOUT_TCP = 2.5          
TIMEOUT_XRAY = 5.0         
CONCURRENT_TESTS = 35      # تعداد پروسه‌های همزمان تست پینگ Xray
TEST_URL = "http://cp.cloudflare.com"

def fetch_configs(urls):
    """جمع‌آوری هوشمند و استخراج کانفیگ‌ها از انواع فرمت‌های متنی، Base64 و HTML"""
    raw_configs = []
    vless_regex = re.compile(r'vless://[^\s"<]+')
    
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=15) as response:
                content = response.read().decode('utf-8', errors='ignore').strip()
                
                # تشخیص و رمزگشایی خودکار در صورت Base64 بودن کل سورس
                if content and not any(proto in content for proto in ["vless://", "vmess://", "ss://"]):
                    try:
                        # اصلاح پدینگ بازمانده در Base64
                        missing_padding = len(content) % 4
                        if missing_padding:
                            content += '=' * (4 - missing_padding)
                        content = base64.b64decode(content).decode('utf-8', errors='ignore')
                    except:
                        pass
                
                # استخراج دقیق تمام عبارات منطبق با الگوی vless
                found = vless_regex.findall(content)
                raw_configs.extend(found)
        except Exception as e:
            print(f"[-] Error fetching from {url}: {e}")
    return raw_configs

def parse_and_filter_vless(configs):
    """پاک‌سازی، حذف تکراری‌ها و فیلتر کردن کانفیگ‌های VLESS بر پایه Reality یا TLS"""
    unique_configs = list(set(configs))
    valid_configs = []
    
    for conf in unique_configs:
        conf = conf.strip()
        if not conf.startswith("vless://"):
            continue
        try:
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
                host_port = rest.split("?", 1)[0]
                host = host_port.split(":")[0]
                port = int(host_port.split(":")[1]) if ":" in host_port else 443
                
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
    """تست سریع در سطح لایه انتقال (TCP)"""
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
    """اجرای دسته‌ای و ناهمگام تست زنده بودن TCP فرستنده‌ها"""
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
    """نگاشت مشخصات سورس به ساختار استاندارد Outbound در معماری هسته Xray"""
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
    """تست عمیق پینگ از طریق ایجاد تونل‌های موقت Xray کور با پورت مجزا"""
    async with semaphore:
        local_socks_port = 11000 + port_index
        config_filename = f"config_temp_{local_socks_port}.json"
        
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
            proc = await asyncio.create_subprocess_exec(
                "./xray", "-c", config_filename,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            await asyncio.sleep(0.35) 
            
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
    print("[+] Step 1: Querying data from subscription URLs...")
    raw_list = fetch_configs(SUBSCRIPTION_URLS)
    print(f"[+] Total raw nodes collected: {len(raw_list)}")
    
    print("[+] Step 2: Running deduplication & Reality/TLS targeted filtering...")
    parsed_configs = parse_and_filter_vless(raw_list)
    print(f"[+] Total filtered configurations for verification: {len(parsed_configs)}")
    
    print("[+] Step 3: Dispatching asynchronous TCP connection test...")
    alive_targets = await filter_live_hosts(parsed_configs)
    print(f"[+] Nodes passed TCP health-check: {len(alive_targets)}")
    
    print("[+] Step 4: Initializing core Xray latency tests...")
    semaphore = asyncio.Semaphore(CONCURRENT_TESTS)
    
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
    
    successful_tests = [r for r in test_results if r is not None]
    successful_tests.sort(key=lambda x: x['ping'])
    print(f"[+] Speed evaluation complete. Total responsive nodes: {len(successful_tests)}")
    
    top_nodes = successful_tests[:FINAL_OUTPUT_COUNT]
    
    print(f"[+] Committing top {len(top_nodes)} speed-tested configurations to file...")
    with open("best_configs.txt", "w") as f:
        for node in top_nodes:
            f.write(node['config'] + "\n")
            
    print("[+] Architecture flow executed successfully. 'best_configs.txt' is ready.")

if __name__ == "__main__":
    asyncio.run(main_pipeline())
