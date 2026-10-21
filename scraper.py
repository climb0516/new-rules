#!/usr/bin/env python3
# Created by 青云 on 2026/10/21.
"""每月新规 scraper — 抓取即将施行的法律法规、强制性国家标准和新闻里的新规，输出 data.json。仅依赖 Python 标准库。"""
import hashlib, html, json, re, struct, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

ROOT  = Path(__file__).parent
OUT   = ROOT / "data.json"
CST   = timezone(timedelta(hours=8))
TODAY = datetime.now(CST).date()
MONTH_START = TODAY.replace(day=1)
KEEP_FROM   = (MONTH_START - timedelta(days=1)).replace(day=1)   # 保留到上个月 1 日起施行的
HORIZON     = TODAY + timedelta(days=400)                         # 太远的不收

UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
      "Accept-Language": "zh-CN,zh;q=0.9"}

FLK      = "https://flk.npc.gov.cn"
FLK_PAGES = 6            # 按公布日期倒序翻几页（每页 50），足够覆盖近几个月公布、本月起施行的
STD_PAGES = 3
NEWS_DAYS = 45
COVER_BUDGET = 150       # 配图抓取最多花多少秒（增量：已有 img、已试过的文章不再抓）
COVER_TRIES  = 3         # 每条新规每次最多试几篇报道

NEWS_QUERIES = ["{m}月1日起施行", "{m}月1日起实施", "{m}月起 新规", "{m}月起 这些新规", "{m}月1日起 正式实施"]

# 分类：关键词命中即打标签（可多个）
TAGS = {
    "社保医保": "社保|医保|养老金|养老保险|医疗保险|工伤|失业保险|生育|长护险|退休",
    "收入个税": "个税|个人所得税|工资|最低工资|收入|税务|纳税|发票|补贴|津贴|个人码",
    "交通出行": "交通|驾驶|驾照|机动车|电动车|电动自行车|摩托车|单车|停车|高速|地铁|公交|铁路|航空|民航|网约车|出行|船舶|道路|公路",
    "消费购物": "消费|商品|价格|收费|网购|平台|快递|外卖|预付|退货|广告|直播带货|能效|家电|充电宝|产品质量|用品|加油|成品油|纺织",
    "住房物业": "住房|房屋|物业|租赁|公积金|不动产|装修|城市更新|老旧小区|燃气|房贷|购房|买房|首套|电梯|供暖",
    "教育育儿": "教育|学校|学生|教师|幼儿园|托育|育儿|招生|未成年|家庭",
    "就业劳动": "就业|劳动|用工|招聘|劳务|职工|求职|人才|灵活就业",
    "金融投资": "金融|银行|保险|证券|基金|减持|上市公司|贷款|信用卡|支付|车险|理财",
    "医疗健康": "就医|看病|医院|医疗|医保|药品|医疗器械|针灸|中医|疫苗|健康|卫生",
    "食品药品": "食品|药品|化妆品|餐饮|保健",
    "三农": "农村|村民|农民|农业|耕地|粮食|宅基地",
    "网络数据": "网络|数据|个人信息|App|APP|人工智能|算法|短视频|网络安全|电信|无线电|手机|手机号|电话卡|星闪",
    "安全应急": "消防|安全生产|安全管理|安全技术|应急|特种设备|危险|安全检查",
    "生态环境": "环境|污染|生态|大气|排放|垃圾|碳",
    "出入境": "出入境|签证|护照|口岸|海关|入境|免签",
}

PROVINCES = "北京 天津 上海 重庆 河北 山西 辽宁 吉林 黑龙江 江苏 浙江 安徽 福建 江西 山东 河南 湖北 湖南 广东 海南 四川 贵州 云南 陕西 甘肃 青海 台湾 内蒙古 广西 西藏 宁夏 新疆 香港 澳门".split()
CITY_PROV = {
    "广东": "广州 深圳 珠海 汕头 佛山 东莞 中山 惠州 江门 湛江 肇庆 番禺 南沙",
    "浙江": "杭州 宁波 温州 绍兴 嘉兴 湖州 金华 台州 义乌", "江苏": "南京 苏州 无锡 常州 南通 扬州 徐州 镇江 盐城",
    "山东": "济南 青岛 烟台 潍坊 临沂 淄博 威海", "福建": "福州 厦门 泉州 漳州", "湖北": "武汉 宜昌 襄阳",
    "湖南": "长沙 株洲 湘潭 岳阳", "四川": "成都 绵阳 宜宾", "河南": "郑州 洛阳 开封", "河北": "石家庄 唐山 保定 雄安",
    "辽宁": "沈阳 大连", "陕西": "西安 宝鸡", "安徽": "合肥 芜湖", "江西": "南昌 赣州 九江", "云南": "昆明 玉溪 大理",
    "贵州": "贵阳 遵义", "广西": "南宁 桂林 柳州", "海南": "海口 三亚", "吉林": "长春", "黑龙江": "哈尔滨",
    "山西": "太原 大同", "内蒙古": "呼和浩特 包头", "甘肃": "兰州", "新疆": "乌鲁木齐", "宁夏": "银川", "青海": "西宁",
    "西藏": "拉萨 林芝 日喀则",
}
CITY2PROV = {c: p for p, cs in CITY_PROV.items() for c in cs.split()}
# 再补一批常见地级市（新闻里常出现、但上面没有的）
for _p, _cs in {
    "四川": "泸州 德阳 南充 乐山 达州 自贡 攀枝花 广元 遂宁 内江 眉山 广安 雅安 巴中 资阳 凉山",
    "湖南": "衡阳 常德 郴州 邵阳 益阳 永州 怀化 娄底 张家界 湘西", "江西": "鹰潭 上饶 景德镇 萍乡 新余 吉安 宜春 抚州",
    "湖北": "黄石 十堰 荆州 荆门 孝感 黄冈 咸宁 随州 恩施 鄂州", "河南": "南阳 许昌 商丘 濮阳 新乡 安阳 焦作 平顶山 信阳 周口 驻马店 漯河 三门峡 鹤壁",
    "广东": "韶关 茂名 梅州 汕尾 河源 阳江 清远 潮州 揭阳 云浮 侨都", "广西": "梧州 北海 防城港 钦州 贵港 玉林 百色 贺州 河池 来宾 崇左",
    "云南": "曲靖 保山 昭通 丽江 普洱 临沧 楚雄 红河 文山 西双版纳 德宏 怒江 迪庆", "新疆": "昌吉 博州 伊犁 喀什 和田 阿克苏 克拉玛依 吐鲁番 哈密 巴音郭楞 塔城 阿勒泰",
    "青海": "果洛 海北 海南州 海西 玉树 黄南 海东", "内蒙古": "呼伦贝尔 通辽 赤峰 鄂尔多斯 乌兰察布 巴彦淖尔 乌海 锡林郭勒 兴安",
    "辽宁": "鞍山 抚顺 本溪 丹东 锦州 营口 阜新 辽阳 盘锦 铁岭 朝阳 葫芦岛", "吉林": "吉林市 四平 辽源 通化 白山 松原 白城 延边",
    "黑龙江": "齐齐哈尔 牡丹江 佳木斯 大庆 鸡西 双鸭山 伊春 七台河 鹤岗 黑河 绥化", "江苏": "连云港 淮安 宿迁 泰州",
    "浙江": "衢州 舟山 丽水", "安徽": "蚌埠 淮南 马鞍山 淮北 铜陵 安庆 黄山 滁州 阜阳 宿州 六安 亳州 池州 宣城",
    "福建": "莆田 三明 南平 龙岩 宁德", "山东": "枣庄 东营 济宁 泰安 日照 德州 聊城 滨州 菏泽",
    "河北": "秦皇岛 邯郸 邢台 张家口 承德 沧州 廊坊 衡水", "山西": "阳泉 长治 晋城 朔州 晋中 运城 忻州 临汾 吕梁",
    "陕西": "铜川 咸阳 渭南 延安 汉中 榆林 安康 商洛", "甘肃": "嘉峪关 金昌 白银 天水 武威 张掖 平凉 酒泉 庆阳 定西 陇南 临夏 甘南",
    "贵州": "六盘水 安顺 毕节 铜仁 黔东南 黔南 黔西南", "宁夏": "石嘴山 吴忠 固原 中卫", "海南": "儋州 琼海 万宁",
}.items():
    for _c in _cs.split():
        CITY2PROV.setdefault(_c, _p)
# 识别不出省份、但明显是某个市州县的
LOCAL_RE = re.compile(r"(?<![城都超上门])[\u4e00-\u9fa5]{1,3}(?:市|州|县|盟)(?![场民监政区])|本市|我市|全市|全省|我省|本省")
NATIONAL = re.compile(r"国务院|国家|全国|中华人民共和国|总局|部令|交通运输部|财政部|人社部|人力资源社会保障部|教育部|公安部|商务部|"
                      r"工信部|工业和信息化部|住建部|住房城乡建设部|民政部|卫健委|医保局|央行|人民银行|金融监管|证监会|网信办|市场监管总局|强制性国家标准|新国标")
# 国外 / 跨境电商的新规不收
FOREIGN = re.compile(r"美国|欧盟|英国|日本|韩国|德国|法国|匈牙利|哈萨克斯坦|俄罗斯|印度|越南|泰国|新加坡|马来西亚|澳大利亚|加拿大|"
                     r"新西兰|古巴|菲律宾|印尼|印度尼西亚|土耳其|意大利|西班牙|荷兰|瑞士|瑞典|挪威|以色列|埃及|南非|阿根廷|智利|"
                     r"乌克兰|柬埔寨|老挝|缅甸|巴基斯坦|孟加拉|尼泊尔|蒙古国|中东|迪拜|华人注意|移民|"
                     r"巴西|墨西哥|沙特|阿联酋|亚马逊|eBay|TikTok Shop|Temu|SHEIN|跨境卖家|海外仓|站点")
OFFICIAL = re.compile(r"新华|央视|人民|中国政府网|国务院|中新|光明|经济日报|中国日报|求是|法治日报|中国青年")

# ── 工具 ──────────────────────────────────────────────────────────────────────

def fetch(url, data=None, headers=None, timeout=25):
    h = {**UA, **(headers or {})}
    return urllib.request.urlopen(urllib.request.Request(url, data=data, headers=h), timeout=timeout).read()

def clean(text):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", text or ""))).strip()

def run(label, fn):
    try:
        res = fn()
        print(f"  ✓ {label:<12} {len(res):>4}")
        return res, ""
    except Exception as e:
        err = f"{type(e).__name__}: {e}"[:200]
        print(f"  ✗ {label:<12}    0 {err}")
        return None, err

def tags_of(text):
    return [k for k, pat in TAGS.items() if re.search(pat, text)]

def region_of(text, national_hint=""):
    """返回省份名；明显是地方但认不出省份返回「其他地方」；全国性的返回 ''。
    national_hint：摘要等补充文本，出现国家级机关时按全国处理（标题里只有「XX 市」也可能是全国新规的地方报道）。"""
    hits = [(text.find(p), p) for p in PROVINCES if p in text]
    hits += [(text.find(c), CITY2PROV[c]) for c in CITY2PROV if c in text]
    if hits:
        return min(hits)[1]
    if NATIONAL.search(text + national_hint):
        return ""
    return "其他地方" if LOCAL_RE.search(text) else ""

def in_window(d):
    return d and KEEP_FROM <= d <= HORIZON

def norm(t):
    return re.sub(r"[^一-龥A-Za-z0-9]", "", t)

# ── 法律法规（国家法律法规数据库） ────────────────────────────────────────────

def flk_page(n):
    body = {"searchRange": 1, "sxrq": [], "gbrq": [], "searchType": 2, "sxx": [], "gbrqYear": [],
            "flfgCodeId": [], "zdjgCodeId": [], "searchContent": "", "pageNum": n, "pageSize": 50}
    d = json.loads(fetch(FLK + "/law-search/search/list", json.dumps(body).encode(),
                         {"Content-Type": "application/json", "Referer": FLK + "/"}))
    if d.get("code") != 200:
        raise ValueError(d.get("msg"))
    return d.get("rows") or []

def laws():
    with ThreadPoolExecutor(3) as ex:
        rows = [r for page in ex.map(flk_page, range(1, FLK_PAGES + 1)) for r in page]
    if not rows:
        raise ValueError("empty")
    out, seen = [], set()
    for r in rows:
        try:
            d = date.fromisoformat(r.get("sxrq") or "")
        except ValueError:
            continue
        if not in_window(d) or r["bbbs"] in seen or r.get("flxz") == "修改、废止的决定":
            continue
        seen.add(r["bbbs"])
        org = r.get("zdjgName") or ""
        local = r.get("flxz") in ("地方法规", "自治条例", "单行条例", "经济特区法规")
        title = r["title"]
        out.append({
            "id": "flk-" + r["bbbs"], "kind": "law", "type": r.get("flxz") or "法规", "title": title,
            "date": d.isoformat(), "pub": r.get("gbrq") or "", "org": org,
            "region": (region_of(org) or region_of(title) or "其他地方") if local else "",
            "tags": tags_of(title), "summary": "",
            "url": f"{FLK}/detail?" + urllib.parse.urlencode({"id": r["bbbs"], "title": title}),
            "articles": [],
        })
    return out

# ── 强制性国家标准（国家标准全文公开系统） ────────────────────────────────────

def std_page(n):
    url = ("https://openstd.samr.gov.cn/bzgk/std/std_list?r=0.5&page=%d&pageSize=50&p.p1=1"
           "&p.p90=circulation_date&p.p91=desc" % n)
    s = fetch(url).decode("utf-8", "ignore")
    out = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", s, re.S):
        tds = [clean(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
        hc = re.search(r"showInfo\('([0-9A-Fa-f]+)'\)", tr)
        if len(tds) >= 9 and tds[1].startswith("GB"):
            out.append((tds, hc[1] if hc else ""))
    return out

def standards():
    with ThreadPoolExecutor(3) as ex:
        rows = [r for page in ex.map(std_page, range(1, STD_PAGES + 1)) for r in page]
    if not rows:
        raise ValueError("empty")
    out = []
    for tds, hc in rows:
        try:
            d = date.fromisoformat(tds[8][:10])
        except ValueError:
            continue
        if not in_window(d):
            continue
        out.append({
            "id": "std-" + norm(tds[1]), "kind": "std", "type": "强制性国标", "title": f"{tds[4]}（{tds[1]}）",
            "date": d.isoformat(), "pub": tds[7][:10], "org": "国家市场监督管理总局 / 国家标准委", "region": "",
            "tags": tags_of(tds[4]), "summary": "",
            "url": f"https://openstd.samr.gov.cn/bzgk/gb/newGbInfo?hcno={hc}" if hc else "https://openstd.samr.gov.cn/",
            "articles": [],
        })
    return out

# ── 新闻（Google / Bing 新闻 RSS） ────────────────────────────────────────────

def google_news(q):
    url = "https://news.google.com/rss/search?" + urllib.parse.urlencode({"q": q, "hl": "zh-CN", "gl": "CN", "ceid": "CN:zh-Hans"})
    x = fetch(url).decode("utf-8", "ignore")
    out = []
    for it in re.findall(r"<item>(.*?)</item>", x, re.S):
        t = clean(re.search(r"<title>(.*?)</title>", it, re.S)[1])
        src = re.search(r"<source[^>]*>(.*?)</source>", it, re.S)
        source = clean(src[1]) if src else ""
        if source and t.endswith(" - " + source):
            t = t[: -len(source) - 3]
        pub = re.search(r"<pubDate>(.*?)</pubDate>", it, re.S)
        out.append({"title": t, "url": clean(re.search(r"<link>(.*?)</link>", it, re.S)[1]), "source": source,
                    "published": pub and pub[1], "desc": ""})
    return out

def bing_news(q):
    url = "https://www.bing.com/news/search?" + urllib.parse.urlencode({"q": q, "format": "rss", "setlang": "zh-cn"})
    x = fetch(url).decode("utf-8", "ignore")
    out = []
    for it in re.findall(r"<item>(.*?)</item>", x, re.S):
        link = clean(re.search(r"<link>(.*?)</link>", it, re.S)[1])
        real = urllib.parse.parse_qs(urllib.parse.urlparse(link).query).get("url")
        pub = re.search(r"<pubDate>(.*?)</pubDate>", it, re.S)
        src = re.search(r"<News:Source>(.*?)</News:Source>", it, re.S)
        desc = re.search(r"<description>(.*?)</description>", it, re.S)
        out.append({"title": clean(re.search(r"<title>(.*?)</title>", it, re.S)[1]), "url": real[0] if real else link,
                    "source": clean(src[1]) if src else "", "published": pub and pub[1], "desc": clean(desc[1]) if desc else ""})
    return out

def parse_pub(s):
    try:
        return parsedate_to_datetime(s).astimezone(CST)
    except Exception:
        return None

EFF_RE = re.compile(r"(?:(20\d{2})\s*年\s*)?(\d{1,2})\s*月\s*(?:(\d{1,2})\s*日\s*)?(?:起|开始)?\s*(?:正式|全面)?"
                    r"(?:施行|实施|执行|落地|生效|实行)")
ROUNDUP_RE = re.compile(r"\d{1,2}\s*月(?:1日)?起[^。]{0,8}(?:这些|一批|多项|一大波|系列)[^，,。]{0,6}(?:新规|规定|法规)")

def effective_date(text, ref):
    m = EFF_RE.search(text)
    if not m:
        return None
    y, mo, d = m[1], int(m[2]), int(m[3] or 1)
    if not 1 <= mo <= 12:
        return None
    try:
        if y:
            return date(int(y), mo, d)
        cand = date(ref.year, mo, d)
        if (ref - cand).days > 200:          # 12 月发的「1 月起施行」→ 明年
            cand = date(ref.year + 1, mo, d)
        return cand
    except ValueError:
        return None

def news(prev_items):
    months = sorted({TODAY.month, (TODAY.replace(day=28) + timedelta(days=5)).month})
    raw = []
    for m in months:
        for q in NEWS_QUERIES:
            for fn in (google_news, bing_news):
                try:
                    raw += fn(q.format(m=m))
                except Exception as e:
                    print(f"    {fn.__name__}({q.format(m=m)}) 失败：{e}")
    if not raw:
        raise ValueError("all queries failed")

    cutoff = datetime.now(CST) - timedelta(days=NEWS_DAYS)
    items = {e["id"]: e for e in prev_items}
    roundups = {}
    for it in raw:
        pub = parse_pub(it["published"])
        t = it["title"]
        if not pub or pub < cutoff or FOREIGN.search(t + it["desc"][:60]):
            continue
        art = {"title": t, "url": it["url"], "source": it["source"], "published": pub.isoformat(timespec="minutes")}
        if ROUNDUP_RE.search(t):
            rm = re.search(r"(\d{1,2})\s*月", t)
            if not rm or int(rm[1]) not in months:
                continue
            key = norm(t)[:20]
            roundups.setdefault(key, {**art, "official": bool(OFFICIAL.search(it["source"] + t))})
            continue
        if it["desc"] and not relevant(it["desc"], t):    # Bing 偶尔配错摘要，配错的整段丢掉，免得按错误的法规名合并
            it["desc"] = ""
        d = effective_date(t, pub.date()) or effective_date(it["desc"], pub.date())
        if not in_window(d):
            continue
        # 法规名优先从标题取《》，没有再从摘要里取
        name = re.search(r"《([^》]{4,60})》", t) or re.search(r"《([^》]{4,60})》", it["desc"])
        key = "news-" + (norm(name[1]) if name else norm(t)[:24])
        e = items.get(key)
        if e is None:
            text = t + " " + (name[1] if name else "")
            e = items[key] = {
                "id": key, "kind": "news", "type": "新闻", "title": t, "name": name[1] if name else "",
                "date": d.isoformat(), "pub": art["published"][:10], "org": "", "region": region_of(t, it["desc"][:120]),
                "tags": tags_of(text + " " + it["desc"][:80]), "summary": it["desc"][:220], "url": it["url"], "articles": [],
            }
        if not e.get("summary") and it["desc"]:
            e["summary"] = it["desc"][:220]
        if all(a["url"] != art["url"] for a in e["articles"]):
            e["articles"].append(art)
    out = []
    for e in cluster([e for e in items.values() if in_window(date.fromisoformat(e["date"]))]):
        e["articles"] = sorted(e["articles"], key=lambda a: a["published"], reverse=True)[:6]
        out.append(e)
    rs = sorted(roundups.values(), key=lambda a: a["published"], reverse=True)
    news.roundups = rs[:30]
    return out
news.roundups = []

STOP = re.compile(r"\d+|新规|新政|规定|起|施行|实施|执行|生效|正式|全面|落地|来了|来啦|要来|重磅|注意|提醒|定了|最新|发布|事关|"
                  r"关注|民生|一定要看|月|日|年|重要|起来|这项|这些|今起|明起|划重点|速看|收藏|丨|新国标|新标准|即将|变化|有变|重大|"
                  r"出台|首个|我们|你|的|了|将|于|和|与|在|是")

def clauses(t):
    """标题按标点切成小句，每句取二字片段；去掉「|标签」「_新浪网」这类尾巴和日期、套话。"""
    t = re.sub(r"\|.*$|_.*$", "", t)
    out = []
    for seg in re.split(r"[，,。！!？?：:；;丨|、\s“”\"（）()【】\[\]]+", t):
        core = norm(STOP.sub("", seg))
        bg = {core[i:i + 2] for i in range(len(core) - 1)}
        if bg:
            out.append(bg)
    return out

def clean_title(t):
    """去掉「|标签|标签」「_新浪网」「- 今日热评」这类尾巴。"""
    t = re.sub(r"\s*[|｜]\s*[^|｜]{0,12}(?:[|｜][^|｜]{0,12})+$", "", t)
    t = re.sub(r"\s*[_\-—]\s*(?:新浪|腾讯|网易|搜狐|凤凰|手机|今日|人民|中国)[^_\-—]{0,16}$", "", t)
    return t.strip(" |｜_-—")

def relevant(summary, title):
    """摘要和标题至少共享 2 个核心二字片段才算相关（Bing 偶尔给出不相干的摘要）。"""
    a = set().union(*clauses(title)) if clauses(title) else set()
    b = norm(summary)
    return sum(1 for x in a if x in b) >= 2

def similar(ca, cb):
    for a in ca:
        for b in cb:
            shared = len(a & b)
            if a == b or (shared >= 2 and shared / min(len(a), len(b)) >= 0.5):
                return True
    return False

def cluster(items):
    """同一施行日期、标题核心词相似（或法规名互相包含）的新闻合并成一条。"""
    items = sorted(items, key=lambda e: -len(e["articles"]))
    groups = []
    for e in items:
        cl = clauses(e["title"]) + clauses(e.get("name") or "")
        for g in groups:
            if g["date"] != e["date"]:
                continue
            same_name = e.get("name") and g.get("name") and (e["name"] in g["name"] or g["name"] in e["name"])
            name_in = (g.get("name") and g["name"] in e["title"]) or (e.get("name") and e["name"] in g["title"])
            if same_name or name_in or similar(cl, g["_cl"]):
                seen = {a["url"] for a in g["articles"]}
                g["articles"] += [a for a in e["articles"] if a["url"] not in seen]
                g["tags"] = sorted(set(g["tags"]) | set(e["tags"]), key=list(TAGS).index)
                g["name"] = g.get("name") or e.get("name") or ""
                g["summary"] = g.get("summary") or e.get("summary") or ""
                if g["region"] and not e["region"]:         # 有一篇按全国报道就算全国
                    g["region"] = ""
                g["_cl"] += cl
                break
        else:
            groups.append({**e, "_cl": list(cl)})
    for g in groups:
        g.pop("_cl")
        titles = [clean_title(a["title"]) for a in g["articles"]] or [clean_title(g["title"])]
        g["title"] = min(titles, key=lambda t: (g.get("name", "") not in t if g.get("name") else 0, abs(len(t) - 26)))
        if g.get("summary") and not relevant(g["summary"], g["title"] + (g.get("name") or "")):
            g["summary"] = ""
    return groups

def merge(laws_, stds, news_):
    """新闻里提到的法规名能对上官方条目时，把报道挂到官方条目上，不再单列。"""
    by_name = {norm(x["title"]): x for x in laws_}
    rest = []
    for n in news_:
        hit = by_name.get(norm(n.get("name") or ""))
        if hit:
            seen = {a["url"] for a in hit["articles"]}
            hit["articles"] += [a for a in n["articles"] if a["url"] not in seen]
            hit["summary"] = hit["summary"] or n["summary"]
            hit["tags"] = sorted(set(hit["tags"]) | set(n["tags"]), key=list(TAGS).index)
        else:
            rest.append(n)
    items = laws_ + stds + rest
    items.sort(key=lambda x: (x["date"], x["kind"] != "law", x["title"]))
    return items

# ── 配图（报道原文的 og:image / 正文首图） ────────────────────────────────────

GN_EXEC = "https://news.google.com/_/DotsSplashUi/data/batchexecute"

def gnews_real(url):
    """news.google.com/rss/articles/<id> 新版链接不再 302：先取文章页里的签名和时间戳，再调 batchexecute 换出原文地址。"""
    aid = urllib.parse.urlparse(url).path.rstrip("/").split("/")[-1]
    s = fetch(f"https://news.google.com/rss/articles/{aid}", timeout=10).decode("utf-8", "ignore")
    sg, ts = re.search(r'data-n-a-sg="([^"]+)"', s), re.search(r'data-n-a-ts="(\d+)"', s)
    if not (sg and ts):
        return None
    inner = ["garturlreq", [["X", "X", ["X", "X"], None, None, 1, 1, "US:en", None, 1, None, None, None, None, None, 0, 1],
                            "X", "X", 1, [1, 1, 1], 1, 1, None, 0, 0, None, 0], aid, int(ts[1]), sg[1]]
    body = urllib.parse.urlencode({"f.req": json.dumps([[["Fbv4je", json.dumps(inner), None, "generic"]]])}).encode()
    r = fetch(GN_EXEC, body, {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}, timeout=10).decode("utf-8", "ignore")
    m = re.search(r'\[\\"garturlres\\",\\"(https?://[^"\\]+)', r)
    return m[1] if m else None

def img_size(b):
    """从文件头读宽高（PNG / GIF / WebP / JPEG），读不出返回 None。"""
    if b[:8] == b"\x89PNG\r\n\x1a\n":
        return struct.unpack(">II", b[16:24])
    if b[:6] in (b"GIF87a", b"GIF89a"):
        return struct.unpack("<HH", b[6:10])
    if b[:4] == b"RIFF" and b[8:12] == b"WEBP":
        c = b[12:16]
        if c == b"VP8 ":
            w, h = struct.unpack("<HH", b[26:30])
            return w & 0x3fff, h & 0x3fff
        if c == b"VP8L":
            v = int.from_bytes(b[21:25], "little")
            return (v & 0x3fff) + 1, ((v >> 14) & 0x3fff) + 1
        if c == b"VP8X":
            return int.from_bytes(b[24:27], "little") + 1, int.from_bytes(b[27:30], "little") + 1
    if b[:2] == b"\xff\xd8":
        i = 2
        while i + 9 < len(b):
            if b[i] != 0xFF:
                i += 1
                continue
            mk = b[i + 1]
            if mk in (0xD8, 0x01, 0xFF) or 0xD0 <= mk <= 0xD7:
                i += 1 if mk == 0xFF else 2
                continue
            if 0xC0 <= mk <= 0xCF and mk not in (0xC4, 0xC8, 0xCC):
                h, w = struct.unpack(">HH", b[i + 5:i + 9])
                return w, h
            i += 2 + struct.unpack(">H", b[i + 2:i + 4])[0]
    return None

# 站点 logo、二维码、模板占位图等
BAD_IMG = re.compile(r"logo|icon|avatar|qrcode|erweima|ewm|/qr/|share|default|placeholder|blank|loading|banner|404|danghui|"
                     r"gov_small|fd-img|wechat|weixin|/assets/|/skin/|/static/|/_next/|\.svg|\.gif", re.I)

OLD_PATH = re.compile(r"(?<!\d)(20[012]\d)[/_-]?(?:0[1-9]|1[0-2])")

def bad_img(u):
    """logo、二维码、模板图；路径里带着前年以前日期的多半是站点常年不换的默认图。"""
    old = OLD_PATH.search(u)
    return bool(BAD_IMG.search(u) or (old and int(old[1]) < TODAY.year - 1))

def good_img(u):
    """不带 Referer（页面设了 no-referrer）能直接拿到、尺寸像样的横图：宽 ≥ 360、高 ≥ 160、宽高比 1.2～2.6（方图多是 logo）。"""
    r = urllib.request.urlopen(urllib.request.Request(u, headers={**UA, "Accept": "image/avif,image/webp,image/*,*/*;q=0.8"}), timeout=10)
    sz = img_size(r.read(262144))
    return bool(sz) and sz[0] >= 360 and sz[1] >= 160 and 1.2 <= sz[0] / sz[1] <= 2.6

def page_imgs(url):
    """候选图：og:image / twitter:image 优先，其次正文里的 <img>。"""
    s = fetch(url, timeout=12)[:600000].decode("utf-8", "ignore")
    out = []
    for m in re.findall(r"<meta[^>]+>", s, re.I):
        if re.search(r"""(?:property|name|itemprop)\s*=\s*["'](?:og:image|twitter:image(?::src)?|image)["']""", m, re.I):
            c = re.search(r"""content\s*=\s*["']([^"']+)""", m, re.I)
            if c:
                out.append(c[1])
    body = s[s.find("<body"):] if "<body" in s else s
    for m in re.findall(r"<img\b[^>]*>", body, re.I | re.S)[:40]:
        c = re.search(r"""(?:data-original|data-src|src)\s*=\s*["']([^"']+)""", m, re.I)
        if c:
            out.append(c[1])
    res = []
    for u in out:
        u = urllib.parse.urljoin(url, html.unescape(u.strip()))
        if u.startswith("http://"):
            u = "https://" + u[7:]                # GitHub Pages 是 https，http 图会被拦
        if u.startswith("https://") and u not in res and not bad_img(u):
            res.append(u)
    return res[:6]

def cover_of(art_url):
    """返回 (图片地址, 原文地址)；没找到返回 (None, 原文地址)；Google 链接解析失败抛异常（下次再试）。"""
    real = gnews_real(art_url) if "news.google.com" in art_url else art_url
    if not real:
        raise ValueError("gnews decode failed")
    for u in page_imgs(real):
        try:
            if good_img(u):
                return u, real
        except Exception:
            pass
    return None, real

def h8(url):
    return hashlib.sha1(url.encode()).hexdigest()[:8]

def covers(items, prev):
    """给新规配图：已有 img 的沿用；没有的从报道原文里找，试过的文章（URL 哈希）记在 cover_tried 里不重复抓。"""
    old = {i["id"]: i for i in prev.get("items") or [] if i.get("img") and not bad_img(i["img"])}
    tried = {k: set(v) for k, v in (prev.get("cover_tried") or {}).items()}
    for i in items:
        o = old.get(i["id"])
        if not i.get("img") and o:
            i["img"], i["img_from"] = o["img"], o.get("img_from", "")
    todo = [i for i in items if i["kind"] != "std" and not i.get("img") and i["articles"]
            and any(h8(a["url"]) not in tried.get(i["id"], ()) for a in i["articles"])]
    nxt = (MONTH_START + timedelta(days=32)).replace(day=1).isoformat()[:7]
    todo.sort(key=lambda i: (i["date"][:7] != nxt, i["date"] < TODAY.isoformat(), -len(i["articles"])))
    deadline = time.time() + COVER_BUDGET
    t0, found = time.time(), [0]

    def one(i):
        done = tried.setdefault(i["id"], set())
        for a in [a for a in i["articles"] if h8(a["url"]) not in done][:COVER_TRIES]:
            if time.time() > deadline:
                return
            try:
                img, real = cover_of(a["url"])
            except Exception:
                continue                           # 网络 / 解析失败，下次再试
            done.add(h8(a["url"]))
            if img:
                i["img"], i["img_from"] = img, real
                found[0] += 1
                return

    with ThreadPoolExecutor(4) as ex:
        list(ex.map(one, todo))
    # 同一张图出自两篇以上不同的文章，多半是站点默认分享图（同一篇文章配给两条新规不算）
    pages = {}
    for i in items:
        if i.get("img"):
            pages.setdefault(i["img"], set()).add(i.get("img_from", ""))
    for i in items:
        if i.get("img") and len(pages[i["img"]]) >= 2:
            i.pop("img"), i.pop("img_from", None)
    ids = {i["id"] for i in items}
    n = sum(1 for i in items if i.get("img"))
    print(f"  ✓ 配图         {n:>4}（本次尝试 {len(todo)} 条，新找到 {found[0]} 张，用时 {time.time() - t0:.0f}s）")
    return {k: sorted(v) for k, v in tried.items() if k in ids and v}

# ── 主流程 ────────────────────────────────────────────────────────────────────

def main():
    prev = {}
    if OUT.exists():
        try:
            prev = json.loads(OUT.read_text("utf-8"))
        except Exception:
            prev = {}
    prev_items = prev.get("items") or []

    print(f"每月新规 · {TODAY}")
    with ThreadPoolExecutor(3) as ex:
        f_law = ex.submit(run, "法律法规", laws)
        f_std = ex.submit(run, "强制性国标", standards)
        f_new = ex.submit(run, "新闻", lambda: news([i for i in prev_items if i["kind"] == "news"]))
    res = {"law": f_law.result(), "std": f_std.result(), "news": f_new.result()}

    sources, val = {}, {}
    for k, (data, err) in res.items():
        old = [i for i in prev_items if i["kind"] == k and in_window(date.fromisoformat(i["date"]))]
        stale = data is None
        val[k] = old if stale else data
        sources[k] = {"ok": not stale, "stale": stale and bool(old), "error": err, "count": len(val[k])}

    items = merge(val["law"], val["std"], val["news"])
    try:
        cover_tried = covers(items, prev)
    except Exception as e:                         # 配图失败不影响主数据
        print(f"  ✗ 配图 {type(e).__name__}: {e}")
        cover_tried = prev.get("cover_tried") or {}
    data = {
        "updated": datetime.now(CST).isoformat(timespec="seconds"),
        "today": TODAY.isoformat(),
        "sources": sources,
        "tags": list(TAGS),
        "items": items,
        "roundups": news.roundups or prev.get("roundups") or [],
        "cover_tried": cover_tried,
    }
    OUT.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), "utf-8")
    print(f"→ data.json {OUT.stat().st_size / 1024:.0f} KB，{len(items)} 条")

if __name__ == "__main__":
    main()
