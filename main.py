import asyncio
import pandas as pd
import random
import os
import re
import json
from playwright.async_api import async_playwright
# --- ПОВНИЙ СПИСОК КАТЕГОРІЙ ---
CATEGORIES = [
    "Солодощі десерти шоколад печиво",
    "Кава чай матча какао",
    "Косметика доглядова креми маски",
    "Декоративна косметика помади туш",
    "Зоотовари корми аксесуари",
    "Корисні снеки протеїн здорове харчування",
    "Товари для дому органайзери кухня",
    "Аксесуари біжутерія сумки окуляри",
    "Одяг базовий локальні бренди",
    "Дитячі товари іграшки розвивашки",
    "Свічки аромати для дому дифузори",
    "БАДи вітаміни",
    "Побутова техніка гаджети",
    "Салони краси барбершопи кафе",
    "Онлайн-сервіси курси підписки"
]

# --- ГЕОГРАФІЯ (Обласні та великі міста) ---
CITIES = [
    "Київ", "Львів", "Одеса", "Дніпро", "Харків", "Запоріжжя", "Івано-Франківськ",
    "Вінниця", "Луцьк", "Рівне", "Тернопіль", "Хмельницький", "Житомир",
    "Чернігів", "Черкаси", "Полтава", "Кропивницький", "Миколаїв", "Чернівці",
    "Ужгород", "Кривий Ріг", "Кременчук", "Біла Церква", "Бровари"
]

OUTPUT_FILE = "final_ukraine_base.xlsx"


PROGRESS_FILE = "progress.json"

NIGHT_MODE = True
def delay_short():
    return random.randint(12, 25) if NIGHT_MODE else random.randint(5, 12)

def delay_city():
    return random.randint(40, 80) if NIGHT_MODE else random.randint(10, 25)

# ---------------- PROGRESS ----------------
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            return json.load(open(PROGRESS_FILE, "r", encoding="utf-8"))
        except:
            return {}
    return {}

def save_progress(data):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ---------------- SAVE ----------------
def save_to_excel(data):
    if not data:
        return 0

    print(f"\n💾 СОХРАНЯЮ {len(data)} записей...")

    df_new = pd.DataFrame(data)

    if os.path.exists(OUTPUT_FILE):
        try:
            df_old = pd.read_excel(OUTPUT_FILE)
            df = pd.concat([df_old, df_new], ignore_index=True)
            df.drop_duplicates(subset=["url"], inplace=True)
        except:
            df = df_new
    else:
        df = df_new

    df.to_excel(OUTPUT_FILE, index=False)
    print(f"✅ В БАЗЕ: {len(df)} записей\n")
    return len(df)

# ---------------- CLEAN ----------------
def clean_name(name):
    if not name:
        return ""
    bad = ["результат", "google", "maps"]
    if any(x in name.lower() for x in bad):
        return ""
    return name.strip()

# ---------------- NAME ----------------
async def get_name(page):
    try:
        el = await page.query_selector("h1.DUwDvf")
        if el:
            return (await el.inner_text()).strip()
    except:
        pass

    try:
        el = await page.query_selector("h1")
        if el:
            return (await el.inner_text()).strip()
    except:
        pass

    return ""

# ---------------- SCRAPE ----------------
async def scrape_maps(page, cat, city):
    print(f"\n🔎 {city} | {cat}")

    url = f"https://www.google.com/maps/search/{cat}+{city}"

    for i in range(3):
        try:
            print(f"🌐 загрузка ({i+1})")
            await page.goto(url, timeout=60000)
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(4000)
            break
        except:
            await asyncio.sleep(5)

    feed = 'div[role="feed"]'

    last = 0
    for i in range(6):
        await page.evaluate("""
            const el = document.querySelector('div[role="feed"]');
            if (el) el.scrollBy(0, el.scrollHeight);
        """)
        await page.wait_for_timeout(2500)

        items = await page.query_selector_all(f'{feed} a[href*="/maps/place/"]')
        print(f"📜 scroll {i+1}: {len(items)}")

        if len(items) == last:
            break
        last = len(items)

    items = await page.query_selector_all(f'{feed} a[href*="/maps/place/"]')

    print(f"📦 найдено карточек: {len(items)}")

    results = []

    for i, item in enumerate(items):
        try:
            print(f"➡️ {i+1}/{len(items)}")

            await item.click()
            await page.wait_for_timeout(random.randint(2000, 3500))

            name = clean_name(await get_name(page))

            if not name:
                print("⚠️ пропуск (нет имени)")
                continue

            html = await page.content()

            phone = ""
            address = ""
            site = ""
            email = ""

            try:
                btn = await page.query_selector('button[data-item-id^="phone:tel:"]')
                if btn:
                    phone = (await btn.get_attribute("data-item-id")).replace("phone:tel:", "")
            except:
                pass

            try:
                addr = await page.query_selector('button[data-item-id="address"]')
                if addr:
                    address = await addr.inner_text()
            except:
                pass

            try:
                site_el = await page.query_selector('a[data-item-id="authority"]')
                if site_el:
                    site = await site_el.get_attribute("href")
            except:
                pass

            emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', html)
            if emails:
                email = emails[0]

            url_item = await item.get_attribute("href")

            results.append({
                "category": cat,
                "city": city,
                "name": name,
                "phone": phone,
                "email": email,
                "site": site,
                "address": address,
                "url": url_item
            })

            print(f"✅ {name}")

        except Exception as e:
            print(f"❌ error: {e}")

    print(f"📊 собрано: {len(results)}")
    return results

# ---------------- MAIN ----------------
async def main():

    buffer = []
    seen = set()
    progress = load_progress()

    async with async_playwright() as p:

        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--start-maximized"
            ]
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="ru-RU"
        )

        page = await context.new_page()

        # 🔥 анти-детект
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        """)

        if os.path.exists(OUTPUT_FILE):
            try:
                df = pd.read_excel(OUTPUT_FILE)
                seen = set(df["url"].dropna().tolist())
                print(f"📂 база: {len(seen)}")
            except:
                pass

        while True:
            for city in CITIES:
                print(f"\n🌍 ===== {city} =====")

                for cat in CATEGORIES:

                    key = f"{city}|{cat}"
                    if progress.get(key):
                        continue

                    data = await scrape_maps(page, cat, city)

                    for item in data:
                        if item["url"] not in seen:
                            seen.add(item["url"])
                            buffer.append(item)

                    progress[key] = True
                    save_progress(progress)

                    print(f"📥 buffer: {len(buffer)}")

                    if len(buffer) >= 50:
                        save_to_excel(buffer)
                        buffer.clear()

                    await asyncio.sleep(delay_short())

                await asyncio.sleep(delay_city())

            print("🔁 цикл завершён → reset progress")
            progress = {}
            save_progress(progress)

if __name__ == "__main__":
    asyncio.run(main())