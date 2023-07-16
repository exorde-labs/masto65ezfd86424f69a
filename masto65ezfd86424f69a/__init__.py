import random
import aiohttp
from datetime import datetime as datett
from datetime import timedelta, timezone
from typing import AsyncGenerator
from bs4 import BeautifulSoup
from urllib.parse import urlencode
import re
import logging

from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
    ExternalId,
    ExternalParentId,
)

################################
default_mastodon_endpoint = 'https://mastodon.social/api/v1/timelines/tag/'
# default values
DEFAULT_OLDNESS_SECONDS = 120
DEFAULT_MAXIMUM_ITEMS = 25
DEFAULT_MIN_POST_LENGTH = 10
NB_SPECIAL_CHECKS = 2
DEFAULT_KEYWORDS = \
["news", "news", "press", "silentsunday", "saturday", "monday", "tuesday" "bitcoin", "ethereum", "eth", "btc", "usdt", "cryptocurrency", "solana",
"doge", "cardano", "monero", "polkadot", "ripple", "xrp", "stablecoin", "defi", "cbdc", "nasdaq", "sp500",  "BNB", "ETF", "SpotETF", "iphone", "it",
"usbc", "eu", "hack", "hacker", "hackers", "virtualreality", "metaverse", "tech", "technology", "art", "game", "trading", "groundnews", "breakingnews",
"Gensler", "FED", "SEC", "IMF", "Macron", "Biden", "Putin", "Zelensky", "Trump", "legal", "bitcoiners", "bitcoincash", "ethtrading", "cryptonews",
"cryptomarket", "cryptoart", "CPTPP", "brexit", "trade", "economy", "USpolitics", "UKpolitics", "NHL", "computer", "computerscience", "stem", "gpt4",
"billgates", "ai", "chatgpt", "openai", "wissen", "french", "meat", "support", "aid", "mutualaid", "mastodon", "bluesky", "animal", "animalrights",
"BitcoinETF", "Crypto", "altcoin", "DeFi", "GameFi", "web3", "web3", "trade",  "NFT", "NFTs", "cryptocurrencies", "Cryptos", "reddit", "elonmusk",
"politics", "business", "twitter", "digital", "airdrop", "gamestop", "finance", "liquidity","token", "economy", "markets", "stocks", "crisis", "gpt", "gpt3",
"russia", "war", "ukraine", "luxury", "LVMH", "Elonmusk", "conflict", "bank", "Gensler", "emeutes", "FaceID", "Riot", "riots", "Franceriot", "France",
"UnitedStates", "USA", "China", "Germany", "Europe", "Canada", "Mexico", "Brazil", "price", "market", "NYSE","NASDAQ", "CAC", "CAC40", "G20", "OilPrice", 
"FTSE", "NYSE", "WallStreet", "money", "forex", "trading", "currency", "USD", "WarrenBuffett", "BlackRock", "Berkshire", "IPO", "Apple", "Tesla","Alphabet",
 "FBstock","debt", "bonds", "XAUUSD", "SP500", "DowJones", "satoshi", "shorts", "live", "algotrading", "tradingalgoritmico", "prorealtime", "ig", "igmarkets", 
 "win", "trading", "trader", "algorithm", "cfdauto", "algos", "bottrading", "tradingrobot", "robottrading", "prorealtimetrading", "algorithmictrading",
"usa ", "canada ", "denmark", "russia", "japan", "italy", "spain", "uk", "eu", "social", "iran", "war","socialism", "Biden", "democracy", "justice", "canada", "leftist",
"election", "vote", "protocol", "network", "org", "organization", "charity", "money", "scam", "token", "tokens", "ecosystem",
"rightwing",  "DAX", "NASDAQ", "RUSSELL", "RUSSELL2000", "GOLD", "XAUUSD", "DAX40", "IBEX", "IBEX35", "oil", "crude", "crudeoil", "us500", "russell", "russell2000"]

def is_within_timeframe_seconds(dt_str, timeframe_sec):
    # Convert the datetime string to a datetime object
    dt = datett.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ")

    # Make it aware about timezone (UTC)
    dt = dt.replace(tzinfo=timezone.utc)

    # Get the current datetime in UTC
    current_dt = datett.now(timezone.utc)

    # Calculate the time difference between the two datetimes
    time_diff = current_dt - dt

    # Check if the time difference is within the specified timeframe in seconds
    if abs(time_diff) <= timedelta(seconds=timeframe_sec):
        return True
    else:
        return False

params = {
    'limit': 50
}

def parse_mastodon_post(post_data):
    # Extracting the URL of the post
    url = post_data['url']

    # Extracting the datetime of the post
    created_at = post_data['created_at']

    # Extracting the hashtags
    hashtags = [tag['name'] for tag in post_data['tags']]

    # Extracting the ID of the post
    post_id = post_data['id']

    # Extracting the parent ID if it exists
    parent_id = post_data['in_reply_to_id']

    # Extracting the language of the post
    language = post_data['language']

    # Extracting the text content of the post stripped from HTML
    content = post_data['content']
    soup = BeautifulSoup(content, 'html.parser')
    text_content = soup.get_text(separator=' ')

    return {
        'url': url,
        'created_at': created_at,
        'external_id': post_id,
        'external_parent_id': parent_id,
        'language': language,
        'content': text_content+","+" ".join(hashtags)
    }


async def scrape_mastodon_hashtag(hashtag, max_oldness_seconds, min_post_length):
    results = []
    consecutive_old_posts = 0
    N_OLD = 3
    URL = default_mastodon_endpoint + hashtag


    for i in range(2):
        async with aiohttp.ClientSession() as session:
            url = URL + '?' + urlencode(params)
            async with session.get(url) as response:
                toots = await response.json()

        if len(toots) == 0:
            break

        for t in toots:
            toot = parse_mastodon_post(t)
            created_at = datett.strptime(toot['created_at'], '%Y-%m-%dT%H:%M:%S.%fZ')
            current_time = datett.utcnow()

            # Check if the post is within the specified timeframe
            if (current_time - created_at).total_seconds() > max_oldness_seconds:
                continue

            # Check if the post meets the minimum length requirement
            if len(toot['content']) < min_post_length:
                continue

            results.append(toot)

        max_id = toots[-1]['id']
        URL = default_mastodon_endpoint + hashtag + '?max_id=' + max_id

    return results


def read_parameters(parameters):
    # Check if parameters is not empty or None
    if parameters and isinstance(parameters, dict):
        try:
            max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        except KeyError:
            max_oldness_seconds = DEFAULT_OLDNESS_SECONDS

        try:
            maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        except KeyError:
            maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS

        try:
            min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        except KeyError:
            min_post_length = DEFAULT_MIN_POST_LENGTH

        try:
            special_kw_checks = parameters.get("special_keywords_checks", NB_SPECIAL_CHECKS)
        except KeyError:
            special_kw_checks = NB_SPECIAL_CHECKS
    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH
        special_kw_checks = NB_SPECIAL_CHECKS

    return max_oldness_seconds, maximum_items_to_collect, min_post_length, special_kw_checks

def filter_keyword_for_hashtag(input_string):
    # Remove everything between brackets
    filtered_string = re.sub(r'[\[\(][^\[\]\(\)]*[\]\)]', '', input_string)
    # Remove all spaces and special characters
    filtered_string = re.sub(r'[^a-zA-Z0-9]', '', filtered_string)
    # Lowercase the filtered string
    filtered_string = filtered_string.lower()
    # Randomly include content within parentheses
    paren_content = re.findall(r'\(([^()]+)\)', input_string)
    if random.random() < 0.5 and paren_content != None and len(paren_content)>0:
        if paren_content:
            filtered_string = paren_content[0]
        else:
            filtered_string = ''

async def query(parameters: dict) -> AsyncGenerator[Item, None]:    
    logging.info(f"[Mastodon] Scraping latest posts on Mastodon major instances")
    max_oldness_seconds, maximum_items_to_collect, min_post_length, special_kw_checks = read_parameters(parameters)
    search_keyword = ""
    try:
        if "url_parameters" in parameters and "keyword" in parameters["url_parameters"]:
            search_keyword = parameters["url_parameters"]["keyword"]
        if "keyword" in parameters:
            search_keyword = parameters["keyword"]
        search_keyword = filter_keyword_for_hashtag(search_keyword)
    except Exception as e:
        logging.info(f"[Mastodon parameters] checking url_parameters: %s",(max_oldness_seconds, maximum_items_to_collect, min_post_length, special_kw_checks))    
        logging.exception(f"[Mastodon parameters] Keyword input read failed: {e}")    

    if search_keyword is None or len(search_keyword) < 1:        
        search_keyword = random.choice(DEFAULT_KEYWORDS)
    logging.info(f"[Mastodon] Scraping hashtag {search_keyword} ...")    

    yielded_items = 0  # Counter for the number of yielded items
    already_seen_ids = []
    # 1. first keyword scrape
    scraped_data = await scrape_mastodon_hashtag(search_keyword, max_oldness_seconds, min_post_length)
    for post in scraped_data:
        if  not post["external_id"] in already_seen_ids and is_within_timeframe_seconds(post['created_at'], max_oldness_seconds):
            logging.info(f"[MASTODON] Found toot = {post}")
            yielded_items += 1
            # Yield the formatted item
            yield Item(
                content=Content(post["content"]),
                created_at=CreatedAt(post["created_at"]),
                domain=Domain("mastodon.social"),
                url=Url(post["url"]),
                external_id=ExternalId(post["external_id"]),
                external_parent_id=ExternalParentId(post["external_parent_id"]),
            )
            already_seen_ids.append(post["external_id"])
            if yielded_items >= maximum_items_to_collect:
                break
    
    # 2. special keyword scrapes
    for i in range(special_kw_checks):
        if yielded_items >= maximum_items_to_collect:
            break
        special_hashtag = random.choice(DEFAULT_KEYWORDS)
        logging.info(f"[Mastodon] Scraping hashtag {special_hashtag}...")    
        scraped_data = await scrape_mastodon_hashtag(special_hashtag, max_oldness_seconds, min_post_length)
        for post in scraped_data:
            if  not post["external_id"] in already_seen_ids \
                and is_within_timeframe_seconds(post['created_at'], max_oldness_seconds):
                logging.info(f"[MASTODON] Found extra toot = {post}")
                yielded_items += 1
                # Yield the formatted item
                yield Item(
                    content=Content(post["content"]),
                    created_at=CreatedAt(post["created_at"]),
                    domain=Domain("mastodon.social"),
                    url=Url(post["url"]),
                    external_id=ExternalId(post["external_id"]),
                    external_parent_id=ExternalParentId(post["external_parent_id"]),
                )
                already_seen_ids.append(post["external_id"])
                if yielded_items >= maximum_items_to_collect:
                    break
