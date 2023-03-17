from config.config import settings
from view.utils import timer

import os
import json
import logging
import asyncio
import datetime

import aiohttp
import pandas as pd
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ShopParams(BaseModel):
    shop_created: str
    insert_date: str
    shopid: int
    name: str
    username: str
    follower_count: int
    has_decoration: bool
    item_count: int
    response_rate: int
    campaign_hot_deal_discount_min: int
    rating_star: float
    shop_rating_good: int
    shop_rating_bad: int
    shop_rating_normal: int
    # description: str

    class Config:
        allow_extra = False


class ShopDetailCrawler:
    def __init__(self):
        self.basepath = os.path.abspath(os.path.dirname(__file__))
        self.shop_detail_api = "https://shopee.co.th/api/v4/shop/get_shop_base?entry_point=ShopByPDP&need_cancel_rate=true&request_source=shop_home_page&version=1&username="
        self.shop_detail = []

        today = datetime.datetime.now()
        self.today_date = today.strftime("%Y-%m-%d %H:%M:%S")

    @timer
    def __call__(self, input_shop_names):
        async def parser_shop_html(html):
            shop = json.loads(html)["data"]
            dateArray = datetime.datetime.utcfromtimestamp(shop["ctime"])
            transfor_time = dateArray.strftime("%Y-%m-%d %H:%M:%S")

            shop_info = ShopParams(
                **shop,
                username=shop["account"]["username"],
                shop_created=transfor_time,
                insert_date=self.today_date,
                shop_rating_good=shop["shop_rating"]["rating_good"],
                shop_rating_bad=shop["shop_rating"]["rating_bad"],
                shop_rating_normal=shop["shop_rating"]["rating_normal"],
            )
            self.shop_detail.append(shop_info.dict())
            logger.debug(f"└── add Shop Detail: {shop['name']}")

        async def get_shop_detail(client, query_url):
            try:
                async with client.get(
                    query_url,
                    proxy=settings.PROXY_URL,
                ) as response:
                    html = await response.text()
                    assert response.status == 200
                    await parser_shop_html(html)
            except Exception as e:
                logger.warning(f"Exception: {e}")

        async def main(crawler_shop_urls):
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
                "referer": "https://shopee.co.th/",
                "X-Requested-With": "XMLHttpRequest",
                "cookie": "__LOCALE__null=TH; csrftoken=w0EC8v4SDEWsvObJy44KH8vMG02f5B5R; _gcl_au=1.1.253236186.1678987039; SPC_SI=HToQZAAAAAB2cG05RmFNZO3jTwAAAAAAWThNb3RxS2Y=; _fbp=fb.2.1678987040408.502461095; _QPWSDCXHZQA=0b3c871c-2348-4eac-df8b-e3761f81dbd5; SPC_F=VaTVUKbCpeqhfc1Mmctb6YTCOg1tW9mz; REC_T_ID=695ee964-c41e-11ed-8210-68e209fbba3c; SPC_R_T_ID=b8Dsr6UiBbdVR13N2RWGkP6iUMCqW3LGeo4f9SIG/r1AVj7rvIgXXtPX4Ssw0D/O2LOZt98UnZu5Vo5xgJ7oO98w0t8dgtIxG8ut0TnujhZ/fLnoM/Llr2ce6JoAGzNnhWNkJ304Y3R45N20f5SCI9ZsPxC9nQblRxmDCdL090g=; SPC_R_T_IV=Q05WZUFrU29qQURsY1J0VA==; SPC_T_ID=b8Dsr6UiBbdVR13N2RWGkP6iUMCqW3LGeo4f9SIG/r1AVj7rvIgXXtPX4Ssw0D/O2LOZt98UnZu5Vo5xgJ7oO98w0t8dgtIxG8ut0TnujhZ/fLnoM/Llr2ce6JoAGzNnhWNkJ304Y3R45N20f5SCI9ZsPxC9nQblRxmDCdL090g=; SPC_T_IV=Q05WZUFrU29qQURsY1J0VA==; _gid=GA1.3.961084277.1678987043; language=en; _ga=GA1.1.2019951774.1678987042; _ga_L4QXS6R7YG=GS1.1.1678987042.1.1.1678987515.50.0.0"
            }
            async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=False, limit=100),
                headers=headers,
            ) as client:
                tasks = [
                    get_shop_detail(client, query_url)
                    for query_url in crawler_shop_urls
                ]
                await asyncio.gather(*tasks)

        crawler_shop_urls = []
        for num in range(len(input_shop_names)):
            crawler_shop_urls.append(self.shop_detail_api + str(input_shop_names[num]))
        asyncio.run(main(crawler_shop_urls))

        df = pd.DataFrame(self.shop_detail)
        logger.debug(df)
        df.to_csv(self.basepath + "/csv/shop_detail.csv", index=False)
        logger.debug(df)
        return df


if __name__ == "__main__":
    """
    api example
    https://shopee.tw/api/v4/shop/get_shop_base?entry_point=ShopByPDP&need_cancel_rate=true&request_source=shop_home_page&username=mhw3bombertw&version=1
    """
    input_shop_names = [
        "fulinxuan",
        "pat6116xx",
        "join800127",
        "ginilin0982353562",
        "ru8285fg56",
        "wangshutung",
        "taiwan88888",
        "cyf66666",
        "buddha8888",
        "dragon9168",
        "sinhochen77",
        "baoshenfg",
        "s0985881631",
        "jouhsuansu",
    ]

    do = ShopDetailCrawler()
    result = do(input_shop_names)

    logger.debug(result)
