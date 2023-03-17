from view.utils import timer

import os
import json
import logging
import asyncio
import datetime

import aiohttp
import pandas as pd
from openpyxl import load_workbook

from pydantic import BaseModel

import datetime

logger = logging.getLogger(__name__)

class ItemParams(BaseModel):
    link: str
    itemid: str
    shopid: int
    catid: int
    name: str
    currency: str
    stock: int
    status: int
    ctime: int
    t_ctime: str
    # sold: int
    historical_sold: int
    liked_count: int
    # brand: str
    cmt_count: int
    item_status: str
    price: int
    price_min: int
    price_max: int
    price_before_discount: int
    show_discount: int
    raw_discount: int
    tier_variations_option: str
    rating_star_avg: int
    rating_star_1: int
    rating_star_2: int
    rating_star_3: int
    rating_star_4: int
    rating_star_5: int
    item_type: int
    # is_adult: bool
    # has_lowest_price_guarantee: bool
    # is_official_shop: bool
    # is_cc_installment_payment_eligible: bool
    # is_non_cc_installment_payment_eligible: bool
    # is_preferred_plus_seller: bool
    # is_mart: bool
    # is_on_flash_sale: bool
    # is_service_by_shopee: bool
    # shopee_verified: bool
    # show_official_shop_label: bool
    # show_shopee_verified_label: bool
    # show_official_shop_label_in_title: bool
    # show_free_shipping: bool
    # insert_date: str

    class Config:
        allow_extra = False


class ProductDetailCrawler:
    def __init__(self):
        self.basepath = os.path.abspath(os.path.dirname(__file__))

        self.search_item_api = "https://shopee.co.th/api/v4/shop/search_items"
        self.items_list = []

        today = datetime.datetime.now()
        self.today_date = today.strftime("%Y-%m-%d %H:%M:%S")

    @timer
    def __call__(self, shop_detail):
        async def parser_shop_html(html):
            info = json.loads(html)

            if info["total_count"] != 0:

                for item in info["items"]:
                    item = item["item_basic"]

                    dateArray = datetime.datetime.utcfromtimestamp(item["ctime"])
                    transfor_time = dateArray.strftime("%Y-%m-%d %H:%M:%S")
                    item["price"] = item["price"]/100000
                    item["price_min"] = item["price_min"]/100000
                    item["price_max"] = item["price_max"]/100000
                    item["price_before_discount"] = item["price_before_discount"]/100000
                    item_info = ItemParams(
                        **item,
                        link=f"https://shopee.co.th/{item['name']}-i.{item['shopid']}.{item['itemid']}",
                        t_ctime=transfor_time,
                        insert_date=self.today_date,
                        rating_star_avg=item["item_rating"]["rating_star"],
                        rating_star_1=item["item_rating"]["rating_count"][1],
                        rating_star_2=item["item_rating"]["rating_count"][2],
                        rating_star_3=item["item_rating"]["rating_count"][3],
                        rating_star_4=item["item_rating"]["rating_count"][4],
                        rating_star_5=item["item_rating"]["rating_count"][5],
                        tier_variations_option=",".join(
                            item["tier_variations"][0]["options"]
                        )
                        if item.get("tier_variations")
                        else "",
                    )
                    self.items_list.append(item_info.dict())

        async def get_item_detail(client, query_url):
            try:
                async with client.get(query_url) as response:
                    html = await response.text()
                    assert response.status == 200
                    await parser_shop_html(html)
            except Exception as e:
                logger.warning(f"Exception: {e}")

        async def main(crawler_itme_urls):
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
                "referer": "https://shopee.co.th/",
                "X-Requested-With": "XMLHttpRequest",
                "cookie": "REC_T_ID=25ad7882-c0ac-11ed-94ae-f898efc71612; SPC_F=kfYIY6X7s82zWvXPbrLPkbDkCZzBA2uJ; __LOCALE__null=TH; _gcl_au=1.1.1467039043.1678864569; _fbp=fb.2.1678864569081.675076186; csrftoken=U0cWcwWjBTUwX2nftX8xxoER0OszglAh; _QPWSDCXHZQA=f4c0ae4d-bebf-4b09-cf0d-766c9ffb6259; SPC_SI=GzoQZAAAAABDSTcxZHlraB3CHgAAAAAAcVB1OXgyRTQ=; _gid=GA1.3.1878277686.1678864570; language=en; _med=refer; SPC_CLIENTID=a2ZZSVk2WDdzODJ6zitlgagripicfcsn; SPC_ST=.M1dlMEpHQ0pCYzNzTmFNWKaoA8eYimGZehvoyFQjkz+n18LY7DfJRtuq5BGFu0BekVBfZeiWHUsZNZGMJwdGfq52jf8OLjZhYKLwOJO6DCisGL4/L8n5d/e+iHojvIwInYqizeoc2AjPH3+XLZY7jy4XAOgUJhxH6QUBX8QTpYp4arlUOzsr/MqoNnKjgOE/pAJvj1eWpJffqPumLBJL0w==; SPC_U=969871636; SPC_T_ID=WJOeWA41czCW/fC7zkUPkgwzmzeD/IwS8CfEDmlnmQEuasvOdyatIE2JnQyPM8YjtnE9oTZk+F+JVP/5qdve2INWpLWcHZOjq2myTHRC673C4TL+uyUtyld1lB8nLRGCj8rlueOnby0SMYPJM6x/SnreN91XnvDFWURSWRU+Th0=; SPC_T_IV=d2c3T2JRNWpmZ2VXUkhjUA==; SPC_R_T_ID=WJOeWA41czCW/fC7zkUPkgwzmzeD/IwS8CfEDmlnmQEuasvOdyatIE2JnQyPM8YjtnE9oTZk+F+JVP/5qdve2INWpLWcHZOjq2myTHRC673C4TL+uyUtyld1lB8nLRGCj8rlueOnby0SMYPJM6x/SnreN91XnvDFWURSWRU+Th0=; SPC_R_T_IV=d2c3T2JRNWpmZ2VXUkhjUA==; AMP_TOKEN=%24NOT_FOUND; _ga=GA1.1.310384165.1678864570; shopee_webUnique_ccd=MgTzBPggpeyJGPMb0AZGjg%3D%3D%7C1VpxUy6Pbhne10ot2IaOff8hBAF21D51VV1kW97jiSIEJywK2WkOb1qCqmFCkJ1bvaOPVVYdQIN60wT6kUW0x7OPw2LZadQDtbM%3D%7CKHCVjYf%2FVLPDHHV1%7C06%7C3; ds=6bc0cbab1c7a72d5a8188035da0cbd2b; _ga_L4QXS6R7YG=GS1.1.1679048940.8.1.1679049499.44.0.0; SPC_EC=SVA5b3lKcHRtelJQNUNsVKnLrwdZGW5jQt+zXqQY9nizYWzdWZB9Qs1B9ft8owpucbrVUr7qiu54cGwCSMb6qH+AfocJaro87NfjgAK7VbL0BK6J5xKpbrNsapRKF+cgHt7mwvU49IX/ijeleQ8YUrjeHIbja2xcHgHi/MKqFEc="
            }
            async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=False, limit=10),
                headers=headers,
            ) as client:
                tasks = [
                    get_item_detail(client, query_url)
                    for query_url in crawler_itme_urls
                ]
                await asyncio.gather(*tasks)

        df_header = pd.DataFrame(
            columns=[field.name for field in ItemParams.__fields__.values()]
        )
        df_header.to_csv(self.basepath + "/csv/pdp_detail.csv", index=False)

        for row in shop_detail.itertuples():
            crawler_itme_urls = []

            shop_id = row.shopid
            shop_product_count = row.item_count
            num = 0
            while num < shop_product_count:
                crawler_itme_urls.append(
                    f"{self.search_item_api}?offset={str(num)}&limit=100&order=desc&filter_sold_out=3&use_case=1&sort_by=sales&order=sales&shopid={shop_id}"
                )
                num += 100
            asyncio.run(main(crawler_itme_urls))

            logger.info(f"└── add Product Page Detail: {shop_product_count} {shop_id}")
        df = pd.DataFrame(self.items_list)
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")

        df.to_csv(
            self.basepath + "/csv/pdp_detail"+current_time+".csv",
            index=False,
            mode="a",
            header=False,
        )

        # create a Pandas Excel writer using xlsxwriter engine
        writer = pd.ExcelWriter("/Users/hungnguyen/Library/CloudStorage/GoogleDrive-thanghungkhi@gmail.com/My Drive/Ecom Data/data.xlsx", mode="a", engine='openpyxl')

        # write the DataFrame to a new sheet in the Excel file
        df.to_excel(writer, sheet_name=current_time)

        # save the Excel file and close the writer
        writer.save()

        return df


if __name__ == "__main__":
    """
    # api example
    # https://shopee.tw/api/v4/shop/search_items?filter_sold_out=1&limit=100&offset=1&order=desc&shopid=5547415&sort_by=pop&use_case=1

    params use_case:
    1: Top Product
    2: ?
    3: ?
    4: Sold out items

    params filter_sold_out:
    1: = sold_out
    2: != sold_out
    3: both

    """

    basepath = os.path.abspath(os.path.dirname(__file__))
    shop_detail = pd.read_csv(basepath + "/csv/shop_detail.csv")
    crawler_product_detail = ProductDetailCrawler()
    result_product_detail = crawler_product_detail(shop_detail)
