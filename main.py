from config.config import settings
from view.utils import timer
from view.check_ip_pool import CheckIPAddress
from view.api_v4_get_shop_detail import ShopDetailCrawler
from view.api_v4_get_product_detail import ProductDetailCrawler
from view.get_shop_name_from_json import get_values
import json

import logging
import schedule
import time

logger = logging.getLogger(__name__)


class Crawler:
    def __init__(self, user_dict):
        self.input_shop_names = user_dict["input_shop_names"]
        self.user_email = user_dict["user_info"]["Email"]
        self.user_name = user_dict["user_info"]["Name"]

    @timer
    def __call__(self):

        # Step 0 > check ip pool as expected (This step is not necessary.)
        logger.info(f"⌲ Step 0: Test the IP you're using 5 times.")
        self.check_ip_pool()

        # Step 1 > input shop_names > get shop_detail
        logger.info(f"⌲ Step 1: Total shop detail fetchedd:")
        crawler_shop_detail = ShopDetailCrawler()
        result_shop_detail = crawler_shop_detail(self.input_shop_names)

        # Step 2 > input shop_detail > get product_id
        logger.info(f"⌲ Step 2: Total pdp detail fetched:")
        crawler_product_detail = ProductDetailCrawler()
        result_product_detail = crawler_product_detail(result_shop_detail)
        result_product_detail["user_name"] = self.user_name
        result_product_detail["user_email"] = self.user_email

        # Step 3 > save shop & pdp data to the Bigquery
        if settings.ENV == "prod":
            logger.info(f"⌲ Step 3: Data saved to BigQuery.")
            self.save_to_bigquery(result_shop_detail, result_product_detail)

    def check_ip_pool(self):
        check_ip = CheckIPAddress()
        check_ip(test_times=5)

    def save_to_bigquery(self, shop_details, product_details):

        client = settings.setup_bigquery()
        shop_details.to_gbq("shopee.shop_detail", client.project, if_exists="append")
        product_details.to_gbq("shopee.pdp_detail", client.project, if_exists="append")


if __name__ == "__main__":
    # Load JSON file into a Python object
    with open('view/Shops.json', 'r') as f:
        json_obj = json.load(f)
    # Insert your email and the shop names you want to crawl
    user_list = [
        {
            "user_info": {
                "Email": "thanghungkhi@gmail.com",
                "Name": "Hung",
            },
            # "input_shop_names": get_values(json_obj, 'username'),
            "input_shop_names": ['kissmybody_official'],
        }
    ]

    # do = Crawler(user_list[0])

    # do()
    schedule.every().hour.do(Crawler(user_list[0]))

    while True:
        schedule.run_pending()
        time.sleep(5)
