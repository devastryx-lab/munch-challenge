## Your solution here.
import json
from json.decoder import JSONDecodeError
from typing import Dict, Iterator, List

from api1 import API1


class TransferProductCatalogue:
    def __init__(self, source_file_name: str) -> None:
        self.source_file_name = source_file_name
        self.cache = dict()  # this mimics an actual cache store
        self.ancestors_dp = dict()
        self.api_client = API1()

    def read_source_file(self) -> Iterator[Dict]:
        """
        Read source file and yield products
        Returns:
            Yields product info form source
        """
        try:
            with open(self.source_file_name, "rb") as f:
                # assuming that IDs are auto incremental values, which implies that a child ID will always be
                # greater than the ID of it's ancestors
                data = sorted(json.load(f), key=lambda x: x["id"])
        except JSONDecodeError:
            raise ValueError("The input source file does not have a valid JSON")
        else:
            for row in data:
                yield row

    def find_ancestors(self, parent_id: int, ancestors: List) -> None:
        """
        Find the ancestors of a product as follows:
        * Try to fetch the ancestors of the immediate parent from ancestors_dp, if it has already been
            computed
        * Else, recursively find the ancestors of the parent

        Append immediate parent name to the final list

        Args:
            parent_id: ID of the parent product
            ancestors: list of ancestors
        """
        # every product that has children is saved in cache
        parent_product_info = self.cache.get(parent_id)
        ancestors.append(parent_product_info["name"])
        if parent_of_parent := parent_product_info.get("parent_id"):
            # check if the ancestors of parent have previously been computed
            if ancestors_of_parent := self.ancestors_dp.get(parent_id):
                ancestors.extend(ancestors_of_parent)
            else:
                ancestors_of_parent = list()
                self.find_ancestors(parent_of_parent, ancestors_of_parent)
                self.ancestors_dp[parent_id] = ancestors_of_parent
                ancestors.extend(ancestors_of_parent)

    def load_data_from_external_source(self):
        """
        Load the data from external source into our system using API1
        """
        for product in self.read_source_file():
            # cache the product if it has any children
            if product.pop("children_ids", None):
                self.cache[product["id"]] = product
            # find ancestors
            ancestors = list()
            if parent_id := product.get("parent_id"):
                self.find_ancestors(parent_id, ancestors)
            product["ancestors"] = ancestors
            # create
            self.api_client.create(product)
