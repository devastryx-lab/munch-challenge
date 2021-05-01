import json
from json.decoder import JSONDecodeError
from typing import List, Dict, Tuple

from api3_fix import API3


class Node:
    """
    Class representing a Product node
    """

    def __init__(self, product_id):
        self.id = product_id
        self.children = list()
        self.parent = None
        self.name = None


class Tree:
    """
    Class representing a multi tree of the product catalogue from source
    """

    def __init__(self):
        self.nodes = dict()

    def push(self, product: Dict) -> None:
        """
        Push product node into the tree at the correct position
        Args:
            product: details of the product
        """
        parent_id = product["parent_id"]
        product_id = product["id"]
        if product_id not in self.nodes:
            self.nodes[product_id] = Node(product_id)
        self.nodes[product_id].name = product["name"]
        if parent_id:
            if parent_id not in self.nodes:
                self.nodes[parent_id] = Node(parent_id)
            self.nodes[product_id].parent = self.nodes[parent_id]
            self.nodes[parent_id].children.append(self.nodes[product_id])

    @property
    def root_nodes(self) -> List[Node]:
        """
        List all root nodes

        Returns:
            list of all root nodes
        """
        return list(node for node in self.nodes.values() if node.parent is None)


class TransferProductCatalogue:
    def __init__(self, source_file_name: str) -> None:
        self.source_file_name = source_file_name
        self.tree = Tree()
        self.batch_size = 5  # later can be accepted as an argument
        self.batch_of_product_info = list()
        self.api_client = API3()

    def read_source_file(self) -> None:
        """
        Read source file and create a Tree from the data
        """
        try:
            with open(self.source_file_name, "rb") as f:
                data = json.load(f)
        except JSONDecodeError:
            raise ValueError("The input source file does not have a valid JSON")
        else:
            for row in data:
                self.tree.push(row)

    def create_data_in_batch(self, product_info: Dict) -> None:
        """
        Create product in batches
        Args:
            product_info: product info to be added to the storage
        """
        if len(self.batch_of_product_info) == self.batch_size:
            self.api_client.bulk_create(self.batch_of_product_info)
            self.batch_of_product_info = list()
        self.batch_of_product_info.append(product_info)

    def create_node_in_datastore(self, node: Node, ancestors: Tuple) -> None:
        """
        Create node in the datastore and add all its children
        Args:
            node: Node to be inserted in the database
            ancestors: list of ancestors
        """
        parent_id = node.parent.id if node.parent else None
        product_info = {"name": node.name, "parent_id": parent_id, "ancestors": list(ancestors)}
        self.create_data_in_batch(product_info)
        ancestors += (node.name,)  # we want to use an immutable data type to hold ancestors
        for child in node.children:
            self.create_node_in_datastore(child, ancestors)

    def load_data_from_external_source(self) -> None:
        """
        Load the data from external source into our system using API1
        """
        self.read_source_file()
        for root in self.tree.root_nodes:
            self.create_node_in_datastore(root, ())
        # a case when the last batch had 4 or more products
        if self.batch_of_product_info:
            self.api_client.bulk_create(self.batch_of_product_info)
