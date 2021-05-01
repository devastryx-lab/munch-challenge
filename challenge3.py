import json
import os.path
from collections import OrderedDict
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
        self.api_client = API3()
        self.pre_processed_nodes = OrderedDict()
        self.pre_processed_product_groups_filename = "pre_processed_product_groups.json"
        self.state_file_name = 'state_file.txt'
        self.batch_size = 5 # can be accepted as an argument later on

    def find_ancestors(self, node: Node, ancestors: Tuple) -> None:
        """
        Recursively find ancestors
        Args:
            node: node being processed
            ancestors: list of ancestors of the current Node
        """
        self.pre_processed_nodes[node.id] = {
            'id' : node.id,
            "name": node.name,
            "parent_id": node.parent.id if node.parent else None,
            "ancestors": list(ancestors),
        }
        ancestors += (node.name,)
        for child in node.children:
            self.find_ancestors(child, ancestors)

    def pre_process_nodes(self):
        """
        Preprocess source nodes to find the ancestors and save then in an external data source
        """
        for root in self.tree.root_nodes:
            self.find_ancestors(root, ())
        with open(self.pre_processed_product_groups_filename, "w+") as f:
            json.dump(self.pre_processed_nodes, f, ensure_ascii=False, indent=4)

    def process_source_file(self) -> None:
        """
        Read source file and create a Tree from the data
        """
        if os.path.exists(self.pre_processed_product_groups_filename):
            return
        try:
            with open(self.source_file_name, "rb") as f:
                data = json.load(f)
        except JSONDecodeError:
            raise ValueError("The input source file does not have a valid JSON")
        else:
            for row in data:
                self.tree.push(row)
            self.pre_process_nodes()

    def load_data_from_external_source(self) -> None:
        """
        Load the data from external source into our system using API1
        """
        self.process_source_file()
        try:
            with open(self.pre_processed_product_groups_filename, "rb") as f:
                data = json.load(f)
        except JSONDecodeError:
            raise ValueError("The input source file does not have a valid JSON")
        else:
            last_processing_record = None
            if os.path.exists(self.state_file_name):
                with open(self.state_file_name, 'r') as f:
                    last_processing_record = int(f.readlines()[0])
            total = len(data.keys())
            for start in range(0, total, self.batch_size):
                end = min(start + self.batch_size, total)
                products = list(data.values())[start:end]
                if not products:
                    continue
                if last_processing_record:
                    if int(products[0]['id']) < last_processing_record:
                        continue
                with open(self.state_file_name, 'w+') as f:
                    f.write(f'{products[0]["id"]}')
                self.api_client.bulk_create(products)
