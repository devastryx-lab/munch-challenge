from typing import List
from uuid import uuid4

from api2 import API2

# Creating this file as it was clearly written in instructions not to edit the source API files


class API3(API2):
    def bulk_create(self, data: List[dict]):
        """Store multiple objects."""
        self._maybe_crash()
        new_objs = [{**obj, "id": uuid4()} for obj in data]
        # there was a bug in the source code which was adding the objects to storage without UUID
        self._storage.update({obj["id"]: obj for obj in new_objs})
        return new_objs
