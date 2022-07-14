from typing import Dict, List

from approaches.approach import Approach
from approaches.sage import SaGe
from approaches.sage_topk import SaGeTopK
from approaches.sage_topk_collab import SaGeTopKCollab
from approaches.virtuoso import Virtuoso


class ApproachFactory():

    @staticmethod
    def types() -> List[str]:
        return [
            "sage",
            "sage-topk",
            "sage-topk-collab",
            "sage-topk-collab-0.5",
            "sage-topk-collab-1.0",
            "virtuoso"]

    @staticmethod
    def create(approach: str, config: Dict[str, str]) -> Approach:
        if approach == "sage":
            return SaGe(approach, config)
        elif approach == "sage-topk":
            return SaGeTopK(approach, config)
        elif approach == "sage-topk-collab":
            return SaGeTopKCollab(approach, config)
        elif approach == "sage-topk-collab-0.5":
            return SaGeTopKCollab(approach, config, refresh_rate=0.5)
        elif approach == "sage-topk-collab-1.0":
            return SaGeTopKCollab(approach, config, refresh_rate=1.0)
        elif approach == "virtuoso":
            return Virtuoso(approach, config)
        raise Exception(f"The approach named {approach} does not exist...")
