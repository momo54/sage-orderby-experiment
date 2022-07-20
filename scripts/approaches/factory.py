from typing import Dict, List

from approaches.approach import Approach
from approaches.sage import SaGe
from approaches.sage_topk import SaGeTopK
from approaches.sage_partial_topk import SaGePartialTopK
from approaches.virtuoso import Virtuoso


class ApproachFactory():

    @staticmethod
    def types() -> List[str]:
        return [
            "sage",
            "sage-topk",
            "sage-partial-topk",
            "virtuoso"]

    @staticmethod
    def create(approach: str, config: Dict[str, str]) -> Approach:
        if approach == "sage":
            return SaGe(approach, config)
        elif approach == "sage-topk":
            return SaGeTopK(approach, config)
        elif approach == "sage-partial-topk":
            return SaGePartialTopK(approach, config)
        elif approach == "virtuoso":
            return Virtuoso(approach, config)
        raise Exception(f"The approach named {approach} does not exist...")
