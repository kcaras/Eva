import unittest
from src.query_parser.eva_parser import EvaFrameQLParser
# from query_planner.logical_select_plan import LogicalSelectPlan
from query_planner.seq_scan_plan import SeqScanPlan
# from query_planner.video_table_plan import VideoTablePlan
from query_parser.table_ref import TableRef
from query_optimizer.statement2plantree import Statement2Plantree
from query_optimizer.rule_query_optimizer import RuleQueryOptimizer, Rules


class ParserToRuleQueryOptimizer(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_simple_query(self, verbose=False):
        parser = EvaFrameQLParser()
        query = "SELECT CLASS FROM TAIPAI;"
        eva_statement_list = parser.parse(query)
        plan_tree = Statement2Plantree.convert(eva_statement_list)
        self.assertIsNone(plan_tree.parent)
        self.assertTrue(type(plan_tree), SeqScanPlan)
        self.assertTrue(len(plan_tree.children) == 1)
        self.assertTrue(type(plan_tree.children[0]) == TableRef)
        rule_list = [Rules.PREDICATE_PUSHDOWN, Rules.PROJECTION_PUSHDOWN_JOIN, Rules.PROJECTION_PUSHDOWN_SELECT]
        if verbose:
            print('Original Plan Tree')
            print(plan_tree)
        qo = RuleQueryOptimizer()
        new_tree = qo.run(plan_tree, rule_list)
        if verbose:
            print('New Plan Tree')
            print(new_tree)
        self.assertIsNone(new_tree.parent)
        self.assertTrue(type(new_tree), SeqScanPlan)
        self.assertTrue(len(new_tree.children) == 1)
        self.assertTrue(type(new_tree.children[0]) == TableRef)
