# this is an implementation of a Rule Based Query Optimizer
from Nodes.NodeProjection import NodeProjection
from Nodes.NodeCondition import NodeCondition
from Nodes.NodeCross import NodeCross

class RuleQueryOptimizer():

    def run(self, plan_tree):
        #curnode = plan_tree.root
        self.traverse(plan_tree)
        return plan_tree

    def traverse(self, curnode):
        if type(curnode) == NodeCross:
            return

        if type(curnode.children) != list:
            children_list = [curnode.children]
        else:
            children_list = curnode.children

        for i, child in enumerate(children_list):
            # if type(curnode) == NodeCondition:
            #     curnode.predicate = self.simply_predicate(curnode.predicate)

            # TODO Refactor when Joins are Implemented
            # projection pushdown
            # if type(curnode) == NodeProjection and type(child) == JoinNode:
            #     pass
                #self.projection_pushdown(curnode, child)

            if type(curnode) == NodeProjection and type(child) == NodeCondition:
                self.projection_pushdown_select(curnode, child)

            # predicate pushdown
            # if type(curnode) == NodeCondition and type(child) == JoinNode:
            #     pass
                #self.predicate_pushdown(curnode, child)

            self.traverse(child)

    # push down predicates so filters done as early as possible
    # TODO Implement when Joins are implemented
    def predicate_pushdown(self, curnode, child):
        pass
        # del curnode.parent.children[curnode.tablename]
        # curnode.parent.children[child.tablename] = child
        # del curnode.children[child.tablename]
        # curnode.children[curnode.tablename] = child.children[curnode.tablename]
        # child.children[curnode.tablename] = curnode
        # child.parent = curnode.parent
        # curnode.parent = child

    # push down projects so that we do not have unnecessary attributes
    # TODO Implement when Joins are implemented
    def projection_pushdown_join(self, curnode, child):
        # curnode is the projection
        # child is the join
        pass
        # for tabname in child.children.keys():
        #     cols = [col for col in child.join_attrs if tabname in col]
        #     cols2 = [col for col in curnode.columns if tabname in col]
        #     cols.extend(cols2)
        #     new_proj1 = ProjectionNode(columns=cols)
        #     new_proj1.children = {tabname: child.children[tabname]}
        #     new_proj1.parent = child
        #     new_proj1.tablename = tabname
        #     child.children[new_proj1.tablename].parent = new_proj1
        #     child.children[new_proj1.tablename] = new_proj1

    # push down projects so that we do not have unnecessary attributes
    def projection_pushdown_select(self, curnode, child):
        # curnode is the projection
        # child is the select
        project_attrs = curnode.attributes
        new_proj = NodeProjection(children=child.children, attributes=project_attrs)
        child.children = new_proj


    # reorder predicates so that DBMS applies most selective first
    def selective_first(self, plan_tree):
        pass

    # No where clause like 1 = 0 or 0 = 0
    # Merging predicates
    def simply_predicate(self, pred):
        new_pred = pred
        return new_pred

    def join_elimination(self, plan_tree):
        pass

