from query_optimizer.rule_query_optimizer import RuleQueryOptimizer, Rules
from expression.comparison_expression import ComparisonExpression
from expression.abstract_expression import ExpressionType
from expression.constant_value_expression import ConstantValueExpression
from expression.tuple_value_expression import TupleValueExpression
from query_planner.logical_select_plan import LogicalSelectPlan
from query_planner.logical_inner_join_plan import LogicalInnerJoinPlan
from query_planner.logical_projection_plan import LogicalProjectionPlan
from query_planner.video_table_plan import VideoTablePlan
from loaders.video_loader import SimpleVideoLoader
from src.models import VideoMetaInfo, VideoFormat


def test_simple_predicate_pushdown(verbose=False):
    # Creating the videos
    meta1 = VideoMetaInfo(file='v1', c_format=VideoFormat.MOV, fps=30)
    video1 = SimpleVideoLoader(video_metadata=meta1)

    meta2 = VideoMetaInfo(file='v2', c_format=VideoFormat.MOV, fps=30)
    video2 = SimpleVideoLoader(video_metadata=meta2)

    projection_output = ['v1.1', 'v2.2']
    root = LogicalProjectionPlan(videos=[video1, video2], column_ids=projection_output, foreign_column_ids=[])

    # Creating Expression for Select: Expression is basically where v1.1 == 4
    const = ConstantValueExpression(value=4)
    tup = TupleValueExpression(col_idx=int(projection_output[0].split('.')[1]))
    expression = ComparisonExpression(exp_type=ExpressionType.COMPARE_EQUAL, left=tup, right=const)

    # used both videos because purposely placed BEFORE the join
    s1 = LogicalSelectPlan(predicate=expression, column_ids=['v1.1'], videos=[video1, video2], foreign_column_ids=[])
    s1.parent = root

    j1 = LogicalInnerJoinPlan(videos=[video1, video2], join_ids=['v1.3', 'v2.3'])
    j1.parent = s1

    t1 = VideoTablePlan(video=video1, tablename='v1')
    t2 = VideoTablePlan(video=video2, tablename='v2')

    s1.set_children([j1])
    t1.parent = j1
    t2.parent = j1

    j1.set_children([t1, t2])
    root.set_children([s1])

    rule_list = [Rules.PREDICATE_PUSHDOWN]
    if verbose:
        print('Original Plan Tree')
        print(root)
    qo = RuleQueryOptimizer()
    new_tree = qo.run(root, rule_list)
    if verbose:
        print('New Plan Tree')
        print(new_tree)

    assert root.parent is None
    assert root.children == [j1]
    assert j1.parent == root
    assert j1.children == [s1, t2]
    assert s1.parent == j1
    assert s1.videos == [video1]
    assert t2.parent == j1
    assert s1.children == [t1]
    assert t1.parent == s1

    print('Simple Predicate Pushdown Succeeded!')


def test_simple_projection_pushdown_join(verbose=False):
    # Creating the videos
    meta1 = VideoMetaInfo(file='v1', c_format=VideoFormat.MOV, fps=30)
    video1 = SimpleVideoLoader(video_metadata=meta1)

    meta2 = VideoMetaInfo(file='v2', c_format=VideoFormat.MOV, fps=30)
    video2 = SimpleVideoLoader(video_metadata=meta2)

    projection_output = ['v1.3', 'v2.4']
    root = LogicalProjectionPlan(videos=[video1, video2], column_ids=projection_output, foreign_column_ids=[])

    j1 = LogicalInnerJoinPlan(videos=[video1, video2], join_ids=['v1.1', 'v2.1'])
    j1.parent = root

    t1 = VideoTablePlan(video=video1, tablename='v1')
    t2 = VideoTablePlan(video=video2, tablename='v2')

    t1.parent = j1
    t2.parent = j1

    j1.set_children([t1, t2])
    root.set_children([j1])

    rule_list = [Rules.PROJECTION_PUSHDOWN_JOIN]
    if verbose:
        print('Original Plan Tree')
        print(root)
    qo = RuleQueryOptimizer()
    new_tree = qo.run(root, rule_list)
    if verbose:
        print('New Plan Tree')
        print(new_tree)

    assert root.parent is None
    assert root.children == [j1]
    assert j1.parent == root
    assert type(j1.children[0]) == LogicalProjectionPlan
    assert type(j1.children[1]) == LogicalProjectionPlan
    assert 'v1.1' in j1.children[0].column_ids
    assert 'v1.3' in j1.children[0].column_ids
    assert 'v2.1' in j1.children[1].column_ids
    assert 'v2.4' in j1.children[1].column_ids
    assert type(t2.parent) == LogicalProjectionPlan
    assert type(t1.parent) == LogicalProjectionPlan
    assert j1.children[0].children == [t1]
    assert j1.children[1].children == [t2]
    print('Simple Projection Pushdown Join Test Successful!')


def test_simple_projection_pushdown_select(verbose=False):
    meta1 = VideoMetaInfo(file='v1', c_format=VideoFormat.MOV, fps=30)
    video1 = SimpleVideoLoader(video_metadata=meta1)

    # Creating Expression for Select: Expression is basically where v2.7 == 4
    const = ConstantValueExpression(value=4)
    tup = TupleValueExpression(col_idx=int(7))
    expression = ComparisonExpression(exp_type=ExpressionType.COMPARE_EQUAL, left=tup, right=const)
    s1 = LogicalSelectPlan(predicate=expression, column_ids=['v1.7'], videos=[video1], foreign_column_ids=[])

    projection_output = ['v1.3', 'v1.4']
    root = LogicalProjectionPlan(videos=[video1], column_ids=projection_output, foreign_column_ids=[])

    t1 = VideoTablePlan(video=video1, tablename='v1')

    root.set_children([s1])
    s1.parent = root
    s1.set_children([t1])
    t1.parent = s1

    rule_list = [Rules.PROJECTION_PUSHDOWN_SELECT]
    if verbose:
        print('Original Plan Tree')
        print(root)
    qo = RuleQueryOptimizer()
    new_tree = qo.run(root, rule_list)
    if verbose:
        print('New Plan Tree')
        print(new_tree)

    assert root.parent is None
    assert root.children == [s1]
    assert s1.parent == root
    assert len(s1.children) == 1
    assert type(s1.children[0]) == LogicalProjectionPlan
    assert 'v1.7' in s1.children[0].column_ids
    assert 'v1.3' in s1.children[0].column_ids
    assert 'v1.4' in s1.children[0].column_ids
    assert type(t1.parent) == LogicalProjectionPlan
    assert s1.children[0].children == [t1]
    print('Simple Projection Pushdown Select Test Successful!')


def test_combined_projection_pushdown(verbose=False):
    # Creating the videos
    meta1 = VideoMetaInfo(file='v1', c_format=VideoFormat.MOV, fps=30)
    video1 = SimpleVideoLoader(video_metadata=meta1)

    meta2 = VideoMetaInfo(file='v2', c_format=VideoFormat.MOV, fps=30)
    video2 = SimpleVideoLoader(video_metadata=meta2)

    projection_output = ['v1.3', 'v2.4']
    root = LogicalProjectionPlan(videos=[video1, video2], column_ids=projection_output, foreign_column_ids=[])

    j1 = LogicalInnerJoinPlan(videos=[video1, video2], join_ids=['v1.1', 'v2.1'])
    j1.parent = root

    const = ConstantValueExpression(value=4)
    tup = TupleValueExpression(col_idx=int(7))
    expression = ComparisonExpression(exp_type=ExpressionType.COMPARE_EQUAL, left=tup, right=const)
    s1 = LogicalSelectPlan(predicate=expression, column_ids=['v2.7'], videos=[video1], foreign_column_ids=[])
    s1.parent = j1

    t1 = VideoTablePlan(video=video1, tablename='v1')
    t2 = VideoTablePlan(video=video2, tablename='v2')
    s1.set_children([t2])
    t1.parent = j1
    t2.parent = s1

    j1.set_children([t1, s1])
    root.set_children([j1])

    rule_list = [Rules.PROJECTION_PUSHDOWN_JOIN, Rules.PROJECTION_PUSHDOWN_SELECT]
    if verbose:
        print('Original Plan Tree')
        print(root)
    qo = RuleQueryOptimizer()
    new_tree = qo.run(root, rule_list)
    if verbose:
        print('New Plan Tree')
        print(new_tree)

    assert root.parent is None
    assert root.children == [j1]
    assert j1.parent == root
    assert type(j1.children[0]) == LogicalProjectionPlan
    assert type(j1.children[1]) == LogicalProjectionPlan
    assert type(s1.parent) == LogicalProjectionPlan
    assert 'v2.1' in s1.parent.column_ids
    assert 'v2.4' in s1.parent.column_ids
    assert s1.parent in j1.children
    assert len(s1.children) == 1
    assert type(s1.children[0]) == LogicalProjectionPlan
    assert 'v2.7' in s1.children[0].column_ids
    assert 'v2.1' in s1.children[0].column_ids
    assert 'v2.4' in s1.children[0].column_ids
    assert type(t1.parent) == LogicalProjectionPlan
    assert 'v1.1' in t1.parent.column_ids
    assert 'v1.3' in t1.parent.column_ids
    assert t1.parent in j1.children
    assert s1.children[0].children == [t2]
    print('Combined Projection Pushdown Select Test Successful!')


def test_complex_projection_pushdown_join():
    pass


def test_both_projection_pushdown_and_predicate_pushdown(verbose=False):
    meta1 = VideoMetaInfo(file='v1', c_format=VideoFormat.MOV, fps=30)
    video1 = SimpleVideoLoader(video_metadata=meta1)

    meta2 = VideoMetaInfo(file='v2', c_format=VideoFormat.MOV, fps=30)
    video2 = SimpleVideoLoader(video_metadata=meta2)

    projection_output = ['v1.1', 'v2.2']
    root = LogicalProjectionPlan(videos=[video1, video2], column_ids=projection_output, foreign_column_ids=[])

    # Creating Expression for Select: Expression is basically where v1.1 == 4
    const = ConstantValueExpression(value=4)
    tup = TupleValueExpression(col_idx=int(projection_output[0].split('.')[1]))
    expression = ComparisonExpression(exp_type=ExpressionType.COMPARE_EQUAL, left=tup, right=const)

    # used both videos because purposely placed BEFORE the join
    s1 = LogicalSelectPlan(predicate=expression, column_ids=['v1.1'], videos=[video1, video2], foreign_column_ids=[])
    s1.parent = root

    j1 = LogicalInnerJoinPlan(videos=[video1, video2], join_ids=['v1.3', 'v2.3'])
    j1.parent = s1

    t1 = VideoTablePlan(video=video1, tablename='v1')
    t2 = VideoTablePlan(video=video2, tablename='v2')

    s1.set_children([j1])
    t1.parent = j1
    t2.parent = j1

    j1.set_children([t1, t2])
    root.set_children([s1])

    rule_list = [Rules.PREDICATE_PUSHDOWN, Rules.PROJECTION_PUSHDOWN_JOIN, Rules.PROJECTION_PUSHDOWN_SELECT]
    if verbose:
        print('Original Plan Tree')
        print(root)
    qo = RuleQueryOptimizer()
    new_tree = qo.run(root, rule_list)
    if verbose:
        print('New Plan Tree')
        print(new_tree)

    assert root.parent is None
    assert root.children == [j1]
    assert j1.parent == root
    assert len(j1.children) == 2
    assert s1 in j1.children
    assert s1.parent == j1
    assert s1.videos == [video1]
    assert len(s1.children) == 1
    assert type(s1.children[0]) == LogicalProjectionPlan
    assert 'v1.1' in s1.children[0].column_ids
    assert 'v1.3' in s1.children[0].column_ids
    assert s1.children[0].children == [t1]
    assert t1.parent == s1.children[0]
    s1_ix = j1.children.index(s1)
    if s1_ix == 0:
        proj_ix = 1
    else:
        proj_ix = 0
    assert type(j1.children[proj_ix]) == LogicalProjectionPlan
    assert j1.children[proj_ix].parent == j1
    assert 'v2.3' in j1.children[proj_ix].column_ids
    assert 'v2.2' in j1.children[proj_ix].column_ids
    assert t2.parent == j1.children[proj_ix]
    print('Combined Projection Pushdown and Predicate Pushdown Test Successful!')


def test_double_join_predicate_pushdown(verbose=False):
    meta1 = VideoMetaInfo(file='v1', c_format=VideoFormat.MOV, fps=30)
    video1 = SimpleVideoLoader(video_metadata=meta1)

    meta2 = VideoMetaInfo(file='v2', c_format=VideoFormat.MOV, fps=30)
    video2 = SimpleVideoLoader(video_metadata=meta2)

    meta3 = VideoMetaInfo(file='v3', c_format=VideoFormat.MOV, fps=30)
    video3 = SimpleVideoLoader(video_metadata=meta3)

    projection_output = ['v1.1', 'v2.2', 'v3.4']
    root = LogicalProjectionPlan(videos=[video1, video2, video3], column_ids=projection_output, foreign_column_ids=[])

    # Creating Expression for Select: Expression is basically where v3.3 == 4
    const = ConstantValueExpression(value=4)
    tup = TupleValueExpression(col_idx=int(projection_output[2].split('.')[1]))
    expression = ComparisonExpression(exp_type=ExpressionType.COMPARE_EQUAL, left=tup, right=const)

    # used both videos because purposely placed BEFORE the join
    s1 = LogicalSelectPlan(predicate=expression, column_ids=['v3.3'], videos=[video1, video2, video3], foreign_column_ids=[])
    s1.parent = root

    j1 = LogicalInnerJoinPlan(videos=[video1, video2], join_ids=['v1.3', 'v2.3'])
    j2 = LogicalInnerJoinPlan(videos=[video1, video2,  video3], join_ids=['v1.3', 'v2.3', 'v3.3'])
    j1.parent = j2

    t1 = VideoTablePlan(video=video1, tablename='v1')
    t2 = VideoTablePlan(video=video2, tablename='v2')
    t3 = VideoTablePlan(video=video3, tablename='v3')

    s1.set_children([j2])
    t1.parent = j1
    t2.parent = j1
    j2.set_children([j1,t3])
    t3.parent = j2
    j1.set_children([t1, t2])
    root.set_children([s1])

    rule_list = [Rules.PREDICATE_PUSHDOWN]
    if verbose:
        print('Original Plan Tree')
        print(root)
    qo = RuleQueryOptimizer()
    new_tree = qo.run(root, rule_list)
    if verbose:
        print('New Plan Tree')
        print(new_tree)

    assert root.parent is None
    assert len(root.children) == 1
    assert root.children[0].parent == root
    assert j2.parent == root
    assert len(j2.children) == 2
    assert j2.children[0] == j1
    assert j2.children[1] == s1
    assert s1.parent == j2
    assert j1.parent == j2
    assert len(s1.videos) == 1
    assert s1.videos[0] == video3
    assert len(s1.children) == 1
    assert s1.children[0] == t3
    assert t3.parent == s1
    assert len(j1.children) == 2
    assert j1.children[0] == t1
    assert j1.children[1] == t2
    assert t1.parent == j1
    assert t2.parent == j1
    print('Double join predicate Pushdown Successful!')


def test_double_join_projection_join_pushdown(verbose=False):
    meta1 = VideoMetaInfo(file='v1', c_format=VideoFormat.MOV, fps=30)
    video1 = SimpleVideoLoader(video_metadata=meta1)

    meta2 = VideoMetaInfo(file='v2', c_format=VideoFormat.MOV, fps=30)
    video2 = SimpleVideoLoader(video_metadata=meta2)

    meta3 = VideoMetaInfo(file='v3', c_format=VideoFormat.MOV, fps=30)
    video3 = SimpleVideoLoader(video_metadata=meta3)

    projection_output = ['v1.1', 'v2.2', 'v3.4']
    root = LogicalProjectionPlan(videos=[video1, video2, video3], column_ids=projection_output, foreign_column_ids=[])

    j1 = LogicalInnerJoinPlan(videos=[video1, video2], join_ids=['v1.3', 'v2.3'])
    j2 = LogicalInnerJoinPlan(videos=[video1, video2, video3], join_ids=['v1.3', 'v2.3', 'v3.3'])
    j1.parent = j2
    j2.parent = root
    t1 = VideoTablePlan(video=video1, tablename='v1')
    t2 = VideoTablePlan(video=video2, tablename='v2')
    t3 = VideoTablePlan(video=video3, tablename='v3')

    t1.parent = j1
    t2.parent = j1
    j2.set_children([t3, j1])
    t3.parent = j2
    j1.set_children([t1, t2])
    root.set_children([j2])

    rule_list = [Rules.PROJECTION_PUSHDOWN_JOIN]
    if verbose:
        print('Original Plan Tree')
        print(root)
    qo = RuleQueryOptimizer()
    new_tree = qo.run(root, rule_list)
    if verbose:
        print('New Plan Tree')
        print(new_tree)

    assert root.parent is None
    assert 'v1.1'in root.column_ids
    assert 'v2.2'in root.column_ids
    assert 'v3.4'in root.column_ids
    assert len(root.children) == 1
    assert root.children[0] == j2
    assert j2.parent == root
    assert len(j2.videos) == 3
    assert video1 in j2.videos
    assert video2 in j2.videos
    assert video3 in j2.videos
    assert len(j2.children) == 2
    assert j1 in j2.children
    j1_ix = j2.children.index(j1)
    pix = 1 - j1_ix
    assert type(j2.children[pix]) == LogicalProjectionPlan
    assert len(j2.children[pix].column_ids) == 2
    assert 'v3.4' in j2.children[pix].column_ids
    assert 'v3.3' in j2.children[pix].column_ids
    assert j2.children[pix].parent == j2
    assert len(j2.children[pix].children) == 1
    assert j2.children[pix].children[0] == t3
    assert len(j1.videos) == 2
    assert video1 in j1.videos
    assert video2 in j1.videos
    assert len(j1.children) == 2
    assert type(j1.children[0]) == LogicalProjectionPlan
    assert type(j1.children[1]) == LogicalProjectionPlan
    assert len(j1.children[0].column_ids) == 2
    assert len(j1.children[0].children) == 1
    assert j1.children[0].children[0] == t1
    assert len(j1.children[1].children) == 1
    assert j1.children[1].children[0] == t2
    assert 'v1.3' in j1.children[0].column_ids
    assert 'v1.1' in j1.children[0].column_ids
    assert len(j1.children[1].column_ids) == 2
    assert 'v2.3' in j1.children[1].column_ids
    assert 'v2.2' in j1.children[1].column_ids
    assert j1.children[0].parent == j1
    assert j1.children[1].parent == j1
    print('Double join Projection Pushdown Successful!')



def test_join_elimination(verbose=False):
    meta1 = VideoMetaInfo(file='v1', c_format=VideoFormat.MOV, fps=30)
    video1 = SimpleVideoLoader(video_metadata=meta1)

    meta2 = VideoMetaInfo(file='v2', c_format=VideoFormat.MOV, fps=30)
    video2 = SimpleVideoLoader(video_metadata=meta2)

    projection_output = ['v1.1', 'v2.2']
    root = LogicalProjectionPlan(videos=[video1, video2], column_ids=projection_output, foreign_column_ids=['v2.2'])

    # Creating Expression for Select: Expression is basically where v1.1 == v2.2
    # Also creating a foreign key constraint for v1 where it requires v2.2
    # hence join elimination should delete the join node and just return all of v1.1 for select
    tup1 = TupleValueExpression(col_idx=1)
    tup2 = TupleValueExpression(col_idx=2)
    expression = ComparisonExpression(exp_type=ExpressionType.COMPARE_EQUAL, left=tup1, right=tup2)

    # used both videos because purposely placed BEFORE the join
    s1 = LogicalSelectPlan(predicate=expression, column_ids=['v1.1', 'v2.2'], videos=[video1, video2],
                           foreign_column_ids=['v2.2'])
    s1.parent = root

    j1 = LogicalInnerJoinPlan(videos=[video1, video2], join_ids=['v1.1', 'v2.2'])
    j1.parent = s1

    t1 = VideoTablePlan(video=video1, tablename='v1')
    t2 = VideoTablePlan(video=video2, tablename='v2')
    t1.parent = j1
    t2.parent = j1

    root.set_children([s1])
    s1.set_children([j1])
    j1.set_children([t1, t2])

    rule_list = [Rules.JOIN_ELIMINATION]
    if verbose:
        print('Original Plan Tree')
        print(root)
    qo = RuleQueryOptimizer()
    new_tree = qo.run(root, rule_list)
    if verbose:
        print('New Plan Tree')
        print(new_tree)


def test_shouldnot_simply_predicate(verbose=False):
    meta1 = VideoMetaInfo(file='v1', c_format=VideoFormat.MOV, fps=30)
    video1 = SimpleVideoLoader(video_metadata=meta1)

    # Creating Expression for Select: Expression is basically where v1.7 == 4
    const = ConstantValueExpression(value=4)
    tup = TupleValueExpression(col_idx=int(7))
    expression = ComparisonExpression(exp_type=ExpressionType.COMPARE_EQUAL, left=tup, right=const)
    s1 = LogicalSelectPlan(predicate=expression, column_ids=['v1.7'], videos=[video1], foreign_column_ids=[])

    projection_output = ['v1.3', 'v1.4']
    root = LogicalProjectionPlan(videos=[video1], column_ids=projection_output, foreign_column_ids=[])

    t1 = VideoTablePlan(video=video1, tablename='v1')

    root.set_children([s1])
    s1.parent = root
    s1.set_children([t1])
    t1.parent = s1
    rule_list = [Rules.SIMPLIFY_PREDICATE]
    if verbose:
        print('Original Plan Tree')
        print(root)
    qo = RuleQueryOptimizer()
    new_tree = qo.run(root, rule_list)
    if verbose:
        print('New Plan Tree')
        print(new_tree)


def test_should_simply_predicate(verbose=False):
    meta1 = VideoMetaInfo(file='v1', c_format=VideoFormat.MOV, fps=30)
    video1 = SimpleVideoLoader(video_metadata=meta1)

    # Creating Expression for Select: Expression is basically where 0==1
    const1 = ConstantValueExpression(value=0)
    const2 = ConstantValueExpression(value=1)
    # Comparing if 0 is equal to 1
    # It will be False which will trigger the simplify predicate function
    expression = ComparisonExpression(exp_type=ExpressionType.COMPARE_EQUAL, left=const1, right=const2)
    s1 = LogicalSelectPlan(predicate=expression, column_ids=[], videos=[], foreign_column_ids=[])

    projection_output = ['v1.3', 'v1.4']
    root = LogicalProjectionPlan(videos=[video1], column_ids=projection_output, foreign_column_ids=[])

    t1 = VideoTablePlan(video=video1, tablename='v1')

    root.set_children([s1])
    s1.parent = root
    s1.set_children([t1])
    t1.parent = s1
    rule_list = [Rules.SIMPLIFY_PREDICATE]
    if verbose:
        print('Original Plan Tree')
        print(root)
    qo = RuleQueryOptimizer()
    new_tree = qo.run(root, rule_list)
    if verbose:
        print('New Plan Tree')
        print(new_tree)


if __name__ == '__main__':
    test_simple_predicate_pushdown()
    test_simple_projection_pushdown_select()
    test_simple_projection_pushdown_join()
    test_combined_projection_pushdown()
    test_both_projection_pushdown_and_predicate_pushdown()
    test_double_join_predicate_pushdown()
    test_double_join_projection_join_pushdown()
    test_join_elimination()
<<<<<<< HEAD
    #test_shouldnot_simply_predicate()
    #test_should_simply_predicate()
=======
    test_shouldnot_simply_predicate()
    test_should_simply_predicate()
>>>>>>> 62fbf696c0f62ba82b8f8653de6d7af488e780d4
