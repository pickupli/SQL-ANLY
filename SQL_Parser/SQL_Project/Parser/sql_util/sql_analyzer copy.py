from sqlglot.optimizer.scope import build_scope,traverse_scope
from sqlglot.optimizer import Scope

from sqlglot import parse_one, exp
from sqlglot.optimizer.qualify import qualify

def gen_table_alias_dict(tree_root):
    table_alias_dict= {}
    # 找出所有表名的 别名映射
    for srcs in tree_root.sources :
        buf_node = tree_root.sources[srcs]
        if type(buf_node)==exp.Table:
            table_alias_dict[buf_node.alias_or_name] = buf_node.name
        elif type(buf_node)==Scope:
            table_alias_dict = dict(gen_table_alias_dict(buf_node),**table_alias_dict)
    return table_alias_dict

def trace_actual_col_name(src_node,tbl_dict):
    col_list = []
    for buf_c in src_node.find_all(exp.Column):
        if tbl_dict.get(buf_c.table) is not None:
            col_list.append((buf_c.name,tbl_dict[buf_c.table]))
        else: 
            # 如果别名 对应的表为空 
            col_list.append((buf_c.name,""))
    return  col_list 

# 合并
def merge_tbl_col_dict(src1_dict,src2_dict):
    target_dict = {}
    for buf_t_name in src1_dict.keys():
        if  target_dict.get(buf_t_name) is not None:
            target_dict[buf_t_name]=target_dict[buf_t_name].union(src1_dict[buf_t_name]) 
        else:
            target_dict[buf_t_name]=src1_dict[buf_t_name].copy()
    for buf_t_name in src2_dict.keys():
        buf_col_set = target_dict.get(buf_t_name)
        if buf_col_set is not None:
            target_dict[buf_t_name]=target_dict[buf_t_name].union(src2_dict[buf_t_name])
        else:
            target_dict[buf_t_name]=src2_dict[buf_t_name].copy()
    return target_dict 

def is_clause_critieria_colum(node,clause_root_txt):
    while node.parent is not None:
        if  type(node.parent) == exp.Select:
            return False
        if str(node.parent) == clause_root_txt:
            return True
        node = node.parent
    return False



# 根据根节点拿到 一个AST 的输出字段 以及 条件字段
def rereat_info_from_ast(ast_root):
    out_dict = {}
    src_dict = {}
    tbl_set = set()
    alias_tbl_dict = gen_table_alias_dict(ast_root)
    scope = ast_root
    for buf_i in scope.traverse():
        print(buf_i)
    # 递归遍历 FROM 区域 可能是表 或者 子查询
    for b_tbl in ast_root.sources :
        if isinstance(scope.sources [b_tbl] ,Scope): 
            sub_map  = rereat_info_from_ast(scope.sources[b_tbl])
            src_dict = merge_tbl_col_dict(src_dict,sub_map['SRC'])
            out_dict = merge_tbl_col_dict(out_dict,sub_map['OUT'])
            tbl_set = tbl_set.union(sub_map['TBL'])
        else:
            b_t_info = scope.sources [b_tbl]
            tbl_set.add(b_t_info.name)

    # 查询的目标列
    for b_col in scope.expression: 
        b_col_infos = trace_actual_col_name(b_col,alias_tbl_dict)
        for buf_c in b_col_infos:
            b_t_name= buf_c[1] 
            if not (b_t_name in out_dict):
                out_dict[b_t_name]=set()
            b_t_c_set = out_dict[b_t_name]
            b_t_c_set.add(buf_c[0])
    

    clause_types = [exp.Where,exp.Order,exp.Ordered,exp.Group,exp.Having]
    for buf_c_t in clause_types:
        for w_clause in scope.find_all(buf_c_t):
            clause_nodes = w_clause.walk()
            for b_node in clause_nodes:
                if type(b_node)==exp.Column:
                    # 从当前节点上溯到当前子句 如果中间没有出现Select 即 更低的子查询 则表面该字段是 where 子句的 条件字段
                    if is_clause_critieria_colum(b_node,str(w_clause)):
                        real_tbl_name = alias_tbl_dict.get(b_node.table)
                        b_c_name= real_tbl_name if real_tbl_name is not None else '' 
                        if not (b_c_name in src_dict):
                            src_dict[b_c_name]=set()
                        b_t_c_set = src_dict[b_c_name]
                        b_t_c_set.add(b_node.name)
                elif type(b_node)==exp.Select:
                    sub_map = rereat_info_from_ast(build_scope(b_node))
                    src_dict = merge_tbl_col_dict(src_dict,sub_map['SRC'])
                    src_dict = merge_tbl_col_dict(src_dict,sub_map['OUT'])
                    tbl_set = tbl_set.union(sub_map['TBL'])


    return {'OUT':out_dict,'SRC':src_dict,'TBL':tbl_set}


def rereat_info_from_sql(sql_txt):
    sql_tree = parse_one(sql_txt)
    sql_tree = qualify(sql_tree)
    root = build_scope(sql_tree)
    final_map = rereat_info_from_ast(root)
    return final_map

def gen_root_from_sql(sql_txt):
    sql_tree = parse_one(sql_txt)
    sql_tree = qualify(sql_tree)
    root = build_scope(sql_tree)
    return root


demo_sql1 = 'SELECT SUM(x.aaa) as sumA ,x.bbb AS fstC ,(tbl1.ccc+tbl2.ttt)*tbl3.www, tbl2.ddd ,tbl3.ggg FROM tbl0 x  JOIN tbl1  JOIN tbl2 JOIN tbl3  WHERE x.caa=2 and x.cbb=tbl1.ccc and tbl1.cee=(select sum(abc) from tbl3 where eee=199) and x.cff = (select a4 from tbl4 where id4=123 )   group by x.fff HAVING AVG(tbl1.zzz)>900 order by x.rrr'
demo_sql2 = "SELECT t.abc,t.etf AS fstC,tbl2.ggg FROM (select tbl0.abc as abc  ,tbl1.etf as etf from tbl0,tbl1 where tbl0.aaa=102) t,tbl2 where tbl2.ddd=4 and tbl1.ccc=tbl2.ddd "
demo_sql3 = 'SELECT tbl0.a,tbl0.b,tbl0.c,tbl0.d,bt1.aa  FROM tbl0,(select aa,bb from tbl1 where cc=1) bt1   WHERE g=2 and c=(select  a1 from tbl1 where b2=3)   group by fff  HAVING AVG(zzz)>900 order by rrr'

#root1 = gen_root_from_sql(demo_sql1)
#root2 = gen_root_from_sql(demo_sql2)
info = rereat_info_from_sql(demo_sql3)
print(info)
