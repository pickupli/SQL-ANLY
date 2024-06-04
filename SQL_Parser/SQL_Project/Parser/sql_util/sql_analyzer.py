from sqlglot.optimizer.scope import build_scope,traverse_scope
from sqlglot import parse_one, exp
from sqlglot.optimizer.qualify import qualify

def gen_table_alias_dict(tree_root):
    table_alias_dict= {}
    for metas in tree_root.find_all(exp.Table):
        table_alias_dict[metas.alias_or_name] = metas.name
    return table_alias_dict

def trace_actual_col_name(src_node,tbl_dict):
    col_list = []
    for buf_c in src_node.find_all(exp.Column):
        col_list.append((buf_c.name,tbl_dict[buf_c.table]))
    return  col_list 

# 挖到最后的column
def dig_col(src_node):
    buf_node = src_node
    while not buf_node.key=='column':
        buf_node=buf_node.this
    return  buf_node.name

# 合并
def merge_tbl_col_dict(src1_dict,src2_dict):
    target_dict = {}
    for buf_t_name in src1_dict.keys():

        if  target_dict.get(buf_t_name) is not None:
            target_dict[buf_t_name]=src1_dict[buf_t_name]
        else:
            target_dict[buf_t_name]=set()
    for buf_t_name in src2_dict.keys():
        buf_col_set = target_dict.get(buf_t_name)
        if buf_col_set is not None:
            target_dict[buf_t_name]=target_dict[buf_t_name].union(src2_dict[buf_t_name])
        else:
            target_dict[buf_t_name]=set()


    return target_dict 



# 根据根节点拿到 一个AST 的输出字段 以及 条件字段
def rereat_info_from_ast(ast_root,alias_tbl_dict):
    out_dict = {}
    src_dict = {}
    tbl_set = set()
    for scope in ast_root.traverse():
        if  not scope.parent:
            for b_tbl in scope.sources :
                b_t_info = scope.sources [b_tbl]
                tbl_set.add(b_t_info.name)                    
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
                    for w_b_col in w_clause.find_all(exp.Column):
                        b_c_name= alias_tbl_dict[w_b_col.table]
                        if not (b_c_name in src_dict):
                            src_dict[b_c_name]=set()
                        b_t_c_set = src_dict[b_c_name]
                        b_t_c_set.add(w_b_col.name)
            '''
            # group 子句
            for g_clause in scope.find_all(exp.Group):
                for o_b_col in g_clause.find_all(exp.Column):
                    b_c_name= alias_tbl_dict[o_b_col.table]
                    if not (b_c_name in src_dict):
                        src_dict[b_c_name]=set()
                    b_t_c_set = src_dict[b_c_name]
                    b_t_c_set.add(o_b_col.name)
            # ordr 子句
            for o_clause in scope.find_all(exp.Order):
                for o_b_col in o_clause.find_all(exp.Column):
                    b_c_name= alias_tbl_dict[o_b_col.table]
                    if not (b_c_name in src_dict):
                        src_dict[b_c_name]=set()
                    b_t_c_set = src_dict[b_c_name]
                    b_t_c_set.add(o_b_col.name)
            # having 子句
            for h_clause in scope.find_all(exp.Having):
                for o_b_col in h_clause.find_all(exp.Column):
                    b_c_name= alias_tbl_dict[o_b_col.table]
                    if not (b_c_name in src_dict):
                        src_dict[b_c_name]=set()
                    b_t_c_set = src_dict[b_c_name]
                    b_t_c_set.add(o_b_col.name)            
            '''

        else:
            scope.parent=None
            sub_map = rereat_info_from_ast(scope,alias_tbl_dict)
            src_dict = merge_tbl_col_dict(src_dict,sub_map['SRC'])
            src_dict = merge_tbl_col_dict(src_dict,sub_map['OUT'])
            tbl_set = tbl_set.union(sub_map['TBL'])
    return {'OUT':out_dict,'SRC':src_dict,'TBL':tbl_set}


def rereat_info_from_sql(sql_txt):
    sql_tree = parse_one(sql_txt)
    sql_tree = qualify(sql_tree)
    alias_tbl_dict= gen_table_alias_dict(sql_tree)
    root = build_scope(sql_tree)
    final_map = rereat_info_from_ast(root,alias_tbl_dict)

    return final_map

