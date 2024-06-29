from lib2to3.pgen2.token import EQUAL
from sqlglot.optimizer.scope import build_scope,traverse_scope
from sqlglot.optimizer import Scope
from sqlglot import parse_one, exp
from sqlglot.optimizer.qualify import qualify
from Parser.sql_util.sql_parser_util import SOURCE_INFO,Scope_Parsed_Info ,SOURCE_TYPE,merge_tbl_col_dict,remove_alias_from_out_col_dict,convert_set_to_list

# 读出条件字段的真实字段名和真实表名        
def find_actual_colname_from_col_for_criteria(src_col,tbl_alias_dict): 
    rtn_col_list = [] 
    buf_tbl_info = tbl_alias_dict.get(src_col.table)
    if buf_tbl_info is not None:
        if buf_tbl_info.Source_Type == SOURCE_TYPE.RAW_TABLE:
            actual_tbl_name = buf_tbl_info.Source_Name
            rtn_col_list.append((actual_tbl_name,src_col.this.name))
        elif buf_tbl_info.Source_Type == SOURCE_TYPE.SUB_QUERY:
            sub_query_info = buf_tbl_info.Sub_Outs
            for buf_t in sub_query_info.keys():
                for name_pair in sub_query_info[buf_t]:
                    if name_pair[1] == src_col.this.alias_or_name:
                        rtn_col_list.append((buf_t,name_pair[0]))
        else:
            raise Exception('未知的来源类型')
    else:
        rtn_col_list.append(("",src_col.this.name))
    return rtn_col_list

# 读出输出字段的真实字段名，别名 和真实表名        
def trace_actual_col_name_for_source(src_node,tbl_aliat_dict):
    col_list = []
    if src_node.is_star :
        if hasattr(src_node,'table') :
            col_list.append((src_node.table,"*","*"))
        else:
            father_scope =  build_scope(src_node.parent_select)
            if len(father_scope.sources)>1 :
                raise Exception('通配* 不能确认来源!')
            else:
                for buf_t in father_scope.sources:
                    if isinstance(father_scope.sources[buf_t] ,exp.Table): 
                        col_list.append((buf_t,"*","*"))
        return col_list
    for buf_a in src_node.find_all(exp.Alias):
        buf_colunms_count = 0 
        for buf_c in buf_a.find_all(exp.Column):
            buf_colunms_count=buf_colunms_count +1
            if tbl_aliat_dict.get(buf_c.table) is not None:
                buf_tbl_info = tbl_aliat_dict[buf_c.table]
                if buf_tbl_info.Source_Type == SOURCE_TYPE.RAW_TABLE:
                    actual_tbl_name = buf_tbl_info.Source_Name
                    col_list.append((actual_tbl_name,buf_c.name,buf_a.alias_or_name))
                elif buf_tbl_info.Source_Type == SOURCE_TYPE.SUB_QUERY:
                    sub_query_info = buf_tbl_info.Sub_Outs
                    for buf_t in sub_query_info.keys():
                        for name_pair in sub_query_info[buf_t]:
                            if name_pair[1] == buf_c.this.alias_or_name or name_pair[1]=="*":
                                # 因为从 Alias 开始的。需要把别名放在第三个字段
                                col_list.append((buf_t,buf_c.name ,buf_c.alias_or_name))
                else:
                    raise Exception('未知的来源类型')
            else: 
                # 如果别名 对应的表为空 
                pass;
        if buf_colunms_count==0:
            # 如果别名 节点下无任何显式的字段名 则表名是个函数计算量 而且和具体字段无关 则不指定该输出字段的 表名
            col_list.append(("",buf_a.alias_or_name ,buf_a.alias_or_name))
                   
    return  col_list 


# 根据根节点拿到 一个AST 的输出字段 以及 条件字段
# gap 为调试打印时 递归调用 缩进 输出用 平时调用可忽略
def rereat_info_from_ast(ast_root,gap=""):
    this_gap = "*" + gap
    result_dict = Scope_Parsed_Info()
    # 先处理 数据源表(包含子查询) 
    for b_tbl in ast_root.sources :
        # 如果FROM 项目 为实际的表
        if isinstance(ast_root.sources[b_tbl] ,exp.Table): 
            b_t_info = ast_root.sources[b_tbl]
            result_dict.ref_tbl_set.add(b_t_info.name)
            result_dict.tbl_alias_dict[b_t_info.alias_or_name] = SOURCE_INFO(SOURCE_TYPE.RAW_TABLE,b_t_info.name,None)
        # 如果FROM 项目 子查询
        elif isinstance(ast_root.sources[b_tbl] ,Scope):
            sub_scope = ast_root.sources[b_tbl]
            sub_map = rereat_info_from_ast(sub_scope,this_gap)
            result_dict.criteria_col_dict = merge_tbl_col_dict(result_dict.criteria_col_dict,sub_map.criteria_col_dict)
            # FROM 子查询的输出 可能是 上一级的输出 也可能是条件字段 所以 子查询的输出不能算上一级查询的输出 。放在tbl_alias_dict
            # 里面 放回给上级查询 
            #result_dict.out_col_dict = merge_tbl_col_dict(result_dict.out_col_dict,sub_map.out_col_dict)
            result_dict.ref_tbl_set = result_dict.ref_tbl_set.union(sub_map.ref_tbl_set)
            result_dict.tbl_alias_dict[b_tbl] =SOURCE_INFO(SOURCE_TYPE.SUB_QUERY,None,sub_map.out_col_dict)
        else:
            raise Exception('不能处理的RESOURCE 类型',b_tbl)
    # UNION 类型 直接递归到下一层 
    if type(ast_root.expression) == exp.Union:
        for buf_s in ast_root.union_scopes:
            sub_map = rereat_info_from_ast(buf_s,this_gap)
            result_dict.criteria_col_dict = merge_tbl_col_dict(result_dict.criteria_col_dict,sub_map.criteria_col_dict)
            result_dict.out_col_dict = merge_tbl_col_dict(result_dict.out_col_dict,sub_map.out_col_dict)
            result_dict.ref_tbl_set = result_dict.ref_tbl_set.union(sub_map.ref_tbl_set)
            # result_dict.tbl_alias_dict[b_tbl] =SOURCE_INFO(SOURCE_TYPE.SUB_QUERY,None,sub_map.out_col_dict)
        return result_dict
    # 查询输出的目标列
    for b_col in ast_root.expression: 
        b_col_infos = trace_actual_col_name_for_source(b_col,result_dict.tbl_alias_dict)
        for buf_c in b_col_infos:
            b_t_name= buf_c[0] 
            if not (b_t_name in result_dict.out_col_dict):
                result_dict.out_col_dict[b_t_name]=set()
            b_t_c_set = result_dict.out_col_dict[b_t_name]
            b_t_c_set.add((buf_c[1],buf_c[2]))
    #查询的条件列
    clause_types = [exp.Where,exp.Order,exp.Ordered,exp.Group,exp.Having]
    for buf_c_t in clause_types:
        for w_clause in ast_root.find_all(buf_c_t):            
            if w_clause.depth!=ast_root.expression.depth+1:
                break
            # 只遍历子句下一级的数据节点 ，子查询的子查询 
            def is_this_layer_column(node):
                # print('%s# CLAUSE[%d]:%s - %s' % (this_gap,b_node.depth,type(b_node),b_node))                
                while node.parent.depth > w_clause.depth :
                    if  type(node.parent) == exp.Select:
                        # print('%s# NODE[%d]:%s - %s' % (this_gap,b_node.depth,type(b_node),b_node))                
                        return True
                    node = node.parent
                return False
            clause_nodes = w_clause.walk(bfs=True,prune=is_this_layer_column)
            for b_node in clause_nodes:
                # print('%s# CLAUSE NODE[%d]:%s - %s' % (this_gap,b_node.depth,type(b_node),b_node))
                if type(b_node)==exp.Column:
                    col_list = find_actual_colname_from_col_for_criteria(b_node,result_dict.tbl_alias_dict)
                    for buf_c in col_list:
                        if not (buf_c[0] in result_dict.criteria_col_dict.keys()):
                            result_dict.criteria_col_dict[buf_c[0]]=set()
                        b_t_c_set = result_dict.criteria_col_dict[buf_c[0]]
                        b_t_c_set.add(buf_c[1])
                elif type(b_node)==exp.Select:
                    sub_map = rereat_info_from_ast(build_scope(b_node),this_gap)
                    result_dict.criteria_col_dict = merge_tbl_col_dict(result_dict.criteria_col_dict,sub_map.criteria_col_dict.copy())
                    result_dict.criteria_col_dict = merge_tbl_col_dict(result_dict.criteria_col_dict,remove_alias_from_out_col_dict(sub_map.out_col_dict))
                    result_dict.ref_tbl_set = result_dict.ref_tbl_set.union(sub_map.ref_tbl_set)
                    result_dict.tbl_alias_dict[b_tbl] = SOURCE_INFO(SOURCE_TYPE.SUB_QUERY,None,sub_map.out_col_dict)
    return result_dict


def rereat_info_from_sql(sql_txt):
    sql_tree = parse_one(sql_txt)
    sql_tree = qualify(sql_tree)
    #scopes = traverse_scope(sql_tree)
    #print(sql_tree)
    root = build_scope(sql_tree)
    final_map = rereat_info_from_ast(root,"*")
    final_map.out_col_dict = remove_alias_from_out_col_dict(final_map.out_col_dict)
    #final_map.out_col_dict = convert_set_to_list(final_map.out_col_dict)
    #final_map.criteria_col_dict = convert_set_to_list(final_map.criteria_col_dict)
    return final_map

def transe_root_from_sql(sql_txt):
    sql_tree = parse_one(sql_txt)
    repr(sql_tree)

    sql_tree = qualify(sql_tree)

    root = build_scope(sql_tree)
    #print(sql_tree.to_s())
    sql_tree.__repr__()
    for buf_type in sql_tree.arg_types:
        print("%s -> [%s]" % (buf_type,sql_tree.arg_types[buf_type]))
        pass
    for buf_arg in sql_tree.args:
        print("%s >> [%s]" % (buf_arg,sql_tree.args[buf_arg]))
        pass

    return sql_tree



#info = rereat_info_from_sql(demo_sql7)
#print('TBLS:',info.ref_tbl_set)
#print('OUT:',info.out_col_dict)
#print('CRITERIA:',info.criteria_col_dict)
