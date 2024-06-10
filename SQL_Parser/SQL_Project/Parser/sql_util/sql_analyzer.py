import imp
from tkinter.messagebox import NO
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
                            if name_pair[1] == buf_c.this.alias_or_name:
                                # 因为从 Alias 开始的。需要把别名放在第三个字段
                                col_list.append((buf_t,name_pair[0] ,buf_a.alias_or_name))
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
            sub_map = rereat_info_from_ast(ast_root.sources[b_tbl],this_gap)
            result_dict.criteria_col_dict = merge_tbl_col_dict(result_dict.criteria_col_dict,sub_map.criteria_col_dict)
            # FROM 子查询的输出 可能是 上一级的输出 也可能是条件字段 所以 子查询的输出不能算上一级查询的输出 。放在tbl_alias_dict
            # 里面 放回给上级查询 
            #result_dict.out_col_dict = merge_tbl_col_dict(result_dict.out_col_dict,sub_map.out_col_dict)
            result_dict.ref_tbl_set = result_dict.ref_tbl_set.union(sub_map.ref_tbl_set)
            result_dict.tbl_alias_dict[b_tbl] =SOURCE_INFO(SOURCE_TYPE.SUB_QUERY,None,sub_map.out_col_dict)
        else:
            raise Exception('不能处理的RESOURCE 类型',b_tbl)
            
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
    #print(scopes[0])
    root = build_scope(sql_tree)
    final_map = rereat_info_from_ast(root,"*")
    final_map.out_col_dict = remove_alias_from_out_col_dict(final_map.out_col_dict)
    #final_map.out_col_dict = convert_set_to_list(final_map.out_col_dict)
    #final_map.criteria_col_dict = convert_set_to_list(final_map.criteria_col_dict)
    return final_map

def gen_root_from_sql(sql_txt):
    sql_tree = parse_one(sql_txt)
    sql_tree = qualify(sql_tree)
    root = build_scope(sql_tree)
    return root


demo_sql1 = 'SELECT SUM(x.aaa) as sumA ,x.bbb AS fstC ,(tbl1.ccc+tbl2.ttt)*tbl3.www, tbl2.ddd ,tbl3.ggg FROM tbl0 x  JOIN tbl1  JOIN tbl2 JOIN tbl3  WHERE x.caa=2 and x.cbb=tbl1.ccc and tbl1.cee=(select sum(abc) from tbl3 where eee=199) and x.cff = (select a4 from tbl4 where id4=123 )   group by x.fff HAVING AVG(tbl1.zzz)>900 order by x.rrr'
demo_sql2 = "SELECT t.abb,(t.erf+t2.fff) AS fstC,t2.ggg   FROM (select tbl0.abc as abb  ,tbl1.etf as erf,(tbl1.cc+tbl0.uuu) as ccc from tbl0,tbl1 where tbl0.aaa=102) t,tbl2 t2,tbl3 where t2.ddd=4 and t.ccc=t2.ddd and t2.eee>100 and t2.aa2>(select sum(sss) from tbl4 where id>1000) "
#demo_sql2 = "select tbl0.abc as abb,tbl1.etf as erf,(tbl1.cc+tbl0.uuu) as ccc from tbl0,tbl1 where tbl0.aaa=102 and ccc>1000"

demo_sql3 = 'SELECT tbl0.a,tbl0.b,tbl0.c,tbl0.d,bt1.aa  FROM tbl0,(select aa,bb from tbl1 where cc=1) bt1   WHERE g=2 and c=(select  a1 from tbl1 where b2=3)   group by fff  HAVING AVG(zzz)>900 order by rrr'
demo_sql4 = 'SELECT tbl0.a,bt1.aa,bt1.bb,tbl2.aaa  FROM tbl0 ,(select aa,bb from tbl1 where cc=1) bt1,tbl2 WHERE tbl0.d=bt1.bb and  bt1.bb=tbl2.ccc and  tbl0.g=2 and tbl0.c=(select  a1 from tbl1 where b2=3)  group by tbl0.fff  HAVING AVG(tbl0.zzz)>900 order by tbl0.rrr'
demo_sql5 = 'SELECT a,b,c  FROM tbl0  where d=1 and e=2 and f=(select aa from tbl1 where bb=3)  group by tbl0.f  HAVING AVG(tbl0.g)>900 order by tbl0.h'
demo_sql6 = 'SELECT a,b,count(*) as COUNT  FROM tbl0  where d=1 and e=2 and f=(select aa from tbl1 where bb=3)  group by tbl0.f  HAVING AVG(tbl0.g)>900 order by COUNt'

info = rereat_info_from_sql(demo_sql6)
print('TBLS:',info.ref_tbl_set)
print('OUT:',info.out_col_dict)
print('CRITERIA:',info.criteria_col_dict)
