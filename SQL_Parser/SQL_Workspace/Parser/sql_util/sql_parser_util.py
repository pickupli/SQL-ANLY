class SOURCE_TYPE:
    RAW_TABLE = 0
    SUB_QUERY = 1

class SOURCE_INFO:
    Source_Type = 0
    Source_Name = None
    Sub_Outs = None    
    def __init__(self,src_type,src_name,sub_squery_out):
        self.Source_Type = src_type
        self.Source_Name = src_name
        self.Sub_Outs = sub_squery_out


class Scope_Parsed_Info:
    ref_tbl_set=set()
    out_col_dict={};      # 输出字段字典
    criteria_col_dict={}; # 条件字段字典
    scope_info_dict={};   # 查询表面 字段名别名字典
    tbl_alias_dict={};    # 数据表 别名字典
    sub_query_output=[];
    def __init__(self):
        self.ref_tbl_set=set()
        self.out_col_dict=dict();      # 输出字段字典
        self.criteria_col_dict=dict(); # 条件字段字典
        self.scope_info_dict=dict();   # 查询表面 字段名别名字典
        self.tbl_alias_dict=dict();    # 数据表 别名字典
        self.sub_query_output=[];


# 合并 两个字典 将两个字典合并 如果有相同的键 则将value 集合合并
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


# 输出字段 字典中含有别名信息。需要和条件字段字典合并 或者最后输出时需要将别名删除
def remove_alias_from_out_col_dict(src_dict):
    rtn_dict = dict()
    for buf_t in src_dict.keys():
        for buf_c in src_dict[buf_t]:
            if rtn_dict.get(buf_t) is None:
                rtn_dict[buf_t]=set()
            rtn_dict[buf_t].add(buf_c[0]) 
    return rtn_dict

# 将输出字典的集合都转化成LIST 便于输出JSONize

def convert_set_to_list(src_dict):
    rtn_list = []
    for buf_t in src_dict.keys():
        rtn_list.append({buf_t :list (src_dict[buf_t])})
    return rtn_list