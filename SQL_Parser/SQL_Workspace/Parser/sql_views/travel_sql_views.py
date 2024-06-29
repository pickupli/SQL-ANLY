# 潜水者的心跳记录 已经弃用
from cmath import e
from django.http import JsonResponse
from Parser.ajax_wrapper import portal_check
from django.views.decorators.csrf import csrf_exempt
from Parser.sql_util import sql_analyzer
import traceback


# SET 不能直接Json 化  转换成 ARRAY
def Trans_Set_To_Array(Src_Dict):
    rtn_array=[]
    for buf_t in Src_Dict.keys():
        rtn_array.append({'TBL_NAME':buf_t,'COLS':list(Src_Dict[buf_t])})
    return rtn_array

@csrf_exempt
def get_sql_info_ajax(request):
    @portal_check
    def biz_view(req, cur_user, ip, params):
        # "SELECT  SUM(x.aaa) as sumA ,x.bbb AS fstC ,(tbl1.ccc+tbl2.ttt)*tbl3.www, tbl2.ddd ,tbl3.ggg FROM tbl0 x  JOIN tbl1  JOIN tbl2 JOIN tbl3  WHERE x.caa=2 and x.cbb=tbl1.ccc and tbl1.cee=(select sum(abc) from tbl3 where eee=199) and x.cff = (select a4 from tbl4 where id4=123 ) group by x.fff order by x.rrr"
        try:
            src_sql = params['sql_content']
            final_dict = sql_analyzer.rereat_info_from_sql(src_sql)
            table_list = list(final_dict.ref_tbl_set)
            criteria_dict = Trans_Set_To_Array(final_dict.criteria_col_dict) 
            out_dict = Trans_Set_To_Array(final_dict.out_col_dict)

        except Exception as err:
            return JsonResponse({'AJAX_RESULT':False,'ERR_MSG':str(err)})
        return JsonResponse({'AJAX_RESULT': True, 'RESULT': {'TBL':table_list,
        'SRC':criteria_dict,'OUT':out_dict}})
    return biz_view(request)