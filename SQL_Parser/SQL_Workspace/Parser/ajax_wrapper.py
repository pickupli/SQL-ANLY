from django.http import JsonResponse
from django.contrib import auth
import json


# 前端装饰器
def portal_check(view_biz):
    def _portal_check(request):
        try:
            # start_time =datetime.now()
            request_params = {}
            request_params = json.loads(request.body.decode("utf-8"))
            cur_user = auth.get_user(request)
            rtn_map  = view_biz(request, cur_user, 'x.x.x.x', request_params)
            return rtn_map
        except Exception as e:
            import traceback
            return JsonResponse({'AJAX_RESULT':False,'ERR_MSG':e.__str__()})
    return _portal_check


def wrap_pager(total_size, page_size, page_num):
    return {"PAGER": [total_size,page_size,page_num]}
