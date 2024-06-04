from django.shortcuts import render
from django.views import View

# Create your views here.
def Demo_view(request):
    resp = render(request, 'sql_demo.html')
    return resp