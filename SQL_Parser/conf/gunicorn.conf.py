import multiprocessing

bind = "127.0.0.1:8080"
workers=1 
errorlog = '/usr/local/DS_Forum/logs/forum_gunicorn.error.log'
accesslog = '/usr/local/DS_Forum/logs/gunicorn.access.log'
loglevel = 'error'
proc_name = 'gunicorn_forum_project'
